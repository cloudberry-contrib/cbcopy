package cluster

/*
 * This file contains structs and functions related to interacting
 * with files and directories, both locally and remotely over SSH.
 */

import (
	"bytes"
	"fmt"
	"os/exec"
	"sort"
	"strings"

	// "github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/cloudberry-contrib/cbcopy/internal/dbconn"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/pkg/errors"
)

type Executor interface {
	ExecuteLocalCommand(commandStr string) (string, error)
	ExecuteClusterCommand(scope Scope, commandList []ShellCommand) *RemoteOutput
}

// This type only exists to allow us to mock Execute[...]Command functions for testing
type GPDBExecutor struct{}

/*
 * A Cluster object stores information about the cluster in three ways:
 * - Segments is basically equivalent to gp_segment_configuration, a plain
 *   list of segment information, and is ordered by content id.
 * - ByContent is a map of content id to the single corresponding segment.
 * - ByHost is a map of hostname to all of the segments on that host.
 * The maps are only stored for efficient lookup; Segments is the "source of
 * truth" for the cluster.  The maps actually hold pointers to the SegConfigs
 * in Segments, so modifying Segments will modify the maps as well.
 */
type Cluster struct {
	ContentIDs []int
	Hostnames  []string
	Segments   []SegConfig
	ByContent  map[int][]*SegConfig
	ByHost     map[string][]*SegConfig
	Executor
}

type SegConfig struct {
	DbID      int
	ContentID int
	Role      string
	Port      int
	Hostname  string
	DataDir   string
}

/*
 * A "scope" is a value composed of one or more of the below constants that is
 * passed into ShellCommands, RemoteOutputs, and related structs and functions
 * to define the scope of the command execution.  The meaning of each value is
 * as follows:
 *
 * ON_SEGMENTS:     Execute one command per segment.
 * ON_HOSTS:        Execute one command per host.
 *
 * INCLUDE_COORDINATOR:  Include the coordinator host or segment in the command list.
 * EXCLUDE_COORDINATOR:  Exclude the coordinator host or segment from the command list.
 *
 * ON_REMOTE:       Execute each command on the specified remote segment/host.
 * ON_LOCAL:        Execute all commands on the coordinator host.
 *
 * INCLUDE_MIRRORS: Include mirror segments and hosts in the command list.
 * EXCLUDE_MIRRORS: Exclude mirror segments and hosts from the command list.
 *
 * A scope is composed of one or more of these values bitwise-OR'd together to
 * obtain a final scope, which has the following bitmask:
 *
 *   /------- INCLUDE_MIRRORS (1) or EXCLUDE_MIRRORS (0)
 *   |/------ INCLUDE_COORDINATOR (1) or EXCLUDE_COORDINATOR (0)
 *   ||/----- ON_LOCAL (1) or ON_REMOTE (0)
 *   |||/---- ON_HOSTS (1) or ON_SEGMENTS (0)
 *   ||||
 *   vvvv
 *   0000
 *
 * For instance, to execute a command on all hosts including the coordinator host,
 * you would pass a function the scope ON_HOSTS | INCLUDE_COORDINATOR.
 *
 * The default scope is 0000, to execute a command on all primary segments,
 * equivalent to ON_SEGMENTS | ON_REMOTE | EXCLUDE_COORDINATOR | EXCLUDE_MIRRORS,
 * though by convention only ON_SEGMENTS need be passed to a function.
 *
 * Technically, the four zero-valued constants are redundant, but are provided
 * so that function callers can specify whatever scope they feel is most clear
 * (e.g. using INCLUDE_COORDINATOR vs. EXCLUDE_COORDINATOR as the basic scopes instead of
 * ON_SEGMENTS vs. ON_HOSTS if every ExecuteClusterCommand call is per-segment
 * and the utility includes the coordinator in commands a good portion of the time.)
 *
 * In version 1.0.10, support for the COORDINATOR scope was added, as GPDB 7 uses
 * "coordinator" in place of "master".  The MASTER scopes are left in place (and
 * identical to the COORDINATOR scopes) for backwards compatibility, but may be
 * deprecated in future.
 */

type Scope uint8

const (
	ON_SEGMENTS         Scope = 0
	ON_HOSTS            Scope = 1
	EXCLUDE_COORDINATOR Scope = 0
	INCLUDE_COORDINATOR Scope = 1 << 1
	EXCLUDE_MASTER      Scope = 0
	INCLUDE_MASTER      Scope = 1 << 1
	ON_REMOTE           Scope = 0
	ON_LOCAL            Scope = 1 << 2
	EXCLUDE_MIRRORS     Scope = 0
	INCLUDE_MIRRORS     Scope = 1 << 3
)

func scopeIsSegments(scope Scope) bool {
	return scope&ON_HOSTS == ON_SEGMENTS
}

func scopeIsHosts(scope Scope) bool {
	return scope&ON_HOSTS == ON_HOSTS
}

func scopeExcludesCoordinator(scope Scope) bool {
	return scope&INCLUDE_COORDINATOR == EXCLUDE_COORDINATOR
}

func scopeIncludesCoordinator(scope Scope) bool {
	return scope&INCLUDE_COORDINATOR == INCLUDE_COORDINATOR
}

func scopeIsRemote(scope Scope) bool {
	return scope&ON_LOCAL == ON_REMOTE
}

func scopeIsLocal(scope Scope) bool {
	return scope&ON_LOCAL == ON_LOCAL
}

func scopeExcludesMirrors(scope Scope) bool {
	return scope&INCLUDE_MIRRORS == EXCLUDE_MIRRORS
}

func scopeIncludesMirrors(scope Scope) bool {
	return scope&INCLUDE_MIRRORS == INCLUDE_MIRRORS
}

/*
 * A ShellCommand stores a command to be executed (in both executable and
 * display form), as well as the results of the command execution and the
 * necessary information to determine how the command will be or was executed.
 *
 * It is assumed that before a caller references Content or Host for a given
 * command, they will check Scope to ensure that that field is meaningful for
 * that command.  GenerateCommandList sets Host to "" for per-segment commands
 * and Content to -2 for per-host commands, just to be safe.
 */
type ShellCommand struct {
	Scope         Scope
	Content       int
	Host          string
	Command       *exec.Cmd
	CommandString string
	Stdout        string
	Stderr        string
	Error         error
	Completed     bool
}

func NewShellCommand(scope Scope, content int, host string, command []string) ShellCommand {
	return ShellCommand{
		Scope:         scope,
		Content:       content,
		Host:          host,
		Command:       exec.Command(command[0], command[1:]...),
		CommandString: strings.Join(command, " "),
	}
}

/*
 * A RemoteOutput is used to make it easier to identify the success or failure
 * of a cluster command and to display the results to the user.
 */
type RemoteOutput struct {
	Scope          Scope
	NumErrors      int
	Commands       []ShellCommand
	FailedCommands []*ShellCommand
}

func NewRemoteOutput(scope Scope, numErrors int, commands []ShellCommand) *RemoteOutput {
	failedCommands := make([]*ShellCommand, numErrors)
	index := 0
	for i := range commands {
		if commands[i].Error != nil {
			failedCommands[index] = &commands[i]
			index++
		}
	}
	return &RemoteOutput{
		Scope:          scope,
		NumErrors:      numErrors,
		Commands:       commands,
		FailedCommands: failedCommands,
	}
}

/*
 * Base cluster functions
 */

func NewCluster(segConfigs []SegConfig) *Cluster {
	cluster := Cluster{}
	cluster.Segments = segConfigs
	cluster.ByContent = make(map[int][]*SegConfig, 0)
	cluster.ByHost = make(map[string][]*SegConfig, 0)
	cluster.Executor = &GPDBExecutor{}

	for i := range cluster.Segments {
		segment := &cluster.Segments[i]
		cluster.ByContent[segment.ContentID] = append(cluster.ByContent[segment.ContentID], segment)
		segmentList := cluster.ByContent[segment.ContentID]
		if len(segmentList) == 2 && segmentList[0].Role == "m" {
			/*
			 * GetSegmentConfiguration always returns primaries before mirrors,
			 * but we can't guarantee the []SegConfig passed in was created by
			 * GetSegmentConfiguration, so if the mirror is first, swap them.
			 */
			segmentList[0], segmentList[1] = segmentList[1], segmentList[0]
		}
		cluster.ByHost[segment.Hostname] = append(cluster.ByHost[segment.Hostname], segment)
		if len(cluster.ByHost[segment.Hostname]) == 1 { // Only add each hostname once
			cluster.Hostnames = append(cluster.Hostnames, segment.Hostname)
		}
	}
	for content := range cluster.ByContent {
		cluster.ContentIDs = append(cluster.ContentIDs, content)
	}
	sort.Ints(cluster.ContentIDs)
	return &cluster
}

/*
 * Because cluster commands can be executed either per-segment or per-host, the
 * "generator" argument to this function can accept one of two types:
 * - func(int) []string, which takes a content id, for per-segment commands
 * - func(string) []string, which takes a hostname, for per-host commands
 * The function uses a type switch to identify the right one, and panics if
 * an invalid function type is passed in via programmer error.
 * This method makes it easier for the user to pass in whichever function fits
 * the kind of command they're generating, as opposed to having to pass in both
 * content and hostname regardless of scope or using some sort of helper struct.
 */
func (cluster *Cluster) GenerateCommandList(scope Scope, generator interface{}) []ShellCommand {
	commands := []ShellCommand{}
	switch generateCommand := generator.(type) {
	case func(content int) []string:
		for _, content := range cluster.ContentIDs {
			if content == -1 && scopeExcludesCoordinator(scope) {
				continue
			}
			commands = append(commands, NewShellCommand(scope, content, "", generateCommand(content)))
		}
	case func(host string) []string:
		for _, host := range cluster.Hostnames {
			hostHasOneContent := len(cluster.GetContentsForHost(host)) == 1
			if host == cluster.GetHostForContent(-1, "p") && scopeExcludesCoordinator(scope) && hostHasOneContent {
				// Only exclude the coordinator host if there are no local segments
				continue
			}
			if host == cluster.GetHostForContent(-1, "m") && scopeExcludesMirrors(scope) && hostHasOneContent {
				// Only exclude the standby coordinator host if there are no segments there
				continue
			}
			commands = append(commands, NewShellCommand(scope, -2, host, generateCommand(host)))
		}
	default:
		gplog.Fatal(nil, "Generator function passed to GenerateCommandList had an invalid function header.")
	}
	return commands
}

func ConstructSSHCommand(useLocal bool, host string, cmd string) []string {
	if useLocal {
		return []string{"bash", "-c", cmd}
	}
	currentUser, _ := operating.System.CurrentUser()
	user := currentUser.Username
	return []string{"ssh", "-o", "StrictHostKeyChecking=no", fmt.Sprintf("%s@%s", user, host), cmd}
}

/*
 * This function essentially wraps GenerateCommandList such that commands to be
 * executed on other hosts are sent through SSH and local commands use Bash.
 */
func (cluster *Cluster) GenerateSSHCommandList(scope Scope, generator interface{}) []ShellCommand {
	var commands []ShellCommand
	localHost := cluster.GetHostForContent(-1)
	switch generateCommand := generator.(type) {
	case func(content int) string:
		commands = cluster.GenerateCommandList(scope, func(content int) []string {
			useLocal := (cluster.GetHostForContent(content) == localHost || scopeIsLocal(scope))
			cmd := generateCommand(content)
			return ConstructSSHCommand(useLocal, cluster.GetHostForContent(content), cmd)
		})
	case func(host string) string:
		commands = cluster.GenerateCommandList(scope, func(host string) []string {
			useLocal := (host == localHost || scopeIsLocal(scope))
			cmd := generateCommand(host)
			return ConstructSSHCommand(useLocal, host, cmd)
		})
	}
	return commands
}

func (executor *GPDBExecutor) ExecuteLocalCommand(commandStr string) (string, error) {
	output, err := exec.Command("bash", "-c", commandStr).CombinedOutput()
	return string(output), err
}

/*
 * This function just executes all of the commands passed to it in parallel; it
 * doesn't care about the scope of the command except to pass that on to the
 * RemoteOutput after execution.
 * TODO: Add batching to prevent bottlenecks when executing in a huge cluster.
 */
func (executor *GPDBExecutor) ExecuteClusterCommand(scope Scope, commandList []ShellCommand) *RemoteOutput {
	length := len(commandList)
	finished := make(chan int)
	numErrors := 0
	for i := range commandList {
		go func(index int) {
			command := commandList[index]
			var stderr bytes.Buffer
			cmd := command.Command
			cmd.Stderr = &stderr
			out, err := cmd.Output()
			command.Stdout = string(out)
			command.Stderr = stderr.String()
			command.Error = err
			command.Completed = true
			commandList[index] = command
			finished <- index
		}(i)
	}
	for i := 0; i < length; i++ {
		index := <-finished
		if commandList[index].Error != nil {
			numErrors++
		}
	}
	return NewRemoteOutput(scope, numErrors, commandList)
}

/*
 * GenerateAndExecuteCommand and CheckClusterError are generic wrapper functions
 * to simplify execution of...
 * 1. shell commands directly on remote hosts via ssh.
 *    - e.g. running an ls on all hosts
 * 2. shell commands on coordinator to push to remote hosts.
 *    - e.g. running multiple scps on coordinator to push a file to all segments
 */
func (cluster *Cluster) GenerateAndExecuteCommand(verboseMsg string, scope Scope, generator interface{}) *RemoteOutput {
	gplog.Verbose(verboseMsg)
	commandList := cluster.GenerateSSHCommandList(scope, generator)
	return cluster.ExecuteClusterCommand(scope, commandList)
}

func (cluster *Cluster) CheckClusterError(remoteOutput *RemoteOutput, finalErrMsg string, messageFunc interface{}, noFatal ...bool) {
	if remoteOutput.NumErrors == 0 {
		return
	}
	for _, failedCommand := range remoteOutput.FailedCommands {
		errStr := fmt.Sprintf("with error %s: %s", failedCommand.Error, failedCommand.Stderr)
		switch getMessage := messageFunc.(type) {
		case func(content int) string:
			content := failedCommand.Content
			host := cluster.GetHostForContent(content)
			gplog.Verbose("%s on segment %d on host %s %s", getMessage(content), content, host, errStr)
		case func(host string) string:
			host := failedCommand.Host
			gplog.Verbose("%s on host %s %s", getMessage(host), host, errStr)
		}
		gplog.Verbose("Command was: %s", failedCommand.CommandString)
	}

	if len(noFatal) == 1 && noFatal[0] == true {
		gplog.Error(finalErrMsg)
	} else {
		LogFatalClusterError(finalErrMsg, remoteOutput.Scope, remoteOutput.NumErrors)
	}
}

func LogFatalClusterError(errMessage string, scope Scope, numErrors int) {
	str := " on"
	if scopeIsLocal(scope) {
		str += " coordinator for" // No good way to toggle "coordinator" vs. "master" here based on version, so default to "coordinator"
	}
	errMessage += str

	segMsg := "segment"
	if scopeIsHosts(scope) {
		segMsg = "host"
	}
	if numErrors != 1 {
		segMsg += "s"
	}
	gplog.Fatal(errors.Errorf("%s %d %s. See %s for a complete list of errors.", errMessage, numErrors, segMsg, gplog.GetLogFilePath()), "")
}

/*
 * Due to how NewCluster sets up ByContent, each content key points to a pair
 * of segments with the primary first and mirror second.  As most users of
 * Cluster are only going to care about primaries, by default each of the
 * Get[Foo]ForContent functions below returns the primary value by default,
 * and an optional parameter can be passed to specify which value is desired.
 */

func getSegmentByRole(segmentList []*SegConfig, role ...string) *SegConfig {
	if len(role) == 1 && role[0] == "m" {
		if len(segmentList) < 2 {
			return nil
		}
		return segmentList[1]
	}
	return segmentList[0]
}

func (cluster *Cluster) GetDbidForContent(contentID int, role ...string) int {
	segConfig := getSegmentByRole(cluster.ByContent[contentID], role...)
	if segConfig == nil {
		return -1
	}
	return segConfig.DbID
}

func (cluster *Cluster) GetPortForContent(contentID int, role ...string) int {
	segConfig := getSegmentByRole(cluster.ByContent[contentID], role...)
	if segConfig == nil {
		return -1
	}
	return segConfig.Port
}

func (cluster *Cluster) GetHostForContent(contentID int, role ...string) string {
	segConfig := getSegmentByRole(cluster.ByContent[contentID], role...)
	if segConfig == nil {
		return ""
	}
	return segConfig.Hostname
}

func (cluster *Cluster) GetDirForContent(contentID int, role ...string) string {
	segConfig := getSegmentByRole(cluster.ByContent[contentID], role...)
	if segConfig == nil {
		return ""
	}
	return segConfig.DataDir
}

func (cluster *Cluster) GetDbidsForHost(hostname string) []int {
	dbids := make([]int, len(cluster.ByHost[hostname]))
	for i, seg := range cluster.ByHost[hostname] {
		dbids[i] = seg.DbID
	}
	return dbids
}

func (cluster *Cluster) GetContentsForHost(hostname string) []int {
	contents := make([]int, len(cluster.ByHost[hostname]))
	for i, seg := range cluster.ByHost[hostname] {
		contents[i] = seg.ContentID
	}
	return contents
}

func (cluster *Cluster) GetPortsForHost(hostname string) []int {
	ports := make([]int, len(cluster.ByHost[hostname]))
	for i, seg := range cluster.ByHost[hostname] {
		ports[i] = seg.Port
	}
	return ports
}

func (cluster *Cluster) GetDirsForHost(hostname string) []string {
	dirs := make([]string, len(cluster.ByHost[hostname]))
	for i, seg := range cluster.ByHost[hostname] {
		dirs[i] = seg.DataDir
	}
	return dirs
}

/*
 * Helper functions
 */

/*
 * This function accepts up to two booleans:
 * By default, it retrieves only primary and coordinator information.
 * If the first boolean is set to true, it also retrieves mirror and standby information.
 * If the second is set to true, it retrieves only mirror and standby information, regardless of the value of the first boolean.
 */
func GetSegmentConfiguration(connection *dbconn.DBConn, getMirrors ...bool) ([]SegConfig, error) {
	includeMirrors := len(getMirrors) == 1 && getMirrors[0]
	includeOnlyMirrors := len(getMirrors) == 2 && getMirrors[1]
	query := ""
	if connection.Version.IsGPDB() && connection.Version.Before("6") {
		whereClause := "WHERE%s f.fsname = 'pg_system'"
		if includeOnlyMirrors {
			whereClause = fmt.Sprintf(whereClause, " s.role = 'm' AND")
		} else if includeMirrors {
			whereClause = fmt.Sprintf(whereClause, "")
		} else {
			whereClause = fmt.Sprintf(whereClause, " s.role = 'p' AND")
		}
		query = fmt.Sprintf(`
SELECT
	s.dbid,
	s.content as contentid,
	s.role,
	s.port,
	s.hostname,
	e.fselocation as datadir
FROM gp_segment_configuration s
JOIN pg_filespace_entry e ON s.dbid = e.fsedbid
JOIN pg_filespace f ON e.fsefsoid = f.oid
%s
ORDER BY s.content, s.role DESC;`, whereClause)
	} else {
		whereClause := "WHERE role = 'p'"
		if includeOnlyMirrors {
			whereClause = "WHERE role = 'm'"
		} else if includeMirrors {
			whereClause = ""
		}
		query = fmt.Sprintf(`
SELECT
	dbid,
	content as contentid,
	role,
	port,
	hostname,
	datadir
FROM gp_segment_configuration
%s
ORDER BY content, role DESC;`, whereClause)
	}

	results := make([]SegConfig, 0)
	err := connection.Select(&results, query)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func MustGetSegmentConfiguration(connection *dbconn.DBConn, getMirrors ...bool) []SegConfig {
	segConfigs, err := GetSegmentConfiguration(connection, len(getMirrors) == 1 && getMirrors[0])
	gplog.FatalOnError(err)
	return segConfigs
}
