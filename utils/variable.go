package utils

import (
	"fmt"
	"sync"

	"github.com/apache/cloudberry-go-libs/gplog"
	"github.com/spf13/pflag"

	"github.com/cloudberry-contrib/cbcopy/internal/dbconn"
)

/*
 * Non-flag variables
 */
var (
	Version       string
	WasTerminated bool
	fmu           sync.Mutex
	/*
	 * Used for synchronizing DoCleanup.  In DoInit() we increment the group
	 * and then wait for at least one DoCleanup to finish, either in DoTeardown
	 * or the signal handler.
	 */
	CleanupGroup *sync.WaitGroup

	// --> automation test purpose, start
	connectionPool     *dbconn.DBConn
	pipeThroughProgram PipeThroughProgram
	// <-- automation test purpose, end
)

/*
 * Command-line flags
 */
var CmdFlags *pflag.FlagSet

/*
 * Setter functions
 */

func SetCmdFlags(flagSet *pflag.FlagSet) {
	CmdFlags = flagSet
}

func SetConnection(conn *dbconn.DBConn) {
	connectionPool = conn
}

func SetVersion(v string) {
	Version = v
}

// Util functions to enable ease of access to global flag values

func MustGetFlagString(flagName string) string {
	value, err := CmdFlags.GetString(flagName)
	gplog.FatalOnError(err)
	return value
}

func MustGetFlagInt(flagName string) int {
	value, err := CmdFlags.GetInt(flagName)
	gplog.FatalOnError(err)
	return value
}

func MustGetFlagBool(flagName string) bool {
	value, err := CmdFlags.GetBool(flagName)
	gplog.FatalOnError(err)
	return value
}

func MustGetFlagStringSlice(flagName string) []string {
	value, err := CmdFlags.GetStringSlice(flagName)
	gplog.FatalOnError(err)
	return value
}

func MustGetFlagStringArray(flagName string) []string {
	value, err := CmdFlags.GetStringArray(flagName)
	gplog.FatalOnError(err)
	return value
}

// --> automation test purpose, start

type PipeThroughProgram struct {
	Name          string
	OutputCommand string
	InputCommand  string
	Extension     string
}

func InitializePipeThroughParameters(compress bool, compressionType string, compressionLevel int) {
	if !compress {
		pipeThroughProgram = PipeThroughProgram{Name: "cat", OutputCommand: "cat -", InputCommand: "cat -", Extension: ""}
		return
	}

	// backward compatibility for inputs without compressionType
	if compressionType == "" {
		compressionType = "gzip"
	}

	if compressionType == "gzip" {
		pipeThroughProgram = PipeThroughProgram{Name: "gzip", OutputCommand: fmt.Sprintf("gzip -c -%d", compressionLevel), InputCommand: "gzip -d -c", Extension: ".gz"}
		return
	}

	if compressionType == "zstd" {
		pipeThroughProgram = PipeThroughProgram{Name: "zstd", OutputCommand: fmt.Sprintf("zstd --compress -%d -c", compressionLevel), InputCommand: "zstd --decompress -c", Extension: ".zst"}
		return
	}
}

func GetPipeThroughProgram() PipeThroughProgram {
	return pipeThroughProgram
}

func SetPipeThroughProgram(compression PipeThroughProgram) {
	pipeThroughProgram = compression
}

// <-- automation test purpose, end
