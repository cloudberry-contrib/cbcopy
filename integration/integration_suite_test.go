package integration

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/cloudberrydb/cbcopy/options"
	"github.com/greenplum-db/gp-common-go-libs/gplog"

	// "github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/cloudberrydb/cbcopy/internal/cluster"

	// "github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/cloudberrydb/cbcopy/internal/dbconn"

	// "github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/cloudberrydb/cbcopy/internal/testhelper"

	// "github.com/greenplum-db/gpbackup/backup"
	"github.com/cloudberrydb/cbcopy/meta/builtin"

	//"github.com/greenplum-db/gpbackup/restore"

	// "github.com/greenplum-db/gpbackup/testutils"
	"github.com/cloudberrydb/cbcopy/testutils"

	// "github.com/greenplum-db/gpbackup/toc"
	"github.com/cloudberrydb/cbcopy/meta/builtin/toc"

	// "github.com/greenplum-db/gpbackup/utils"
	"github.com/cloudberrydb/cbcopy/utils"

	"github.com/cloudberrydb/cbcopy/copy"

	"github.com/spf13/pflag"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
	. "github.com/onsi/gomega/gexec"
)

var (
	buffer             *bytes.Buffer
	connectionPool     *dbconn.DBConn
	tocfile            *toc.TOC
	backupfile         *utils.FileWithByteCount
	testCluster        *cluster.Cluster
	gpbackupHelperPath string
	stderr, logFile    *Buffer

	// GUC defaults. Initially set to GPDB4 values
	concurrencyDefault    = "20"
	memSharedDefault      = "20"
	memSpillDefault       = "20"
	memAuditDefault       = "0"
	cpuSetDefault         = "-1"
	includeSecurityLabels = false
)

func TestQueries(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "database query tests")
}

func runCommand(cmd string, args ...string) {
	fmt.Printf("    -- runCommand, %v %v\n", cmd, args)
	gplog.Debug("    -- runCommand, %v %v\n", cmd, args)
	command := exec.Command(cmd, args...)
	output, err := command.CombinedOutput()
	if err != nil && !strings.Contains(err.Error(), "not exist") && !strings.Contains(string(output), "not exist") {
		fmt.Printf("%s", output)
		Fail(fmt.Sprintf("%v", err))
	}
}

var _ = BeforeSuite(func() {

	// terry. this testhelper.SetupTestLogger() does not print to log file when callin gplog, which is not convenient for debug
	// _, stderr, logFile = testhelper.SetupTestLogger()
	gplog.InitializeLogging("cbcopy_integration_test", "")

	fmt.Print("== suite start ===\n\n")
	gplog.Debug("\n\n== suite start ===\n\n")

	runCommand("dropdb", "testdb")
	runCommand("createdb", "testdb")
	runCommand("dropdb", "testdb")
	runCommand("createdb", "testdb")

	/*
		_ = exec.Command("dropdb", "testdb").Run()
		err := exec.Command("createdb", "testdb").Run()
		if err != nil {
			Fail("Cannot create database testdb; is GPDB running?")
		}
		Expect(err).To(BeNil())
	*/

	connectionPool = testutils.SetupTestDbConn("testdb")

	// We can't use AssertQueryRuns since if a role already exists it will error
	fmt.Printf("connectionPool.Exec, query is: %v\n\n", "CREATE ROLE testrole SUPERUSER")
	gplog.Debug("\n\nconnectionPool.Exec, query is: %v\n\n", "CREATE ROLE testrole SUPERUSER")
	_, _ = connectionPool.Exec("CREATE ROLE testrole SUPERUSER")

	fmt.Printf("connectionPool.Exec, query is: %v\n\n", "CREATE ROLE anothertestrole SUPERUSER")
	gplog.Debug("\n\nconnectionPool.Exec, query is: %v\n\n", "CREATE ROLE anothertestrole SUPERUSER")
	_, _ = connectionPool.Exec("CREATE ROLE anothertestrole SUPERUSER")

	builtin.InitializeMetadataParams(connectionPool)
	builtin.SetConnection(connectionPool)
	segConfig := cluster.MustGetSegmentConfiguration(connectionPool)
	testCluster = cluster.NewCluster(segConfig)
	testhelper.AssertQueryRuns(connectionPool, "SET ROLE testrole")
	testhelper.AssertQueryRuns(connectionPool, "ALTER DATABASE testdb OWNER TO anothertestrole")
	testhelper.AssertQueryRuns(connectionPool, "ALTER SCHEMA public OWNER TO anothertestrole")
	testhelper.AssertQueryRuns(connectionPool, "DROP PROTOCOL IF EXISTS gphdfs")
	testhelper.AssertQueryRuns(connectionPool, `SET standard_conforming_strings TO "on"`)
	testhelper.AssertQueryRuns(connectionPool, `SET search_path=pg_catalog`)
	if connectionPool.Version.Before("6") {
		testhelper.AssertQueryRuns(connectionPool, "SET allow_system_table_mods = 'DML'")
		testutils.SetupTestFilespace(connectionPool, testCluster)
	} else {
		testhelper.AssertQueryRuns(connectionPool, "SET allow_system_table_mods = true")

		remoteOutput := testCluster.GenerateAndExecuteCommand(
			"Creating filespace test directories on all hosts",
			cluster.ON_HOSTS|cluster.INCLUDE_COORDINATOR,
			func(contentID int) string {
				return fmt.Sprintf("mkdir -p /tmp/test_dir && mkdir -p /tmp/test_dir1 && mkdir -p /tmp/test_dir2")
			})
		if remoteOutput.NumErrors != 0 {
			Fail("Could not create filespace test directory on 1 or more hosts")
		}
	}

	gpbackupHelperPath = buildAndInstallBinaries()

	// Set GUC Defaults and version logic
	if connectionPool.Version.AtLeast("6") {
		memSharedDefault = "80"
		memSpillDefault = "0"

		includeSecurityLabels = true
	}

	// extra action, for automation test purpose
	_, _ = connectionPool.Exec("drop extension plpython3u;")
})

var backupCmdFlags *pflag.FlagSet

//var restoreCmdFlags *pflag.FlagSet

var _ = BeforeEach(func() {
	fmt.Print("== case start ===\n\n")
	gplog.Debug("\n\n== case start ===\n\n")

	buffer = bytes.NewBuffer([]byte(""))

	/* gpbackup code,

	backupCmdFlags = pflag.NewFlagSet("gpbackup", pflag.ExitOnError)
	backup.SetCmdFlags(backupCmdFlags)
	backup.SetFilterRelationClause("")
	*/
	backupCmdFlags = pflag.NewFlagSet("gpbackup", pflag.ExitOnError)
	builtin.SetCmdFlags(backupCmdFlags)
	builtin.SetFilterRelationClause("")

	// terry note: previous builtin.SetCmdFlags does not take effect, maybe it's because gpbackup code structure is not same as cbcopy, below code is adaptive change with respect to cbcopy code
	copy.SetFlagDefaults(backupCmdFlags)
	utils.CmdFlags = backupCmdFlags

	// reset configuration in testcase, otherwise one testcase config setting might impact other testcase execution
	builtin.SetSchemaFilter(options.SCHEMA, "")
	builtin.SetSchemaFilter(options.EXCLUDE_SCHEMA, "")
	builtin.SetRelationFilter(options.INCLUDE_TABLE, "")
	builtin.SetRelationFilter(options.EXCLUDE_TABLE, "")

	//restoreCmdFlags = pflag.NewFlagSet("gprestore", pflag.ExitOnError)
	//restore.SetCmdFlags(restoreCmdFlags)
})

var _ = AfterEach(func() {
	fmt.Print("== case finish ===\n\n")
	gplog.Debug("\n\n== case finish ===\n\n")
})

var _ = AfterSuite(func() {
	fmt.Print("== suite done ===\n\n")
	gplog.Debug("\n\n== suite done ===\n\n")

	CleanupBuildArtifacts()
	if connectionPool.Version.Before("6") {
		testutils.DestroyTestFilespace(connectionPool)
	} else {
		remoteOutput := testCluster.GenerateAndExecuteCommand(
			"Removing /tmp/test_dir* directories on all hosts",
			cluster.ON_HOSTS|cluster.INCLUDE_COORDINATOR,
			func(contentID int) string {
				return fmt.Sprintf("rm -rf /tmp/test_dir*")
			})
		if remoteOutput.NumErrors != 0 {
			Fail("Could not remove /tmp/testdir* directories on 1 or more hosts")
		}
	}
	if connectionPool != nil {
		connectionPool.Close()
		err := exec.Command("dropdb", "testdb").Run()
		Expect(err).To(BeNil())
	}
	connection1 := testutils.SetupTestDbConn("template1")
	testhelper.AssertQueryRuns(connection1, "DROP ROLE testrole")
	testhelper.AssertQueryRuns(connection1, "DROP ROLE anothertestrole")
	connection1.Close()
	_ = os.RemoveAll("/tmp/helper_test")
	_ = os.RemoveAll("/tmp/plugin_dest")
})
