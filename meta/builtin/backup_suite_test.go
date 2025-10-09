package builtin_test

/*
 * This file contains integration tests for gpbackup as a whole, rather than
 * tests relating to functions in any particular file.
 */

import (
	"github.com/cloudberry-contrib/cbcopy/internal/dbconn"
	"github.com/cloudberry-contrib/cbcopy/meta/builtin"
	"github.com/cloudberry-contrib/cbcopy/meta/builtin/toc"
	"github.com/cloudberry-contrib/cbcopy/testutils"
	"github.com/cloudberry-contrib/cbcopy/utils"

	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	//"github.com/greenplum-db/gp-common-go-libs/dbconn"
	//"github.com/greenplum-db/gpbackup/backup"
	//"github.com/greenplum-db/gpbackup/testutils"
	//"github.com/greenplum-db/gpbackup/toc"
	//"github.com/greenplum-db/gpbackup/utils"
	"github.com/spf13/pflag"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
)

var (
	connectionPool *dbconn.DBConn
	defaultConnNum = 0
	mock           sqlmock.Sqlmock
	stdout         *Buffer
	stderr         *Buffer
	logfile        *Buffer
	buffer         *Buffer
	tocfile        *toc.TOC
	backupfile     *utils.FileWithByteCount
)

func TestBackup(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "backup tests")
}

var cmdFlags *pflag.FlagSet

var _ = BeforeEach(func() {
	cmdFlags = pflag.NewFlagSet("gpbackup", pflag.ExitOnError)

	builtin.SetCmdFlags(cmdFlags)

	utils.SetPipeThroughProgram(utils.PipeThroughProgram{})

	connectionPool, mock, stdout, stderr, logfile = testutils.SetupTestEnvironment()
	builtin.SetConnection(connectionPool) // hack for automation test, todo: refactor in the future
	builtin.InitializeMetadataParams(connectionPool)
	buffer = NewBuffer()
})

/*
 * While this function is technically redundant with dbconn.NewVersion, it's
 * here to allow `defer`ing version changes easily, instead of needing e.g.
 * "defer func() { connection.Version = dbconn.NewVersion(versionStr) }()" or
 * something similarly ugly.
 */
func SetDBVersion(connection *dbconn.DBConn, versionStr string) {
	connection.Version = dbconn.NewVersion(versionStr)
	builtin.SetConnection(connection) // hack for automation test, todo: refactor in the future
}
