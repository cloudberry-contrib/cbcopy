package testhelper

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"

	// "github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/cloudberry-contrib/cbcopy/internal/dbconn"

	"github.com/jmoiron/sqlx"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

/*
 * Functions for setting up the test environment and mocking out variables
 */

func SetupTestLogger() (*gbytes.Buffer, *gbytes.Buffer, *gbytes.Buffer) {
	testStdout := gbytes.NewBuffer()
	testStderr := gbytes.NewBuffer()
	testLogfile := gbytes.NewBuffer()
	testLogger := gplog.NewLogger(testStdout, testStderr, testLogfile, "gbytes.Buffer", gplog.LOGINFO, "testProgram")
	gplog.SetLogger(testLogger)
	return testStdout, testStderr, testLogfile
}

func SetupTestEnvironment() (*dbconn.DBConn, sqlmock.Sqlmock, *gbytes.Buffer, *gbytes.Buffer, *gbytes.Buffer) {
	testStdout, testStderr, testLogfile := SetupTestLogger()
	connection, mock := CreateAndConnectMockDB(1)
	operating.System = operating.InitializeSystemFunctions()
	return connection, mock, testStdout, testStderr, testLogfile
}

func CreateMockDB() (*sqlx.DB, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	mockdb := sqlx.NewDb(db, "sqlmock")
	Expect(err).To(BeNil(), "Could not create mock database connection")

	return mockdb, mock
}

/*
 * While this function is technically redundant with dbconn.NewVersion, it's
 * here to allow `defer`ing version changes easily, instead of needing e.g.
 * "defer func() { connection.Version = dbconn.NewVersion(versionStr) }()" or
 * something similarly ugly.
 */
func SetDBVersion(connection *dbconn.DBConn, versionStr string) {
	connection.Version = dbconn.NewVersion(versionStr)
}

func CreateMockDBConn(errs ...error) (*dbconn.DBConn, sqlmock.Sqlmock) {
	mockdb, mock := CreateMockDB()
	driver := &TestDriver{DB: mockdb, DBName: "testdb", User: "testrole"}
	if len(errs) > 0 {
		driver.ErrsToReturn = errs
	}
	connection := dbconn.NewDBConnFromEnvironment("testdb")
	connection.Driver = driver
	connection.Host = "testhost"
	connection.Port = 5432
	return connection, mock
}

func ExpectVersionQuery(mock sqlmock.Sqlmock, versionStr string) {
	versionRow := sqlmock.NewRows([]string{"versionstring"}).AddRow(fmt.Sprintf("(Greenplum Database %s)", versionStr))
	//mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_catalog.version() AS versionstring")).WillReturnRows(versionRow)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT version() AS versionstring")).WillReturnRows(versionRow)
}

func CreateAndConnectMockDB(numConns int) (*dbconn.DBConn, sqlmock.Sqlmock) {
	connection, mock := CreateMockDBConn()
	ExpectVersionQuery(mock, "5.1.0")
	connection.MustConnect(numConns)
	return connection, mock
}

func ExpectRegexp(buffer *gbytes.Buffer, testStr string) {
	Expect(buffer).Should(gbytes.Say(regexp.QuoteMeta(testStr)))
}

func NotExpectRegexp(buffer *gbytes.Buffer, testStr string) {
	Expect(buffer).ShouldNot(gbytes.Say(regexp.QuoteMeta(testStr)))
}

func ShouldPanicWithMessage(message string) {
	r := recover()
	Expect(r).NotTo(BeNil(), "Function did not panic as expected")

	errorMessage := strings.TrimSpace(fmt.Sprintf("%v", r))
	Expect(errorMessage).Should(ContainSubstring(message))
}

func AssertQueryRuns(connection *dbconn.DBConn, query string) {
	//terry, comment out this line to print out what query is executed
	fmt.Printf("AssertQueryRuns, query is: %v\n\n", query)
	gplog.Debug("\n\nAssertQueryRuns, query is: %v\n\n", query)

	_, err := connection.Exec(query)
	Expect(err).To(BeNil(), "%s", query)
}

/*
 * This function call should be followed by a call to InitializeSystemFunctions
 * in a defer statement or AfterEach block.
 */
func MockFileContents(contents string) {
	r, w, _ := os.Pipe()
	operating.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (operating.ReadCloserAt, error) { return r, nil }
	_, _ = w.Write([]byte(contents))
	_ = w.Close()
}
