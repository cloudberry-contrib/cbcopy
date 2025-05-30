package testutils

import (
	"fmt"
	"path/filepath"
	"regexp"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"

	//"github.com/greenplum-db/gpbackup/backup"
	"io"
	"os/exec"
	"strings"

	"github.com/cloudberrydb/cbcopy/meta/builtin"
	"github.com/sergi/go-diff/diffmatchpatch"

	//"github.com/greenplum-db/gp-common-go-libs/cluster"
	//"github.com/greenplum-db/gpbackup/backup"
	//"github.com/greenplum-db/gpbackup/filepath"
	//"github.com/greenplum-db/gpbackup/restore"

	//"github.com/greenplum-db/gp-common-go-libs/cluster"
	//"github.com/greenplum-db/gpbackup/backup"
	//"github.com/greenplum-db/gpbackup/filepath"
	//"github.com/greenplum-db/gpbackup/restore"
	"os"

	// "github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/cloudberrydb/cbcopy/internal/cluster"

	// "github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/cloudberrydb/cbcopy/internal/testhelper"

	//"github.com/greenplum-db/gpbackup/backup"
	//"github.com/greenplum-db/gpbackup/filepath"
	//"github.com/greenplum-db/gpbackup/restore"
	//"github.com/greenplum-db/gpbackup/toc"
	//"github.com/greenplum-db/gpbackup/utils"
	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/meta/builtin/toc"
	"github.com/cloudberrydb/cbcopy/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
)

/*
 * Functions for setting up the test environment and mocking out variables
 */

func SetupTestEnvironment() (*dbconn.DBConn, sqlmock.Sqlmock, *Buffer, *Buffer, *Buffer) {
	connectionPool, mock, testStdout, testStderr, testLogfile := testhelper.SetupTestEnvironment()

	// Default if not set is GPDB version `5.1.0`
	envTestGpdbVersion := os.Getenv("TEST_GPDB_VERSION")
	envTestGpdbVersion = "7.0.0"
	if envTestGpdbVersion != "" {
		testhelper.SetDBVersion(connectionPool, envTestGpdbVersion)
	}

	// SetupTestCluster()

	//builtin.SetVersion("0.1.0")

	var x *dbconn.DBConn

	//x = (*dbconn.DBConn)(connectionPool) // Cannot convert an expression of the type '*dbconn.DBConn' to the type '*dbconn.DBConn'

	/**

	(cbcopy) internal/dbconn/dbconn.go

	func NewDBConn
		return &DBConn{
		ConnPool:   nil,
		NumConns:   0,
		Driver:     GPDBDriver{},
		User:       username,
		Password:   "",
		DBName:     dbname,
		Host:       host,
		Port:       port,
		Tx:         nil,
		Version:    GPDBVersion{},
		HdwVersion: GPDBVersion{},
	}

	(gp-common-go-lib) dbconn/dbconn.go

		return &DBConn{
		ConnPool: nil,
		NumConns: 0,
		Driver:   &GPDBDriver{},
		User:     username,
		DBName:   dbname,
		Host:     host,
		Port:     port,
		Tx:       nil,
		Version:  GPDBVersion{},
	}

	*/

	x = &dbconn.DBConn{
		ConnPool: connectionPool.ConnPool,
		NumConns: connectionPool.NumConns,
		Driver:   connectionPool.Driver,
		User:     connectionPool.User,
		Password: "",
		DBName:   connectionPool.DBName,
		Host:     connectionPool.Host,
		Port:     connectionPool.Port,
		Tx:       connectionPool.Tx,
		Version: dbconn.GPDBVersion{
			VersionString: connectionPool.Version.VersionString,
			SemVer:        connectionPool.Version.SemVer,
			Type:          connectionPool.Version.Type,
		},
	}

	return x, mock, testStdout, testStderr, testLogfile
}

/* //
func SetupTestCluster() *cluster.Cluster {
	testCluster := SetDefaultSegmentConfiguration()
	builtin.SetCluster(testCluster)
	restore.SetCluster(testCluster)
	testFPInfo := filepath.NewFilePathInfo(testCluster, "", "20170101010101", "gpseg")
	builtin.SetFPInfo(testFPInfo)
	restore.SetFPInfo(testFPInfo)
	return testCluster
}

*/

func SetupTestDbConn(dbname string) *dbconn.DBConn {
	conn := dbconn.NewDBConnFromEnvironment(dbname)
	conn.MustConnect(1)
	return conn
}

// Connects to specific segment in utility mode
func SetupTestDBConnSegment(dbname string, port int, host string, gpVersion dbconn.GPDBVersion) *dbconn.DBConn {

	if dbname == "" {
		gplog.Fatal(errors.New("No database provided"), "")
	}
	if port == 0 {
		gplog.Fatal(errors.New("No segment port provided"), "")
	}
	// Don't fail if no host is passed, as that implies connecting on the local host
	username := operating.System.Getenv("PGUSER")
	if username == "" {
		currentUser, _ := operating.System.CurrentUser()
		username = currentUser.Username
	}
	if host == "" {
		host := operating.System.Getenv("PGHOST")
		if host == "" {
			host, _ = operating.System.Hostname()
		}
	}

	conn := &dbconn.DBConn{
		ConnPool: nil,
		NumConns: 0,
		Driver:   &dbconn.GPDBDriver{},
		User:     username,
		DBName:   dbname,
		Host:     host,
		Port:     port,
		Tx:       nil,
		Version:  dbconn.GPDBVersion{},
	}

	var gpRoleGuc string
	if gpVersion.IsGPDB() && gpVersion.Before("7") {
		gpRoleGuc = "gp_session_role"
	} else {
		gpRoleGuc = "gp_role"
	}

	connStr := fmt.Sprintf("postgres://%s@%s:%d/%s?sslmode=disable&statement_cache_capacity=0&%s=utility", conn.User, conn.Host, conn.Port, conn.DBName, gpRoleGuc)

	segConn, err := conn.Driver.Connect("pgx", connStr)
	if err != nil {
		gplog.FatalOnError(err)
	}
	conn.ConnPool = make([]*sqlx.DB, 1)
	conn.ConnPool[0] = segConn

	conn.Tx = make([]*sqlx.Tx, 1)
	conn.NumConns = 1

	version, err := dbconn.InitializeVersion(conn)

	if err != nil {
		gplog.FatalOnError(err)
	}
	conn.Version = version
	return conn
}

/*
 */

/* //
func SetDefaultSegmentConfiguration() *cluster.Cluster {
	configCoordinator := cluster.SegConfig{ContentID: -1, Hostname: "localhost", DataDir: "gpseg-1"}
	configSegOne := cluster.SegConfig{ContentID: 0, Hostname: "localhost", DataDir: "gpseg0"}
	configSegTwo := cluster.SegConfig{ContentID: 1, Hostname: "localhost", DataDir: "gpseg1"}
	return cluster.NewCluster([]cluster.SegConfig{configCoordinator, configSegOne, configSegTwo})
}
*/

func SetupTestFilespace(connectionPool *dbconn.DBConn, testCluster *cluster.Cluster) {
	remoteOutput := testCluster.GenerateAndExecuteCommand("Creating filespace test directory",
		cluster.ON_HOSTS|cluster.INCLUDE_COORDINATOR,
		func(contentID int) string {
			return fmt.Sprintf("mkdir -p /tmp/test_dir")
		})
	if remoteOutput.NumErrors != 0 {
		Fail("Could not create filespace test directory on 1 or more hosts")
	}
	// Construct a filespace config like the one that gpfilespace generates
	filespaceConfigQuery := `COPY (SELECT hostname || ':' || dbid || ':/tmp/test_dir/' || preferred_role || content FROM gp_segment_configuration AS subselect) TO '/tmp/temp_filespace_config';`
	testhelper.AssertQueryRuns(connectionPool, filespaceConfigQuery)
	out, err := exec.Command("bash", "-c", "echo \"filespace:test_dir\" > /tmp/filespace_config").CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Cannot create test filespace configuration: %s: %s", out, err.Error()))
	}
	out, err = exec.Command("bash", "-c", "cat /tmp/temp_filespace_config >> /tmp/filespace_config").CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Cannot finalize test filespace configuration: %s: %s", out, err.Error()))
	}
	// Create the filespace and verify it was created successfully
	out, err = exec.Command("bash", "-c", "gpfilespace --config /tmp/filespace_config").CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Cannot create test filespace: %s: %s", out, err.Error()))
	}
	filespaceName := dbconn.MustSelectString(connectionPool, "SELECT fsname AS string FROM pg_filespace WHERE fsname = 'test_dir';")
	if filespaceName != "test_dir" {
		Fail("Filespace test_dir was not successfully created")
	}
}

func DestroyTestFilespace(connectionPool *dbconn.DBConn) {
	filespaceName := dbconn.MustSelectString(connectionPool, "SELECT fsname AS string FROM pg_filespace WHERE fsname = 'test_dir';")
	if filespaceName != "test_dir" {
		return
	}
	testhelper.AssertQueryRuns(connectionPool, "DROP FILESPACE test_dir")
	out, err := exec.Command("bash", "-c", "rm -rf /tmp/test_dir /tmp/filespace_config /tmp/temp_filespace_config").CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Could not remove test filespace directory and configuration files: %s: %s", out, err.Error()))
	}
}

/*
 */

func DefaultMetadata(objType string, hasPrivileges bool, hasOwner bool, hasComment bool, hasSecurityLabel bool) builtin.ObjectMetadata {
	privileges := make([]builtin.ACL, 0)
	if hasPrivileges {
		privileges = []builtin.ACL{DefaultACLForType("testrole", objType)}
	}
	owner := ""
	if hasOwner {
		owner = "testrole"
	}
	comment := ""
	if hasComment {
		n := ""
		switch objType[0] {
		case 'A', 'E', 'I', 'O', 'U':
			n = "n"
		}
		comment = fmt.Sprintf("This is a%s %s comment.", n, strings.ToLower(objType))
	}
	securityLabelProvider := ""
	securityLabel := ""
	if hasSecurityLabel {
		securityLabelProvider = "dummy"
		securityLabel = "unclassified"
	}
	switch objType {
	case "DOMAIN":
		objType = "TYPE"
	case "FOREIGN SERVER":
		objType = "SERVER"
	case "MATERIALIZED VIEW":
		objType = "RELATION"
	case "SEQUENCE":
		objType = "RELATION"
	case "TABLE":
		objType = "RELATION"
	case "VIEW":
		objType = "RELATION"
	}
	return builtin.ObjectMetadata{
		Privileges:            privileges,
		ObjectType:            objType,
		Owner:                 owner,
		Comment:               comment,
		SecurityLabelProvider: securityLabelProvider,
		SecurityLabel:         securityLabel,
	}

}

// objType should be an all-caps string like TABLE, INDEX, etc.

func DefaultMetadataMap(objType string, hasPrivileges bool, hasOwner bool, hasComment bool, hasSecurityLabel bool) builtin.MetadataMap {
	return builtin.MetadataMap{
		builtin.UniqueID{ClassID: ClassIDFromObjectName(objType), Oid: 1}: DefaultMetadata(objType, hasPrivileges, hasOwner, hasComment, hasSecurityLabel),
	}
}

/**/
var objNameToClassID = map[string]uint32{
	"AGGREGATE":                 1255,
	"CAST":                      2605,
	"COLLATION":                 3456,
	"CONSTRAINT":                2606,
	"CONVERSION":                2607,
	"DATABASE":                  1262,
	"DOMAIN":                    1247,
	"EVENT TRIGGER":             3466,
	"EXTENSION":                 3079,
	"FOREIGN DATA WRAPPER":      2328,
	"FOREIGN SERVER":            1417,
	"FUNCTION":                  1255,
	"INDEX":                     2610,
	"LANGUAGE":                  2612,
	"OPERATOR CLASS":            2616,
	"OPERATOR FAMILY":           2753,
	"OPERATOR":                  2617,
	"PROTOCOL":                  7175,
	"RESOURCE GROUP":            6436,
	"RESOURCE QUEUE":            6026,
	"ROLE":                      1260,
	"RULE":                      2618,
	"SCHEMA":                    2615,
	"SEQUENCE":                  1259,
	"TABLE":                     1259,
	"TABLESPACE":                1213,
	"TEXT SEARCH CONFIGURATION": 3602,
	"TEXT SEARCH DICTIONARY":    3600,
	"TEXT SEARCH PARSER":        3601,
	"TEXT SEARCH TEMPLATE":      3764,
	"TRIGGER":                   2620,
	"TYPE":                      1247,
	"USER MAPPING":              1418,
	"VIEW":                      1259,
	"MATERIALIZED VIEW":         1259,
}

func ClassIDFromObjectName(objName string) uint32 {
	return objNameToClassID[objName]

}

func DefaultACLForType(grantee string, objType string) builtin.ACL {
	return builtin.ACL{
		Grantee:    grantee,
		Select:     objType == "PROTOCOL" || objType == "SEQUENCE" || objType == "TABLE" || objType == "VIEW" || objType == "FOREIGN TABLE" || objType == "MATERIALIZED VIEW",
		Insert:     objType == "PROTOCOL" || objType == "TABLE" || objType == "VIEW" || objType == "FOREIGN TABLE" || objType == "MATERIALIZED VIEW",
		Update:     objType == "SEQUENCE" || objType == "TABLE" || objType == "VIEW" || objType == "FOREIGN TABLE" || objType == "MATERIALIZED VIEW",
		Delete:     objType == "TABLE" || objType == "VIEW" || objType == "FOREIGN TABLE" || objType == "MATERIALIZED VIEW",
		Truncate:   objType == "TABLE" || objType == "VIEW" || objType == "MATERIALIZED VIEW",
		References: objType == "TABLE" || objType == "VIEW" || objType == "FOREIGN TABLE" || objType == "MATERIALIZED VIEW",
		Trigger:    objType == "TABLE" || objType == "VIEW" || objType == "FOREIGN TABLE" || objType == "MATERIALIZED VIEW",
		Usage:      objType == "LANGUAGE" || objType == "SCHEMA" || objType == "SEQUENCE" || objType == "FOREIGN DATA WRAPPER" || objType == "FOREIGN SERVER",
		Execute:    objType == "FUNCTION" || objType == "AGGREGATE",
		Create:     objType == "DATABASE" || objType == "SCHEMA" || objType == "TABLESPACE",
		Temporary:  objType == "DATABASE",
		Connect:    objType == "DATABASE",
	}
}

func DefaultACLForTypeWithGrant(grantee string, objType string) builtin.ACL {
	return builtin.ACL{
		Grantee:             grantee,
		SelectWithGrant:     objType == "PROTOCOL" || objType == "SEQUENCE" || objType == "TABLE" || objType == "VIEW" || objType == "MATERIALIZED VIEW",
		InsertWithGrant:     objType == "PROTOCOL" || objType == "TABLE" || objType == "VIEW" || objType == "MATERIALIZED VIEW",
		UpdateWithGrant:     objType == "SEQUENCE" || objType == "TABLE" || objType == "VIEW" || objType == "MATERIALIZED VIEW",
		DeleteWithGrant:     objType == "TABLE" || objType == "VIEW" || objType == "MATERIALIZED VIEW",
		TruncateWithGrant:   objType == "TABLE" || objType == "VIEW" || objType == "MATERIALIZED VIEW",
		ReferencesWithGrant: objType == "TABLE" || objType == "VIEW" || objType == "MATERIALIZED VIEW",
		TriggerWithGrant:    objType == "TABLE" || objType == "VIEW" || objType == "MATERIALIZED VIEW",
		UsageWithGrant:      objType == "LANGUAGE" || objType == "SCHEMA" || objType == "SEQUENCE" || objType == "FOREIGN DATA WRAPPER" || objType == "FOREIGN SERVER",
		ExecuteWithGrant:    objType == "FUNCTION",
		CreateWithGrant:     objType == "DATABASE" || objType == "SCHEMA" || objType == "TABLESPACE",
		TemporaryWithGrant:  objType == "DATABASE",
		ConnectWithGrant:    objType == "DATABASE",
	}
}

func DefaultACLWithout(grantee string, objType string, revoke ...string) builtin.ACL {
	defaultACL := DefaultACLForType(grantee, objType)
	for _, priv := range revoke {
		switch priv {
		case "SELECT":
			defaultACL.Select = false
		case "INSERT":
			defaultACL.Insert = false
		case "UPDATE":
			defaultACL.Update = false
		case "DELETE":
			defaultACL.Delete = false
		case "TRUNCATE":
			defaultACL.Truncate = false
		case "REFERENCES":
			defaultACL.References = false
		case "TRIGGER":
			defaultACL.Trigger = false
		case "EXECUTE":
			defaultACL.Execute = false
		case "USAGE":
			defaultACL.Usage = false
		case "CREATE":
			defaultACL.Create = false
		case "TEMPORARY":
			defaultACL.Temporary = false
		case "CONNECT":
			defaultACL.Connect = false
		}
	}
	return defaultACL
}

func DefaultACLWithGrantWithout(grantee string, objType string, revoke ...string) builtin.ACL {
	defaultACL := DefaultACLForTypeWithGrant(grantee, objType)
	for _, priv := range revoke {
		switch priv {
		case "SELECT":
			defaultACL.SelectWithGrant = false
		case "INSERT":
			defaultACL.InsertWithGrant = false
		case "UPDATE":
			defaultACL.UpdateWithGrant = false
		case "DELETE":
			defaultACL.DeleteWithGrant = false
		case "TRUNCATE":
			defaultACL.TruncateWithGrant = false
		case "REFERENCES":
			defaultACL.ReferencesWithGrant = false
		case "TRIGGER":
			defaultACL.TriggerWithGrant = false
		case "EXECUTE":
			defaultACL.ExecuteWithGrant = false
		case "USAGE":
			defaultACL.UsageWithGrant = false
		case "CREATE":
			defaultACL.CreateWithGrant = false
		case "TEMPORARY":
			defaultACL.TemporaryWithGrant = false
		case "CONNECT":
			defaultACL.ConnectWithGrant = false
		}
	}
	return defaultACL
}

//Wrapper functions around gomega operators for ease of use in tests

func SliceBufferByEntries(entries []toc.MetadataEntry, buffer *Buffer) ([]string, string) {
	contents := buffer.Contents()
	hunks := make([]string, 0)
	length := uint64(len(contents))
	var end uint64
	for _, entry := range entries {
		start := entry.StartByte
		end = entry.EndByte
		if start > length {
			start = length
		}
		if end > length {
			end = length
		}
		hunks = append(hunks, string(contents[start:end]))
	}
	return hunks, string(contents[end:])
}

func CompareSlicesIgnoringWhitespace(actual []string, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}
	for i := range actual {
		if strings.TrimSpace(actual[i]) != expected[i] {
			return false
		}
	}
	return true
}

func formatEntries(entries []toc.MetadataEntry, slice []string) string {
	formatted := ""
	for i, item := range slice {
		formatted += fmt.Sprintf("%v -> %q\n", entries[i], item)
	}
	return formatted
}

func formatContents(slice []string) string {
	formatted := ""
	for _, item := range slice {
		formatted += fmt.Sprintf("%q\n", item)
	}
	return formatted
}

func formatDiffs(actual []string, expected []string) string {
	// return "diffs"
	dmp := diffmatchpatch.New()
	diffs := ""
	for idx := range actual {
		diffs += dmp.DiffPrettyText(dmp.DiffMain(expected[idx], actual[idx], false))
	}
	return diffs
}

func AssertBufferContents(entries []toc.MetadataEntry, buffer *Buffer, expected ...string) {
	if len(entries) == 0 {
		Fail("TOC is empty")
	}
	// fmt.Println(fmt.Sprintf("entries, --> %v, %v", len(entries), entries))
	hunks, remaining := SliceBufferByEntries(entries, buffer)
	// fmt.Println(fmt.Sprintf("hunks, --> %v, %v", len(hunks), hunks))
	if remaining != "" {
		Fail(fmt.Sprintf("Buffer contains extra contents that are not being counted by TOC:\n%s\n\nActual TOC entries were:\n\n%s", remaining, formatEntries(entries, hunks)))
	}
	ok := CompareSlicesIgnoringWhitespace(hunks, expected)
	if !ok {
		Fail(fmt.Sprintf("Actual TOC entries:\n\n%s\n\ndid not match expected contents (ignoring whitespace):\n\n%s \n\nDiff:\n>>%s\x1b[31m<<", formatEntries(entries, hunks), formatContents(expected), formatDiffs(hunks, expected)))
	}
}

func ExpectEntry(entries []toc.MetadataEntry, index int, schema, referenceObject, name, objectType string) {
	Expect(len(entries)).To(BeNumerically(">", index))
	structmatcher.ExpectStructsToMatchExcluding(entries[index], toc.MetadataEntry{Schema: schema, Name: name, ObjectType: objectType, ReferenceObject: referenceObject, StartByte: 0, EndByte: 0}, "StartByte", "EndByte")
}

/**/
func ExpectEntryCount(entries []toc.MetadataEntry, index int) {
	Expect(len(entries)).To(BeNumerically("==", index))
}

func ExecuteSQLFile(connectionPool *dbconn.DBConn, filename string) {
	connStr := []string{
		"-U", connectionPool.User,
		"-d", connectionPool.DBName,
		"-h", connectionPool.Host,
		"-p", fmt.Sprintf("%d", connectionPool.Port),
		"-f", filename,
		"-v", "ON_ERROR_STOP=1",
		"-q",
	}
	fmt.Printf("-- ExecuteSQLFile, %v, %s --\n", connectionPool, filename)
	out, err := exec.Command("psql", connStr...).CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Execution of SQL file encountered an error: %s", out))
	}
}

func ExecuteSQLFileV2(host string, port int, user string, dbName string, scriptFile string) {
	connStr := []string{
		"-h", host,
		"-p", fmt.Sprintf("%d", port),
		"-U", user,
		"-d", dbName,
		"-f", scriptFile,
		"-v",
		//"ON_ERROR_STOP=1",
		"-q",
	}
	fmt.Printf("    -- ExecuteSQLFileV2, psql %v --\n", connStr)
	out, err := exec.Command("psql", connStr...).CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Execution of SQL file encountered an error: %s", out))
	}
}

func ExecuteSQLFileV2WithSaveResultFile(host string, port int, user string, dbName string, scriptFile string, resultFile string) {
	connStr := []string{
		"-h", host,
		"-p", fmt.Sprintf("%d", port),
		"-U", user,
		"--no-password",
		"-d", dbName,
		"-f", scriptFile,
		"--echo-all",
		//"--echo-queries",
		"--no-psqlrc",
	}
	fmt.Printf("    -- ExecuteSQLFileV2WithSaveResultFile, psql %v --\n", connStr)
	out, err := exec.Command("psql", connStr...).CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Execution of SQL file encountered an error: %s, %s", out, err))
	}

	file, err := os.Create(resultFile)
	if err != nil {
		Fail(fmt.Sprintf("Faile to create file %s, encountered an error: %s", resultFile, err))
	}
	file.Write(out)
	file.Close()
}

func GetServerVersion(host string, port int, user string) string {

	connStr := []string{
		"-h", host,
		"-p", fmt.Sprintf("%d", port),
		"-U", user,
		"--no-password",
		"-d", "postgres",
		"--no-psqlrc",
		"--tuples-only",
		"-c", "select version()",
	}
	//fmt.Printf("    -- Execute, %v, %s --\n", "psql", connStr)
	out, err := exec.Command("psql", connStr...).CombinedOutput()

	if err != nil {
		Fail(fmt.Sprintf("Execution encountered an error: %s, %s", out, err))
	}

	serverVersion := strings.TrimSpace(string(out))

	return serverVersion

	/*
		PostgreSQL 9.4.26 (Greenplum Database 6.20.0 build 9999) (HashData Warehouse 3.13.8 build 27594) on x86_64-unknown-linux-gnu, compiled by gcc (GCC) 10.2.1 20210130 (Red Hat 10.2.1-11), 64-bit compiled on Feb 10 2023 17:22:03

		PostgreSQL 14.4 (Apache Cloudberry 1.2.0 build commit:5b5ae3f8aa638786f01bbd08307b6474a1ba1997) on x86_64-pc-linux-gnu, compiled by gcc (GCC) 10.2.1 20210130 (Red Hat 10.2.1-11), 64-bit compiled on Feb 16 2023 23:44:39
	*/

	re := regexp.MustCompile(`\d+\.\d+(\.\d+)?`)
	match := re.FindString(serverVersion)
	//fmt.Println(match)

	return match
}

func GetPslVersion() string {
	connStr := []string{
		"--version",
	}
	//fmt.Printf("    -- Execute, %v, %s --\n", "psql", connStr)
	out, err := exec.Command("psql", connStr...).CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Execution encountered an error: %s, %s", out, err))
	}

	psqlVersion := strings.TrimSpace(string(out))

	/*
		psql (Apache Cloudberry) 14.4

		psql (PostgreSQL) 9.4.26
	*/
	re := regexp.MustCompile(`\d+\.\d+`)
	match := re.FindString(psqlVersion)
	//fmt.Println(match)

	return match
}

func BufferLength(buffer *Buffer) uint64 {
	return uint64(len(buffer.Contents()))
}

func OidFromCast(connectionPool *dbconn.DBConn, castSource uint32, castTarget uint32) uint32 {
	query := fmt.Sprintf("SELECT c.oid FROM pg_cast c WHERE castsource = '%d' AND casttarget = '%d'", castSource, castTarget)
	result := struct {
		Oid uint32
	}{}
	err := connectionPool.Get(&result, query)
	if err != nil {
		Fail(fmt.Sprintf("Execution of query failed: %v", err))
	}
	return result.Oid
}

func OidFromObjectName(connectionPool *dbconn.DBConn, schemaName string, objectName string, params builtin.MetadataQueryParams) uint32 {
	catalogTable := params.CatalogTable
	if params.OidTable != "" {
		catalogTable = params.OidTable
	}
	schemaStr := ""
	if schemaName != "" {
		schemaStr = fmt.Sprintf(" AND %s = (SELECT oid FROM pg_namespace WHERE nspname = '%s')", params.SchemaField, schemaName)
	}
	query := fmt.Sprintf("SELECT oid FROM %s WHERE %s ='%s'%s", catalogTable, params.NameField, objectName, schemaStr)
	result := struct {
		Oid uint32
	}{}
	err := connectionPool.Get(&result, query)
	if err != nil {
		Fail(fmt.Sprintf("Execution of query failed: %v", err))
	}
	return result.Oid
}

func UniqueIDFromObjectName(connectionPool *dbconn.DBConn, schemaName string, objectName string, params builtin.MetadataQueryParams) builtin.UniqueID {
	query := fmt.Sprintf("SELECT '%s'::regclass::oid", params.CatalogTable)
	result := struct {
		Oid uint32
	}{}
	err := connectionPool.Get(&result, query)
	if err != nil {
		Fail(fmt.Sprintf("Execution of query failed: %v", err))
	}

	return builtin.UniqueID{ClassID: result.Oid, Oid: OidFromObjectName(connectionPool, schemaName, objectName, params)}
}

func GetUserByID(connectionPool *dbconn.DBConn, oid uint32) string {
	return dbconn.MustSelectString(connectionPool, fmt.Sprintf("SELECT rolname AS string FROM pg_roles WHERE oid = %d", oid))
}

func CreateSecurityLabelIfGPDB6(connectionPool *dbconn.DBConn, objectType string, objectName string) {
	if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
		testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf("SECURITY LABEL FOR dummy ON %s %s IS 'unclassified';", objectType, objectName))
	}
}

/**/

func SkipIfNot4(connectionPool *dbconn.DBConn) {
	if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("5")) || connectionPool.Version.IsCBDBFamily() {
		Skip("Test only applicable to GPDB4")
	}
}

func SkipIfBefore5(connectionPool *dbconn.DBConn) {
	if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("5") {
		Skip("Test only applicable to GPDB5 and above")
	}
}

func SkipIfBefore6(connectionPool *dbconn.DBConn) {
	if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("6") {
		Skip("Test only applicable to GPDB6 and above")
	}
}

func SkipIfBefore7(connectionPool *dbconn.DBConn) {
	if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("7") {
		Skip("Test only applicable to GPDB7 and above")
	}
}

func InitializeTestTOC(buffer io.Writer, which string) (*toc.TOC, *utils.FileWithByteCount) {
	tocfile := &toc.TOC{}
	tocfile.InitializeMetadataEntryMap()
	backupfile := utils.NewFileWithByteCount(buffer)
	backupfile.Filename = which
	return tocfile, backupfile
}

func SetupTestTablespace(connectionPool *dbconn.DBConn, tablespaceLocation string) {
	// Create the directory using mkdir -p command
	exec.Command("bash", "-c", fmt.Sprintf("mkdir -p %s", tablespaceLocation)).CombinedOutput()

	// Get absolute path for the tablespace location
	absPath, err := filepath.Abs(tablespaceLocation)
	if err != nil {
		Fail(fmt.Sprintf("Could not get absolute path for tablespace: %v", err))
	}

	// Create tablespace in database
	tablespaceName := filepath.Base(tablespaceLocation)
	query := fmt.Sprintf("CREATE TABLESPACE %s LOCATION '%s'", tablespaceName, absPath)

	_, err = connectionPool.Exec(query)
	if err != nil {
		// Clean up the created directory if tablespace creation fails
		cleanupOut, cleanupErr := exec.Command("bash", "-c", fmt.Sprintf("rm -rf %s", tablespaceLocation)).CombinedOutput()
		if cleanupErr != nil {
			Fail(fmt.Sprintf("Could not create tablespace in database: %v. Additionally, cleanup failed: %s: %s",
				err, cleanupOut, cleanupErr.Error()))
		}
		Fail(fmt.Sprintf("Could not create tablespace in database: %v", err))
	}
}

func CleanupTestTablespace(connectionPool *dbconn.DBConn, tablespaceLocation string) {
	tablespaceName := filepath.Base(tablespaceLocation)
	testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf("DROP TABLESPACE %s", tablespaceName))

	cleanupOut, cleanupErr := exec.Command("bash", "-c", fmt.Sprintf("rm -rf %s", tablespaceLocation)).CombinedOutput()
	if cleanupErr != nil {
		Fail(fmt.Sprintf("Could not cleanup tablespace directory: %s: %s", cleanupOut, cleanupErr.Error()))
	}
}
