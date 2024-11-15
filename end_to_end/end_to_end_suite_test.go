package end_to_end_test

import (
	"fmt"
	"os"
	"os/exec"
	path "path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cloudberrydb/cbcopy/internal/testhelper"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
)

var (
	testFailure bool
	sourceConn  *dbconn.DBConn
	destConn    *dbconn.DBConn
	cbcopyPath  string
)

func cbcopy(cbcopyPath string, args ...string) []byte {
	args = append([]string{"--verbose"}, args...)
	command := exec.Command(cbcopyPath, args...)
	return mustRunCommand(command)
}

func assertDataRestored(conn *dbconn.DBConn, tableToTupleCount map[string]int) {
	for tableName, expectedNumTuples := range tableToTupleCount {
		actualTupleCount := dbconn.MustSelectString(conn, fmt.Sprintf("SELECT count(*) AS string FROM %s", tableName))
		if strconv.Itoa(expectedNumTuples) != actualTupleCount {
			Fail(fmt.Sprintf("Expected:\n\t%s rows to have been restored into table %s\nActual:\n\t%s rows were restored", strconv.Itoa(expectedNumTuples), tableName, actualTupleCount))
		}
	}
}

func checkTableExists(conn *dbconn.DBConn, tableName string) bool {
	var schema, table string
	s := strings.Split(tableName, ".")
	if len(s) == 2 {
		schema, table = s[0], s[1]
	} else if len(s) == 1 {
		schema = "public"
		table = s[0]
	} else {
		Fail(fmt.Sprintf("Table %s is not in a valid format", tableName))
	}
	exists := dbconn.MustSelectString(conn, fmt.Sprintf("SELECT EXISTS (SELECT * FROM pg_tables WHERE schemaname = '%s' AND tablename = '%s') AS string", schema, table))
	return (exists == "true")
}

func assertTablesRestored(conn *dbconn.DBConn, tables []string) {
	for _, tableName := range tables {
		if !checkTableExists(conn, tableName) {
			Fail(fmt.Sprintf("Table %s does not exist when it should", tableName))
		}
	}
}

func mustRunCommand(cmd *exec.Cmd) []byte {
	output, err := cmd.CombinedOutput()
	if err != nil {
		testFailure = true
		fmt.Printf("%s", output)
		Fail(fmt.Sprintf("%v", err))
	}
	return output
}

func TestEndToEnd(t *testing.T) {
	format.MaxLength = 0
	RegisterFailHandler(Fail)
	RunSpecs(t, "EndToEnd Suite")
}

var _ = BeforeSuite(func() {
	testhelper.SetupTestLogger()

	sourceConn = testutils.SetupTestDbConn("postgres")
	destConn = testutils.SetupTestDbConn("postgres")

	// default GUC setting varies between versions so set it explicitly
	testhelper.AssertQueryRuns(sourceConn, "SET gp_autostats_mode='on_no_stats'")

	// Create source and target databases
	testhelper.AssertQueryRuns(sourceConn, "DROP DATABASE IF EXISTS source_db")
	testhelper.AssertQueryRuns(sourceConn, "CREATE DATABASE source_db")
	testhelper.AssertQueryRuns(destConn, "DROP DATABASE IF EXISTS target_db")
	testhelper.AssertQueryRuns(destConn, "CREATE DATABASE target_db")

	projectRoot, err := os.Getwd()
	if err != nil {
		Fail(fmt.Sprintf("Could not get current directory: %v", err))
	}

	projectRoot = path.Dir(projectRoot)
	cbcopyPath = path.Join(projectRoot, "cbcopy")
	if _, err := os.Stat(cbcopyPath); err != nil {
		Fail(fmt.Sprintf("cbcopy binary not found at %s: %v", cbcopyPath, err))
	}
})

var _ = AfterSuite(func() {
	if testFailure {
		return
	}

	if sourceConn != nil {
		testhelper.AssertQueryRuns(sourceConn, "DROP DATABASE IF EXISTS source_db")
		sourceConn.Close()
	}
	if destConn != nil {
		testhelper.AssertQueryRuns(destConn, "DROP DATABASE IF EXISTS target_db")
		destConn.Close()
	}
})

func end_to_end_setup() {
}

func end_to_end_teardown() {
}

var _ = Describe("migration mode tests", func() {
	BeforeEach(func() {
		end_to_end_setup()
	})
	AfterEach(func() {
		end_to_end_teardown()
	})

	It("runs cbcopy with --dbname mode", func() {
		srcTestConn := testutils.SetupTestDbConn("source_db")
		destTestConn := testutils.SetupTestDbConn("target_db")

		defer func() {
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.test_table1")
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.test_table2")

			testhelper.AssertQueryRuns(destTestConn, "DROP TABLE IF EXISTS public.test_table1")
			testhelper.AssertQueryRuns(destTestConn, "DROP TABLE IF EXISTS public.test_table2")

			srcTestConn.Close()
			destTestConn.Close()
		}()

		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE public.test_table1 (i int)")
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE public.test_table2 (i int)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO public.test_table1 SELECT generate_series(1,100)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO public.test_table2 SELECT generate_series(1,200)")

		cbcopy(cbcopyPath,
			"--source-host", sourceConn.Host,
			"--source-port", strconv.Itoa(sourceConn.Port),
			"--source-user", sourceConn.User,
			"--dest-host", destConn.Host,
			"--dest-port", strconv.Itoa(destConn.Port),
			"--dest-user", destConn.User,
			"--dbname", "source_db",
			"--dest-dbname", "target_db",
			"--truncate")

		assertTablesRestored(destTestConn, []string{
			"public.test_table1",
			"public.test_table2",
		})
		assertDataRestored(destTestConn, map[string]int{
			"public.test_table1": 100,
			"public.test_table2": 200,
		})
	})

	It("runs cbcopy with --schema mode", func() {
		// Connect to source and target databases
		srcTestConn := testutils.SetupTestDbConn("source_db")
		destTestConn := testutils.SetupTestDbConn("target_db")

		// Cleanup function to drop schemas and databases
		defer func() {
			// Drop source schema in source database
			testhelper.AssertQueryRuns(srcTestConn, "DROP SCHEMA IF EXISTS source_schema CASCADE")
			// Drop target schema in target database
			testhelper.AssertQueryRuns(destTestConn, "DROP SCHEMA IF EXISTS target_schema CASCADE")

			// Close database connections
			srcTestConn.Close()
			destTestConn.Close()
		}()

		// Create source schema and test tables in source database
		testhelper.AssertQueryRuns(srcTestConn, "CREATE SCHEMA source_schema")
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE source_schema.test_table1 (i int)")
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE source_schema.test_table2 (i int)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO source_schema.test_table1 SELECT generate_series(1,100)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO source_schema.test_table2 SELECT generate_series(1,200)")

		// Execute schema migration from source_schema to target_schema
		cbcopy(cbcopyPath,
			"--source-host", sourceConn.Host,
			"--source-port", strconv.Itoa(sourceConn.Port),
			"--source-user", sourceConn.User,
			"--dest-host", destConn.Host,
			"--dest-port", strconv.Itoa(destConn.Port),
			"--dest-user", destConn.User,
			"--schema", fmt.Sprintf("%s.source_schema", "source_db"),
			"--dest-schema", fmt.Sprintf("%s.target_schema", "target_db"),
			"--truncate")

		assertTablesRestored(destTestConn, []string{
			"target_schema.test_table1",
			"target_schema.test_table2",
		})
		assertDataRestored(destTestConn, map[string]int{
			"target_schema.test_table1": 100,
			"target_schema.test_table2": 200,
		})
	})

	It("runs cbcopy with --schema-mapping-file mode", func() {
		// Connect to source and target databases
		srcTestConn := testutils.SetupTestDbConn("source_db")
		destTestConn := testutils.SetupTestDbConn("target_db")

		// Cleanup function to drop schemas and databases
		defer func() {
			// Drop source schema in source database
			testhelper.AssertQueryRuns(srcTestConn, "DROP SCHEMA IF EXISTS source_schema CASCADE")
			// Drop target schema in target database
			testhelper.AssertQueryRuns(destTestConn, "DROP SCHEMA IF EXISTS target_schema CASCADE")

			// Close database connections
			srcTestConn.Close()
			destTestConn.Close()
		}()

		// Create source schema and test tables in source database
		testhelper.AssertQueryRuns(srcTestConn, "CREATE SCHEMA source_schema")
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE source_schema.test_table1 (i int)")
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE source_schema.test_table2 (i int)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO source_schema.test_table1 SELECT generate_series(1,100)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO source_schema.test_table2 SELECT generate_series(1,200)")

		// Create schema mapping file
		mappingFile := "/tmp/schema_mapping.txt"
		content := "source_db.source_schema,target_db.target_schema"
		err := os.WriteFile(mappingFile, []byte(content), 0644)
		Expect(err).NotTo(HaveOccurred())
		defer os.Remove(mappingFile)

		// Execute schema migration using mapping file
		cbcopy(cbcopyPath,
			"--source-host", sourceConn.Host,
			"--source-port", strconv.Itoa(sourceConn.Port),
			"--source-user", sourceConn.User,
			"--dest-host", destConn.Host,
			"--dest-port", strconv.Itoa(destConn.Port),
			"--dest-user", destConn.User,
			"--schema-mapping-file", mappingFile,
			"--truncate")

		// Verify tables and data were migrated to target schema
		assertTablesRestored(destTestConn, []string{
			"target_schema.test_table1",
			"target_schema.test_table2",
		})
		assertDataRestored(destTestConn, map[string]int{
			"target_schema.test_table1": 100,
			"target_schema.test_table2": 200,
		})
	})

	It("runs cbcopy with --include-table mode", func() {
		// Set up test connections
		srcTestConn := testutils.SetupTestDbConn("source_db")
		destTestConn := testutils.SetupTestDbConn("target_db")

		// Cleanup function
		defer func() {
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.include_table1")
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.include_table2")

			testhelper.AssertQueryRuns(destTestConn, "DROP TABLE IF EXISTS public.include_table1")
			testhelper.AssertQueryRuns(destTestConn, "DROP TABLE IF EXISTS public.include_table2")

			srcTestConn.Close()
			destTestConn.Close()
		}()

		// Create test tables and insert data
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE public.include_table1 (i int)")
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE public.include_table2 (i int)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO public.include_table1 SELECT generate_series(1,100)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO public.include_table2 SELECT generate_series(1,200)")

		testhelper.AssertQueryRuns(destTestConn, "CREATE TABLE public.include_table1 (i int)")
		testhelper.AssertQueryRuns(destTestConn, "CREATE TABLE public.include_table2 (i int)")
		// Execute migration
		cbcopy(cbcopyPath,
			"--source-host", sourceConn.Host,
			"--source-port", strconv.Itoa(sourceConn.Port),
			"--source-user", sourceConn.User,
			"--dest-host", destConn.Host,
			"--dest-port", strconv.Itoa(destConn.Port),
			"--dest-user", destConn.User,
			"--include-table", fmt.Sprintf("%s.public.include_table1,%s.public.include_table2", "source_db", "source_db"),
			"--dest-table", "target_db.public.include_table1,target_db.public.include_table2",
			"--truncate")

		// Verify results
		assertTablesRestored(destTestConn, []string{
			"public.include_table1",
			"public.include_table2",
		})
		assertDataRestored(destTestConn, map[string]int{
			"public.include_table1": 100,
			"public.include_table2": 200,
		})
	})

	It("runs cbcopy with --include-table-file mode", func() {
		// Set up test connections
		srcTestConn := testutils.SetupTestDbConn("source_db")
		destTestConn := testutils.SetupTestDbConn("target_db")

		// Cleanup function
		defer func() {
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.include_table1")
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.include_table2")

			testhelper.AssertQueryRuns(destTestConn, "DROP TABLE IF EXISTS public.include_table1")
			testhelper.AssertQueryRuns(destTestConn, "DROP TABLE IF EXISTS public.include_table2")

			srcTestConn.Close()
			destTestConn.Close()
		}()

		// Create test tables and insert data
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE public.include_table1 (i int)")
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE public.include_table2 (i int)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO public.include_table1 SELECT generate_series(1,100)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO public.include_table2 SELECT generate_series(1,200)")

		// Create include table file
		tableFile := "/tmp/include_tables.txt"
		content := "source_db.public.include_table1\nsource_db.public.include_table2"
		err := os.WriteFile(tableFile, []byte(content), 0644)
		Expect(err).NotTo(HaveOccurred())
		defer os.Remove(tableFile)

		// Execute migration
		cbcopy(cbcopyPath,
			"--source-host", sourceConn.Host,
			"--source-port", strconv.Itoa(sourceConn.Port),
			"--source-user", sourceConn.User,
			"--dest-host", destConn.Host,
			"--dest-port", strconv.Itoa(destConn.Port),
			"--dest-user", destConn.User,
			"--include-table-file", tableFile,
			"--dest-dbname", "target_db",
			"--truncate")

		// Verify results
		assertTablesRestored(destTestConn, []string{
			"public.include_table1",
			"public.include_table2",
		})
		assertDataRestored(destTestConn, map[string]int{
			"public.include_table1": 100,
			"public.include_table2": 200,
		})
	})

	It("runs cbcopy with --append mode", func() {
		srcTestConn := testutils.SetupTestDbConn("source_db")
		destTestConn := testutils.SetupTestDbConn("target_db")

		defer func() {
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.test_table")
			testhelper.AssertQueryRuns(destTestConn, "DROP TABLE IF EXISTS public.test_table")
			srcTestConn.Close()
			destTestConn.Close()
		}()

		// Create and populate source table
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE public.test_table (i int)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO public.test_table SELECT generate_series(1,100)")

		// Create and populate destination table with different data
		testhelper.AssertQueryRuns(destTestConn, "CREATE TABLE public.test_table (i int)")
		testhelper.AssertQueryRuns(destTestConn, "INSERT INTO public.test_table SELECT generate_series(101,200)")

		cbcopy(cbcopyPath,
			"--source-host", sourceConn.Host,
			"--source-port", strconv.Itoa(sourceConn.Port),
			"--source-user", sourceConn.User,
			"--dest-host", destConn.Host,
			"--dest-port", strconv.Itoa(destConn.Port),
			"--dest-user", destConn.User,
			"--include-table", fmt.Sprintf("%s.public.test_table", "source_db"),
			"--dest-table", "target_db.public.test_table",
			"--append")

		assertDataRestored(destTestConn, map[string]int{
			"public.test_table": 200, // 100 original rows + 100 appended rows
		})
	})

	It("runs cbcopy with different copy strategies", func() {
		strategies := []string{"CopyOnMaster", "CopyOnSegment", "ExtDestGeCopy", "ExtDestLtCopy"}

		for _, strategy := range strategies {
			By(fmt.Sprintf("Testing with %s strategy", strategy))

			srcTestConn := testutils.SetupTestDbConn("source_db")
			destTestConn := testutils.SetupTestDbConn("target_db")

			// Create and populate source table
			testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE public.test_table (i int)")
			testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO public.test_table SELECT generate_series(1,1000)")

			testhelper.AssertQueryRuns(destTestConn, "CREATE TABLE public.test_table (i int)")

			// Set environment variable for testing specific strategy
			os.Setenv("TEST_COPY_STRATEGY", strategy)
			defer os.Unsetenv("TEST_COPY_STRATEGY")

			time.Sleep(1 * time.Second)
			cbcopy(cbcopyPath,
				"--source-host", sourceConn.Host,
				"--source-port", strconv.Itoa(sourceConn.Port),
				"--source-user", sourceConn.User,
				"--dest-host", destConn.Host,
				"--dest-port", strconv.Itoa(destConn.Port),
				"--dest-user", destConn.User,
				"--include-table", fmt.Sprintf("%s.public.test_table", "source_db"),
				"--dest-table", "target_db.public.test_table",
				"--truncate")

			assertDataRestored(destTestConn, map[string]int{
				"public.test_table": 1000,
			})

			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.test_table")
			testhelper.AssertQueryRuns(destTestConn, "DROP TABLE IF EXISTS public.test_table")
			srcTestConn.Close()
			destTestConn.Close()
		}
	})

})
