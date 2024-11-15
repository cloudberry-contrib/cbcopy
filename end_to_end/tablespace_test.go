package end_to_end_test

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/internal/testhelper"

	"github.com/cloudberrydb/cbcopy/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func checkTableInTablespace(conn *dbconn.DBConn, tableName string, tablespaceName string) bool {
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

	query := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1 
			FROM pg_class c 
			JOIN pg_namespace n ON c.relnamespace = n.oid 
			JOIN pg_tablespace t ON c.reltablespace = t.oid 
			WHERE n.nspname = '%s' 
			AND c.relname = '%s' 
			AND t.spcname = '%s'
		) AS exists`, schema, table, tablespaceName)

	exists := dbconn.MustSelectString(conn, query)
	return exists == "true"
}

func assertTableInTablespace(conn *dbconn.DBConn, tableName string, tablespaceName string) {
	if !checkTableInTablespace(conn, tableName, tablespaceName) {
		Fail(fmt.Sprintf("Table %s is not in tablespace %s when it should be",
			tableName, tablespaceName))
	}
}

func assertTablesInTablespace(conn *dbconn.DBConn, tables []string, tablespaceName string) {
	for _, tableName := range tables {
		assertTableInTablespace(conn, tableName, tablespaceName)
	}
}

var _ = Describe("Tablespace migration tests", func() {
	BeforeEach(func() {
		end_to_end_setup()
	})
	AfterEach(func() {
		end_to_end_teardown()
	})

	It("migrates tables with same tablespace name in source and target", func() {
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
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE source_schema.test_table1 (i int) TABLESPACE e2e_test_same_tablespace")
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE source_schema.test_table2 (i int) TABLESPACE e2e_test_same_tablespace")
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
		assertTablesInTablespace(destTestConn, []string{
			"target_schema.test_table1",
			"target_schema.test_table2",
		}, "e2e_test_same_tablespace")
	})

	It("migrates tables with different tablespace names", func() {
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

		// Create source schema and test tables in source database with source tablespace
		testhelper.AssertQueryRuns(srcTestConn, "CREATE SCHEMA source_schema")
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE source_schema.test_table1 (i int) TABLESPACE e2e_test_source_tablespace")
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE source_schema.test_table2 (i int) TABLESPACE e2e_test_source_tablespace")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO source_schema.test_table1 SELECT generate_series(1,100)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO source_schema.test_table2 SELECT generate_series(1,200)")

		// Create tablespace mapping file
		mappingFile := "/tmp/tablespace_mapping.txt"
		content := "e2e_test_source_tablespace,e2e_test_dest_tablespace"
		err := os.WriteFile(mappingFile, []byte(content), 0644)
		Expect(err).NotTo(HaveOccurred())
		defer os.Remove(mappingFile)

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
			"--tablespace-mapping-file", mappingFile,
			"--truncate")

		// Verify tables are restored
		assertTablesRestored(destTestConn, []string{
			"target_schema.test_table1",
			"target_schema.test_table2",
		})

		// Verify data is restored correctly
		assertDataRestored(destTestConn, map[string]int{
			"target_schema.test_table1": 100,
			"target_schema.test_table2": 200,
		})

		// Verify tables are in the destination tablespace
		assertTablesInTablespace(destTestConn, []string{
			"target_schema.test_table1",
			"target_schema.test_table2",
		}, "e2e_test_dest_tablespace")
	})

	It("migrates tables to specified destination tablespace using --dest-tablespace", func() {
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

		// Create source schema and test tables in source database with source tablespace
		testhelper.AssertQueryRuns(srcTestConn, "CREATE SCHEMA source_schema")
		// Create tables in different source tablespaces
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE source_schema.test_table1 (i int) TABLESPACE e2e_test_source_tablespace")
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE source_schema.test_table2 (i int) TABLESPACE e2e_test_same_tablespace")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO source_schema.test_table1 SELECT generate_series(1,100)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO source_schema.test_table2 SELECT generate_series(1,200)")

		// Execute schema migration from source_schema to target_schema
		// Use --dest-tablespace to move all tables to the destination tablespace
		cbcopy(cbcopyPath,
			"--source-host", sourceConn.Host,
			"--source-port", strconv.Itoa(sourceConn.Port),
			"--source-user", sourceConn.User,
			"--dest-host", destConn.Host,
			"--dest-port", strconv.Itoa(destConn.Port),
			"--dest-user", destConn.User,
			"--schema", fmt.Sprintf("%s.source_schema", "source_db"),
			"--dest-schema", fmt.Sprintf("%s.target_schema", "target_db"),
			"--dest-tablespace", "e2e_test_dest_tablespace",
			"--truncate")

		// Verify tables are restored
		assertTablesRestored(destTestConn, []string{
			"target_schema.test_table1",
			"target_schema.test_table2",
		})

		// Verify data is restored correctly
		assertDataRestored(destTestConn, map[string]int{
			"target_schema.test_table1": 100,
			"target_schema.test_table2": 200,
		})

		// Verify all tables are in the destination tablespace
		assertTablesInTablespace(destTestConn, []string{
			"target_schema.test_table1",
			"target_schema.test_table2",
		}, "e2e_test_dest_tablespace")
	})
})
