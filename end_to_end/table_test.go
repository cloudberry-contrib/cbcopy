package end_to_end_test

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cloudberry-contrib/cbcopy/internal/dbconn"
	"github.com/cloudberry-contrib/cbcopy/internal/testhelper"

	"github.com/cloudberry-contrib/cbcopy/testutils"

	. "github.com/onsi/ginkgo/v2"
)

func checkViewExists(conn *dbconn.DBConn, tableName string) bool {
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
	exists := dbconn.MustSelectString(conn, fmt.Sprintf("SELECT EXISTS (SELECT * FROM pg_views WHERE schemaname = '%s' AND viewname = '%s') AS string", schema, table))
	return (exists == "true")
}

func assertViewsRestored(conn *dbconn.DBConn, views []string) {
	for _, viewName := range views {
		if !checkViewExists(conn, viewName) {
			Fail(fmt.Sprintf("View %s does not exist when it should", viewName))
		}
	}
}

func checkTableDistributionPolicy(conn *dbconn.DBConn, tableName string) string {
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
		SELECT COALESCE(pg_catalog.pg_get_table_distributedby(c.oid), '') AS policy
		FROM pg_class c 
		JOIN pg_namespace n ON c.relnamespace = n.oid 
		WHERE n.nspname = '%s' AND c.relname = '%s'`, schema, table)

	policy := dbconn.MustSelectString(conn, query)
	return policy
}

func assertTableIsReplicated(conn *dbconn.DBConn, tableName string) {
	policy := checkTableDistributionPolicy(conn, tableName)
	if !strings.Contains(policy, "REPLICATED") {
		Fail(fmt.Sprintf("Table %s should be replicated but has policy: %s", tableName, policy))
	}
}

func assertTablesAreReplicated(conn *dbconn.DBConn, tables []string) {
	for _, tableName := range tables {
		assertTableIsReplicated(conn, tableName)
	}
}

var _ = Describe("Table migration tests", func() {
	BeforeEach(func() {
		end_to_end_setup()
	})
	AfterEach(func() {
		end_to_end_teardown()
	})

	It("migrates both tables and views using --include-table mode", func() {
		// Set up test connections
		srcTestConn := testutils.SetupTestDbConn("source_db")
		destTestConn := testutils.SetupTestDbConn("target_db")

		// Cleanup function to drop both table and view
		defer func() {
			testhelper.AssertQueryRuns(srcTestConn, "DROP VIEW IF EXISTS public.test_view2")
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.test_table1")

			testhelper.AssertQueryRuns(destTestConn, "DROP VIEW IF EXISTS public.test_view2")
			testhelper.AssertQueryRuns(destTestConn, "DROP TABLE IF EXISTS public.test_table1")

			srcTestConn.Close()
			destTestConn.Close()
		}()

		// Create a table and a dependent view in source database
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE public.test_table1 (i int)")
		testhelper.AssertQueryRuns(srcTestConn, `
			CREATE VIEW public.test_view2 AS 
			SELECT i 
			FROM public.test_table1
		`)
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO public.test_table1 SELECT generate_series(1,100)")

		// Execute migration for both table and view
		cbcopy(cbcopyPath,
			"--source-host", sourceConn.Host,
			"--source-port", strconv.Itoa(sourceConn.Port),
			"--source-user", sourceConn.User,
			"--dest-host", destConn.Host,
			"--dest-port", strconv.Itoa(destConn.Port),
			"--dest-user", destConn.User,
			"--include-table", fmt.Sprintf("%s.public.test_table1,%s.public.test_view2", "source_db", "source_db"),
			"--dest-dbname", "target_db",
			"--truncate")

		// Verify both table and view are restored
		assertTablesRestored(destTestConn, []string{
			"public.test_table1",
		})
		assertViewsRestored(destTestConn, []string{
			"public.test_view2",
		})
		// Verify only table data is restored (views don't contain data)
		assertDataRestored(destTestConn, map[string]int{
			"public.test_table1": 100,
			"public.test_view2":  100,
		})
	})

	It("migrates single view using --include-table mode", func() {
		// Set up test connections
		srcTestConn := testutils.SetupTestDbConn("source_db")
		destTestConn := testutils.SetupTestDbConn("target_db")

		// Cleanup function
		defer func() {
			testhelper.AssertQueryRuns(srcTestConn, "DROP VIEW IF EXISTS public.test_view")
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.base_table")

			testhelper.AssertQueryRuns(destTestConn, "DROP VIEW IF EXISTS public.test_view")
			testhelper.AssertQueryRuns(destTestConn, "DROP TABLE IF EXISTS public.base_table")

			srcTestConn.Close()
			destTestConn.Close()
		}()

		// Create a base table and a view in source database
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE public.base_table (i int)")
		testhelper.AssertQueryRuns(srcTestConn, `
			CREATE VIEW public.test_view AS 
			SELECT i + 1 as incremented_value
			FROM public.base_table
		`)
		testhelper.AssertQueryRuns(destTestConn, "CREATE TABLE public.base_table (i int)")

		// Execute migration for view only
		cbcopy(cbcopyPath,
			"--source-host", sourceConn.Host,
			"--source-port", strconv.Itoa(sourceConn.Port),
			"--source-user", sourceConn.User,
			"--dest-host", destConn.Host,
			"--dest-port", strconv.Itoa(destConn.Port),
			"--dest-user", destConn.User,
			"--include-table", fmt.Sprintf("%s.public.test_view", "source_db"),
			"--dest-dbname", "target_db",
			"--truncate")

		// Verify only view is restored
		assertViewsRestored(destTestConn, []string{
			"public.test_view",
		})

		// No need to verify data as view doesn't contain data
	})

	It("migrates both tables and views to different schema using --include-table and --dest-schema", func() {
		// Set up test connections
		srcTestConn := testutils.SetupTestDbConn("source_db")
		destTestConn := testutils.SetupTestDbConn("target_db")

		// Cleanup function to drop both table and view
		defer func() {
			testhelper.AssertQueryRuns(srcTestConn, "DROP VIEW IF EXISTS public.test_view2")
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.test_table1")

			testhelper.AssertQueryRuns(destTestConn, "DROP SCHEMA IF EXISTS target_schema CASCADE")

			srcTestConn.Close()
			destTestConn.Close()
		}()

		// Create a table and a dependent view in source database
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE public.test_table1 (i int)")
		testhelper.AssertQueryRuns(srcTestConn, `
			CREATE VIEW public.test_view2 AS 
			SELECT i 
			FROM public.test_table1
		`)
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO public.test_table1 SELECT generate_series(1,100)")

		// Create target schema in destination database
		testhelper.AssertQueryRuns(destTestConn, "CREATE SCHEMA target_schema")

		// Execute migration for both table and view with schema redirection
		cbcopy(cbcopyPath,
			"--source-host", sourceConn.Host,
			"--source-port", strconv.Itoa(sourceConn.Port),
			"--source-user", sourceConn.User,
			"--dest-host", destConn.Host,
			"--dest-port", strconv.Itoa(destConn.Port),
			"--dest-user", destConn.User,
			"--include-table", fmt.Sprintf("%s.public.test_table1,%s.public.test_view2", "source_db", "source_db"),
			"--dest-schema", "target_db.target_schema",
			"--truncate")

		// Verify both table and view are restored in target schema
		assertTablesRestored(destTestConn, []string{
			"target_schema.test_table1",
		})
		assertViewsRestored(destTestConn, []string{
			"target_schema.test_view2",
		})
		// Verify only table data is restored (views don't contain data)
		assertDataRestored(destTestConn, map[string]int{
			"target_schema.test_table1": 100,
			"target_schema.test_view2":  100,
		})
	})

	It("migrates replicated table correctly", func() {
		srcTestConn := testutils.SetupTestDbConn("source_db")
		testutils.SkipIfBefore6(srcTestConn)

		destTestConn := testutils.SetupTestDbConn("target_db")

		// Cleanup function
		defer func() {
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.replicated_test_table")
			testhelper.AssertQueryRuns(destTestConn, "DROP TABLE IF EXISTS public.replicated_test_table")

			srcTestConn.Close()
			destTestConn.Close()
		}()

		testhelper.AssertQueryRuns(srcTestConn, `
			CREATE TABLE public.replicated_test_table (
				id int,
				name varchar(50),
				created_date date
			) DISTRIBUTED REPLICATED`)

		testhelper.AssertQueryRuns(srcTestConn, `
			INSERT INTO public.replicated_test_table 
			SELECT i, 'name_' || i, '2024-01-01'::date + (i % 30) 
			FROM generate_series(1, 500) AS i`)

		testhelper.AssertQueryRuns(destTestConn, `
			CREATE TABLE public.replicated_test_table (
				id int,
				name varchar(50),
				created_date date
			) DISTRIBUTED REPLICATED`)

		cbcopy(cbcopyPath,
			"--source-host", sourceConn.Host,
			"--source-port", strconv.Itoa(sourceConn.Port),
			"--source-user", sourceConn.User,
			"--dest-host", destConn.Host,
			"--dest-port", strconv.Itoa(destConn.Port),
			"--dest-user", destConn.User,
			"--include-table", fmt.Sprintf("%s.public.replicated_test_table", "source_db"),
			"--dest-dbname", "target_db",
			"--truncate")

		assertTablesRestored(destTestConn, []string{
			"public.replicated_test_table",
		})

		assertDataRestored(destTestConn, map[string]int{
			"public.replicated_test_table": 500,
		})

		assertTablesAreReplicated(destTestConn, []string{
			"public.replicated_test_table",
		})

		srcCount := dbconn.MustSelectString(srcTestConn,
			"SELECT COUNT(DISTINCT name) AS string FROM public.replicated_test_table")
		destCount := dbconn.MustSelectString(destTestConn,
			"SELECT COUNT(DISTINCT name) AS string FROM public.replicated_test_table")

		if srcCount != destCount {
			Fail(fmt.Sprintf("Data integrity check failed: source has %s unique names, dest has %s", srcCount, destCount))
		}
	})

	It("migrates multiple replicated tables to same target schema", func() {
		srcTestConn := testutils.SetupTestDbConn("source_db")
		testutils.SkipIfBefore6(srcTestConn)

		destTestConn := testutils.SetupTestDbConn("target_db")

		// Cleanup function
		defer func() {
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.rep_table1, public.rep_table2")
			testhelper.AssertQueryRuns(destTestConn, "DROP SCHEMA IF EXISTS target_schema CASCADE")

			srcTestConn.Close()
			destTestConn.Close()
		}()

		testhelper.AssertQueryRuns(srcTestConn, `
			CREATE TABLE public.rep_table1 (id int, data text) DISTRIBUTED REPLICATED`)
		testhelper.AssertQueryRuns(srcTestConn, `
			CREATE TABLE public.rep_table2 (id int, value numeric) DISTRIBUTED REPLICATED`)

		testhelper.AssertQueryRuns(srcTestConn,
			"INSERT INTO public.rep_table1 SELECT i, 'data_' || i FROM generate_series(1, 100) AS i")
		testhelper.AssertQueryRuns(srcTestConn,
			"INSERT INTO public.rep_table2 SELECT i, i * 1.5 FROM generate_series(1, 200) AS i")

		testhelper.AssertQueryRuns(destTestConn, "CREATE SCHEMA target_schema")
		testhelper.AssertQueryRuns(destTestConn, `
			CREATE TABLE target_schema.rep_table1 (id int, data text) DISTRIBUTED REPLICATED`)
		testhelper.AssertQueryRuns(destTestConn, `
			CREATE TABLE target_schema.rep_table2 (id int, value numeric) DISTRIBUTED REPLICATED`)

		cbcopy(cbcopyPath,
			"--source-host", sourceConn.Host,
			"--source-port", strconv.Itoa(sourceConn.Port),
			"--source-user", sourceConn.User,
			"--dest-host", destConn.Host,
			"--dest-port", strconv.Itoa(destConn.Port),
			"--dest-user", destConn.User,
			"--include-table", fmt.Sprintf("%s.public.rep_table1,%s.public.rep_table2", "source_db", "source_db"),
			"--dest-schema", "target_db.target_schema",
			"--truncate")

		assertTablesRestored(destTestConn, []string{
			"target_schema.rep_table1",
			"target_schema.rep_table2",
		})

		assertDataRestored(destTestConn, map[string]int{
			"target_schema.rep_table1": 100,
			"target_schema.rep_table2": 200,
		})

		assertTablesAreReplicated(destTestConn, []string{
			"target_schema.rep_table1",
			"target_schema.rep_table2",
		})
	})
})
