package end_to_end_test

import (
	"strconv"

	"github.com/cloudberry-contrib/cbcopy/internal/testhelper"
	"github.com/cloudberry-contrib/cbcopy/testutils"

	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("--skip-existing migration tests", func() {
	BeforeEach(func() {
		end_to_end_setup()
	})
	AfterEach(func() {
		end_to_end_teardown()
	})

	It("skips a regular table that already exists on the destination", func() {
		srcTestConn := testutils.SetupTestDbConn("source_db")
		destTestConn := testutils.SetupTestDbConn("target_db")
		defer func() {
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.se_keep")
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.se_copy")
			testhelper.AssertQueryRuns(destTestConn, "DROP TABLE IF EXISTS public.se_keep")
			testhelper.AssertQueryRuns(destTestConn, "DROP TABLE IF EXISTS public.se_copy")
			srcTestConn.Close()
			destTestConn.Close()
		}()

		// Source: two tables, each with 100 rows.
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE public.se_keep (i int)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO public.se_keep SELECT generate_series(1,100)")
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE public.se_copy (i int)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO public.se_copy SELECT generate_series(1,100)")

		// Destination: only se_keep is pre-populated, with DIFFERENT contents
		// (5 rows). After --skip-existing, those 5 rows must remain untouched
		// and se_copy must be created with the source's 100 rows.
		testhelper.AssertQueryRuns(destTestConn, "CREATE TABLE public.se_keep (i int)")
		testhelper.AssertQueryRuns(destTestConn, "INSERT INTO public.se_keep SELECT generate_series(1,5)")

		cbcopy(cbcopyPath,
			"--source-host", sourceConn.Host,
			"--source-port", strconv.Itoa(sourceConn.Port),
			"--source-user", sourceConn.User,
			"--dest-host", destConn.Host,
			"--dest-port", strconv.Itoa(destConn.Port),
			"--dest-user", destConn.User,
			"--dbname", "source_db",
			"--dest-dbname", "target_db",
			"--skip-existing")

		// se_keep should be untouched (still 5 rows), se_copy should be the
		// freshly copied 100 rows.
		assertDataRestored(destTestConn, map[string]int{
			"public.se_keep": 5,
			"public.se_copy": 100,
		})
	})

	It("skips an entire partition tree when the root exists on the destination", func() {
		srcTestConn := testutils.SetupTestDbConn("source_db")
		destTestConn := testutils.SetupTestDbConn("target_db")
		defer func() {
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.se_part CASCADE")
			testhelper.AssertQueryRuns(destTestConn, "DROP TABLE IF EXISTS public.se_part CASCADE")
			srcTestConn.Close()
			destTestConn.Close()
		}()

		// Source: range-partitioned table with two leaves, 50 rows each.
		testhelper.AssertQueryRuns(srcTestConn, `
			CREATE TABLE public.se_part (i int) PARTITION BY RANGE(i)
			(PARTITION p1 START (1) END (51), PARTITION p2 START (51) END (101))
		`)
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO public.se_part SELECT generate_series(1,100)")

		// Destination: same root structure pre-created, leaves empty.
		// --skip-existing should leave it completely alone.
		testhelper.AssertQueryRuns(destTestConn, `
			CREATE TABLE public.se_part (i int) PARTITION BY RANGE(i)
			(PARTITION p1 START (1) END (51), PARTITION p2 START (51) END (101))
		`)

		cbcopy(cbcopyPath,
			"--source-host", sourceConn.Host,
			"--source-port", strconv.Itoa(sourceConn.Port),
			"--source-user", sourceConn.User,
			"--dest-host", destConn.Host,
			"--dest-port", strconv.Itoa(destConn.Port),
			"--dest-user", destConn.User,
			"--dbname", "source_db",
			"--dest-dbname", "target_db",
			"--skip-existing")

		// Root existed on dest -> entire tree skipped -> still 0 rows.
		assertDataRestored(destTestConn, map[string]int{
			"public.se_part": 0,
		})
	})
})
