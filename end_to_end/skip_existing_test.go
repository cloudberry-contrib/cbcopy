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

	It("--schema mode honors --skip-existing (regression for issue #34)", func() {
		srcTestConn := testutils.SetupTestDbConn("source_db")
		destTestConn := testutils.SetupTestDbConn("target_db")
		defer func() {
			testhelper.AssertQueryRuns(srcTestConn, "DROP SCHEMA IF EXISTS sch_src CASCADE")
			testhelper.AssertQueryRuns(destTestConn, "DROP SCHEMA IF EXISTS sch_dst CASCADE")
			srcTestConn.Close()
			destTestConn.Close()
		}()

		// Source: source_db.sch_src has two tables, each 100 rows.
		testhelper.AssertQueryRuns(srcTestConn, "CREATE SCHEMA sch_src")
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE sch_src.se_keep (i int)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO sch_src.se_keep SELECT generate_series(1,100)")
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE sch_src.se_copy (i int)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO sch_src.se_copy SELECT generate_series(1,100)")

		// Destination: target_db.sch_dst has only se_keep pre-populated
		// with 5 distinct rows. After --skip-existing it must stay at 5
		// and sch_dst.se_copy must be created with the source's 100 rows.
		testhelper.AssertQueryRuns(destTestConn, "CREATE SCHEMA sch_dst")
		testhelper.AssertQueryRuns(destTestConn, "CREATE TABLE sch_dst.se_keep (i int)")
		testhelper.AssertQueryRuns(destTestConn, "INSERT INTO sch_dst.se_keep SELECT generate_series(1,5)")

		cbcopy(cbcopyPath,
			"--source-host", sourceConn.Host,
			"--source-port", strconv.Itoa(sourceConn.Port),
			"--source-user", sourceConn.User,
			"--dest-host", destConn.Host,
			"--dest-port", strconv.Itoa(destConn.Port),
			"--dest-user", destConn.User,
			"--schema", "source_db.sch_src",
			"--dest-schema", "target_db.sch_dst",
			"--skip-existing")

		assertDataRestored(destTestConn, map[string]int{
			"sch_dst.se_keep": 5,
			"sch_dst.se_copy": 100,
		})
	})

	It("--include-table without --dest-table honors --skip-existing (regression for issue #34)", func() {
		srcTestConn := testutils.SetupTestDbConn("source_db")
		destTestConn := testutils.SetupTestDbConn("target_db")
		defer func() {
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.it_keep")
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.it_copy")
			testhelper.AssertQueryRuns(destTestConn, "DROP TABLE IF EXISTS public.it_keep")
			testhelper.AssertQueryRuns(destTestConn, "DROP TABLE IF EXISTS public.it_copy")
			srcTestConn.Close()
			destTestConn.Close()
		}()

		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE public.it_keep (i int)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO public.it_keep SELECT generate_series(1,100)")
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE public.it_copy (i int)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO public.it_copy SELECT generate_series(1,100)")

		// Destination: only public.it_keep is pre-populated (5 rows). The
		// --include-table flow without --dest-table follows the
		// CopyModeTable code path that does *not* go through
		// processDestinationTables; this case verifies that path also
		// loads the dest inventory under --skip-existing.
		testhelper.AssertQueryRuns(destTestConn, "CREATE TABLE public.it_keep (i int)")
		testhelper.AssertQueryRuns(destTestConn, "INSERT INTO public.it_keep SELECT generate_series(1,5)")

		cbcopy(cbcopyPath,
			"--source-host", sourceConn.Host,
			"--source-port", strconv.Itoa(sourceConn.Port),
			"--source-user", sourceConn.User,
			"--dest-host", destConn.Host,
			"--dest-port", strconv.Itoa(destConn.Port),
			"--dest-user", destConn.User,
			"--include-table", "source_db.public.it_keep",
			"--include-table", "source_db.public.it_copy",
			"--dest-dbname", "target_db",
			"--skip-existing")

		assertDataRestored(destTestConn, map[string]int{
			"public.it_keep": 5,
			"public.it_copy": 100,
		})
	})

	It("--include-table + --dest-table honors --skip-existing (regression for the pre-fix working path)", func() {
		// This was the ONLY mode that worked before the PR #35 fix
		// (CopyModeTable + --dest-table, which routes through
		// processDestinationTables). The fix extracted loadDestInventory
		// out of that function and moved filterTablePairsByDestExisting
		// from MigrateMetadata into doCopy; this case ensures both
		// changes preserve the previously-working behavior.
		//
		// Note: cbcopy's ValidateDestTables requires every --dest-table
		// to already exist on the destination database (it is a strict
		// precondition of the --dest-table mode). So both renamed dest
		// FQNs are pre-populated with distinct row counts here. With
		// --skip-existing, both must remain at their pre-populated
		// values -- nothing copied over the top.
		srcTestConn := testutils.SetupTestDbConn("source_db")
		destTestConn := testutils.SetupTestDbConn("target_db")
		defer func() {
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.dt_a")
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.dt_b")
			testhelper.AssertQueryRuns(destTestConn, "DROP TABLE IF EXISTS public.dt_a_dst")
			testhelper.AssertQueryRuns(destTestConn, "DROP TABLE IF EXISTS public.dt_b_dst")
			srcTestConn.Close()
			destTestConn.Close()
		}()

		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE public.dt_a (i int)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO public.dt_a SELECT generate_series(1,100)")
		testhelper.AssertQueryRuns(srcTestConn, "CREATE TABLE public.dt_b (i int)")
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO public.dt_b SELECT generate_series(1,100)")

		// Both renamed dest FQNs pre-exist with distinct row counts so we
		// can prove they were each skipped (not just unaltered by luck).
		testhelper.AssertQueryRuns(destTestConn, "CREATE TABLE public.dt_a_dst (i int)")
		testhelper.AssertQueryRuns(destTestConn, "INSERT INTO public.dt_a_dst SELECT generate_series(1,5)")
		testhelper.AssertQueryRuns(destTestConn, "CREATE TABLE public.dt_b_dst (i int)")
		testhelper.AssertQueryRuns(destTestConn, "INSERT INTO public.dt_b_dst SELECT generate_series(1,7)")

		cbcopy(cbcopyPath,
			"--source-host", sourceConn.Host,
			"--source-port", strconv.Itoa(sourceConn.Port),
			"--source-user", sourceConn.User,
			"--dest-host", destConn.Host,
			"--dest-port", strconv.Itoa(destConn.Port),
			"--dest-user", destConn.User,
			"--include-table", "source_db.public.dt_a",
			"--include-table", "source_db.public.dt_b",
			"--dest-table", "target_db.public.dt_a_dst",
			"--dest-table", "target_db.public.dt_b_dst",
			"--skip-existing")

		assertDataRestored(destTestConn, map[string]int{
			"public.dt_a_dst": 5,
			"public.dt_b_dst": 7,
		})
	})

	It("--schema mode skips an entire partition tree (regression for issue #34)", func() {
		// Partition skip-existing under --schema mode (with src/dst schema
		// names that differ, so TranslateToDestFQN is exercised). The
		// existing partition-tree case (line 63) covers --dbname mode;
		// this one extends the same gpcopy-style "root-exists -> whole-tree
		// skipped" semantics to --schema.
		srcTestConn := testutils.SetupTestDbConn("source_db")
		destTestConn := testutils.SetupTestDbConn("target_db")
		defer func() {
			testhelper.AssertQueryRuns(srcTestConn, "DROP SCHEMA IF EXISTS sch_src CASCADE")
			testhelper.AssertQueryRuns(destTestConn, "DROP SCHEMA IF EXISTS sch_dst CASCADE")
			srcTestConn.Close()
			destTestConn.Close()
		}()

		testhelper.AssertQueryRuns(srcTestConn, "CREATE SCHEMA sch_src")
		testhelper.AssertQueryRuns(srcTestConn, `
			CREATE TABLE sch_src.part_t (i int) PARTITION BY RANGE(i)
			(PARTITION p1 START (1) END (51), PARTITION p2 START (51) END (101))
		`)
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO sch_src.part_t SELECT generate_series(1,100)")

		// Destination: identical root structure under a different schema
		// name (--dest-schema maps sch_src -> sch_dst). Leaves are empty
		// on dest. Whole tree must remain at 0 rows after --skip-existing.
		testhelper.AssertQueryRuns(destTestConn, "CREATE SCHEMA sch_dst")
		testhelper.AssertQueryRuns(destTestConn, `
			CREATE TABLE sch_dst.part_t (i int) PARTITION BY RANGE(i)
			(PARTITION p1 START (1) END (51), PARTITION p2 START (51) END (101))
		`)

		cbcopy(cbcopyPath,
			"--source-host", sourceConn.Host,
			"--source-port", strconv.Itoa(sourceConn.Port),
			"--source-user", sourceConn.User,
			"--dest-host", destConn.Host,
			"--dest-port", strconv.Itoa(destConn.Port),
			"--dest-user", destConn.User,
			"--schema", "source_db.sch_src",
			"--dest-schema", "target_db.sch_dst",
			"--skip-existing")

		assertDataRestored(destTestConn, map[string]int{
			"sch_dst.part_t": 0,
		})
	})

	It("--dbname mode skips a partition tree even when destination has fewer leaves than source (gpcopy semantics)", func() {
		// Documents the gpcopy-compatible behavior: existence is matched
		// at the partition ROOT, not per-leaf. If the root is present
		// on the destination, the entire tree is skipped -- including
		// leaves that exist only on the source. This is intentional
		// and not a bug; users wanting "fill in missing leaves" need
		// a different flag (see issue #34 follow-up if any).
		srcTestConn := testutils.SetupTestDbConn("source_db")
		destTestConn := testutils.SetupTestDbConn("target_db")
		defer func() {
			testhelper.AssertQueryRuns(srcTestConn, "DROP TABLE IF EXISTS public.uneven_part CASCADE")
			testhelper.AssertQueryRuns(destTestConn, "DROP TABLE IF EXISTS public.uneven_part CASCADE")
			srcTestConn.Close()
			destTestConn.Close()
		}()

		// Source: 4 leaves p1..p4, 50 rows each, 200 total.
		testhelper.AssertQueryRuns(srcTestConn, `
			CREATE TABLE public.uneven_part (i int) PARTITION BY RANGE(i)
			(PARTITION p1 START (1)   END (51),
			 PARTITION p2 START (51)  END (101),
			 PARTITION p3 START (101) END (151),
			 PARTITION p4 START (151) END (201))
		`)
		testhelper.AssertQueryRuns(srcTestConn, "INSERT INTO public.uneven_part SELECT generate_series(1,200)")

		// Destination: same root, only 3 leaves (no p4), pre-populated
		// with 5 distinct rows per leaf (15 total). After --skip-existing
		// the whole tree must remain untouched -- p4 is NOT created on
		// dest, p1/p2/p3 are NOT overwritten.
		testhelper.AssertQueryRuns(destTestConn, `
			CREATE TABLE public.uneven_part (i int) PARTITION BY RANGE(i)
			(PARTITION p1 START (1)   END (51),
			 PARTITION p2 START (51)  END (101),
			 PARTITION p3 START (101) END (151))
		`)
		testhelper.AssertQueryRuns(destTestConn, "INSERT INTO public.uneven_part VALUES (1),(2),(3),(4),(5),(51),(52),(53),(54),(55),(101),(102),(103),(104),(105)")

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

		// Whole tree untouched -- exactly the 15 rows we pre-loaded.
		assertDataRestored(destTestConn, map[string]int{
			"public.uneven_part": 15,
		})
	})
})
