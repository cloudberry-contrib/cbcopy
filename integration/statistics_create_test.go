package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	// "github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/cloudberry-contrib/cbcopy/internal/testhelper"

	// "github.com/greenplum-db/gpbackup/backup"
	"github.com/cloudberry-contrib/cbcopy/meta/builtin"

	// "github.com/greenplum-db/gpbackup/testutils"
	"github.com/cloudberry-contrib/cbcopy/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("cbcopy integration tests", func() {
	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintStatisticsStatementsForTable", func() {
		It("prints attribute and tuple statistics for a table", func() {
			tables := []builtin.Table{
				{Relation: builtin.Relation{SchemaOid: 2200, Schema: "public", Name: "foo"}},
			}

			// Create and ANALYZE a table to generate statistics
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foo(i int, j text, k bool)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.foo")
			testhelper.AssertQueryRuns(connectionPool, "INSERT INTO public.foo VALUES (1, 'a', 't')")
			testhelper.AssertQueryRuns(connectionPool, "INSERT INTO public.foo VALUES (2, 'b', 'f')")
			testhelper.AssertQueryRuns(connectionPool, "ANALYZE public.foo")

			oldTableOid := testutils.OidFromObjectName(connectionPool, "public", "foo", builtin.TYPE_RELATION)
			tables[0].Oid = oldTableOid

			beforeAttStats := builtin.GetAttributeStatistics(connectionPool, tables)
			beforeTupleStats := builtin.GetTupleStatistics(connectionPool, tables)
			beforeTupleStat := beforeTupleStats[oldTableOid]

			// Drop and recreate the table to clear the statistics
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.foo")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foo(i int, j text, k bool)")

			// Reload the retrieved statistics into the new table
			builtin.PrintStatisticsStatements(backupfile, tocfile, tables, beforeAttStats, beforeTupleStats)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			newTableOid := testutils.OidFromObjectName(connectionPool, "public", "foo", builtin.TYPE_RELATION)
			tables[0].Oid = newTableOid
			afterAttStats := builtin.GetAttributeStatistics(connectionPool, tables)
			afterTupleStats := builtin.GetTupleStatistics(connectionPool, tables)
			afterTupleStat := afterTupleStats[newTableOid]

			oldAtts := beforeAttStats[oldTableOid]
			newAtts := afterAttStats[newTableOid]

			// Ensure the statistics match
			Expect(afterTupleStats).To(HaveLen(len(beforeTupleStats)))
			structmatcher.ExpectStructsToMatchExcluding(&beforeTupleStat, &afterTupleStat, "Oid")
			Expect(oldAtts).To(HaveLen(3))
			Expect(newAtts).To(HaveLen(3))
			for i := range oldAtts {
				structmatcher.ExpectStructsToMatchExcluding(&oldAtts[i], &newAtts[i], "Oid", "Relid")
			}
		})
		It("prints attribute and tuple statistics for a quoted table", func() {
			tables := []builtin.Table{
				{Relation: builtin.Relation{SchemaOid: 2200, Schema: "public", Name: "\"foo'\"\"''bar\""}},
			}

			// Create and ANALYZE the tables to generate statistics
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.\"foo'\"\"''bar\"(i int, j text, k bool)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.\"foo'\"\"''bar\"")
			testhelper.AssertQueryRuns(connectionPool, "INSERT INTO public.\"foo'\"\"''bar\" VALUES (1, 'a', 't')")
			testhelper.AssertQueryRuns(connectionPool, "ANALYZE public.\"foo'\"\"''bar\"")

			oldTableOid := testutils.OidFromObjectName(connectionPool, "public", "foo''\"''''bar", builtin.TYPE_RELATION)
			tables[0].Oid = oldTableOid

			beforeAttStats := builtin.GetAttributeStatistics(connectionPool, tables)
			beforeTupleStats := builtin.GetTupleStatistics(connectionPool, tables)
			beforeTupleStat := beforeTupleStats[oldTableOid]

			// Drop and recreate the table to clear the statistics
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.\"foo'\"\"''bar\"")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.\"foo'\"\"''bar\"(i int, j text, k bool)")

			// Reload the retrieved statistics into the new table
			builtin.PrintStatisticsStatements(backupfile, tocfile, tables, beforeAttStats, beforeTupleStats)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			newTableOid := testutils.OidFromObjectName(connectionPool, "public", "foo''\"''''bar", builtin.TYPE_RELATION)
			tables[0].Oid = newTableOid
			afterAttStats := builtin.GetAttributeStatistics(connectionPool, tables)
			afterTupleStats := builtin.GetTupleStatistics(connectionPool, tables)
			afterTupleStat := afterTupleStats[newTableOid]

			oldAtts := beforeAttStats[oldTableOid]
			newAtts := afterAttStats[newTableOid]

			// Ensure the statistics match
			Expect(afterTupleStats).To(HaveLen(len(beforeTupleStats)))
			structmatcher.ExpectStructsToMatchExcluding(&beforeTupleStat, &afterTupleStat, "Oid")
			Expect(oldAtts).To(HaveLen(3))
			Expect(newAtts).To(HaveLen(3))
			for i := range oldAtts {
				structmatcher.ExpectStructsToMatchExcluding(&oldAtts[i], &newAtts[i], "Oid", "Relid")
			}
		})
	})
})
