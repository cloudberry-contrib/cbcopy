package builtin_test

import (
	"fmt"

	// "github.com/greenplum-db/gpbackup/backup"
	"github.com/cloudberrydb/cbcopy/meta/builtin"

	// "github.com/greenplum-db/gpbackup/testutils"
	"github.com/cloudberrydb/cbcopy/testutils"

	"github.com/lib/pq"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/statistics tests", func() {
	getStatInsertReplace := func(smallint int, oid int) (string, string, string, string, string) {
		insertReplace1, insertReplace2, insertReplace3, insertReplace4, insertReplace5 := "", "", "", "", ""
		if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
			insertReplace1 = `
	false::boolean,`
			insertReplace2 = fmt.Sprintf(`
	%d::smallint,`, smallint)
			insertReplace3 = fmt.Sprintf(`
	%d::oid,`, oid)
			insertReplace4 = `
	NULL::real[],`
			insertReplace5 = `
	NULL,`
		}

		// GPDB 7+ has collations
		if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily() {
			insertReplace3 = insertReplace3 + `
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,`
		}

		return insertReplace1, insertReplace2, insertReplace3, insertReplace4, insertReplace5
	}

	Describe("PrintStatisticsStatementsForTable", func() {
		It("prints tuple stats and attr stats for all tables", func() {
			tocfile, backupfile = testutils.InitializeTestTOC(buffer, "statistics")

			testTable1 := builtin.Table{Relation: builtin.Relation{Oid: 123, Schema: "testschema", Name: "testtable1"}}
			tupleStat1 := builtin.TupleStatistic{Schema: "testschema", Table: "testtable1"}
			attStat1 := []builtin.AttributeStatistic{}

			testTable2 := builtin.Table{Relation: builtin.Relation{Oid: 456, Schema: "testschema", Name: "testtable2"}}
			tupleStat2 := builtin.TupleStatistic{Schema: "testschema", Table: "testtable2"}
			attStat2 := []builtin.AttributeStatistic{
				{Schema: "testschema", Table: "testtable2", AttName: "testattWithArray", Type: "_array"},
				{Schema: "testschema", Table: "testtable2", AttName: "testatt", Type: "_array", Relid: 2, AttNumber: 3, NullFraction: .4,
					Width: 10, Distinct: .5, Kind1: 20, Operator1: 10, Numbers1: []string{"1", "2", "3"}, Values1: []string{"4", "5", "6"}},
			}

			tables := []builtin.Table{testTable1, testTable2}
			tupleStats := map[uint32]builtin.TupleStatistic{
				123: tupleStat1,
				456: tupleStat2,
			}
			attStats := map[uint32][]builtin.AttributeStatistic{
				123: attStat1,
				456: attStat2,
			}

			builtin.PrintStatisticsStatements(backupfile, tocfile, tables, attStats, tupleStats)
			testutils.ExpectEntry(tocfile.StatisticsEntries, 0, "testschema", "", "testtable1", "STATISTICS")
			testutils.ExpectEntry(tocfile.StatisticsEntries, 1, "testschema", "", "testtable2", "STATISTICS")
			testutils.ExpectEntry(tocfile.StatisticsEntries, 2, "testschema", "", "testtable2", "STATISTICS")
			testutils.ExpectEntry(tocfile.StatisticsEntries, 3, "testschema", "", "testtable2", "STATISTICS")
			testutils.ExpectEntry(tocfile.StatisticsEntries, 4, "testschema", "", "testtable2", "STATISTICS")
			testutils.ExpectEntry(tocfile.StatisticsEntries, 5, "testschema", "", "testtable2", "STATISTICS")

			insertReplace1, insertReplace2, insertReplace3, insertReplace4, insertReplace5 := getStatInsertReplace(0, 0)

			expected := []string{
				`UPDATE pg_class
SET
	relpages = 0::int,
	reltuples = 0.000000::real
WHERE oid = 'testschema.testtable1'::regclass::oid;`,

				`UPDATE pg_class
SET
	relpages = 0::int,
	reltuples = 0.000000::real
WHERE oid = 'testschema.testtable2'::regclass::oid;`,

				`DELETE FROM pg_statistic WHERE starelid = 'testschema.testtable2'::regclass::oid AND staattnum = 0;`,

				fmt.Sprintf(`INSERT INTO pg_statistic VALUES (
	'testschema.testtable2'::regclass::oid,
	0::smallint,%[1]s
	0.000000::real,
	0::integer,
	0.000000::real,
	0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,%[2]s
	0::oid,
	0::oid,
	0::oid,
	0::oid,%[3]s
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],%[4]s
	NULL,
	NULL,
	NULL,%[5]s
	NULL);`, insertReplace1, insertReplace2, insertReplace3, insertReplace4, insertReplace5),

				`DELETE FROM pg_statistic WHERE starelid = 'testschema.testtable2'::regclass::oid AND staattnum = 3;`,

				fmt.Sprintf(`INSERT INTO pg_statistic VALUES (
	'testschema.testtable2'::regclass::oid,
	3::smallint,%[1]s
	0.400000::real,
	10::integer,
	0.500000::real,
	0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,%[2]s
	0::oid,
	0::oid,
	0::oid,
	0::oid,%[3]s
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],%[4]s
	NULL,
	NULL,
	NULL,%[5]s
	NULL);`, insertReplace1, insertReplace2, insertReplace3, insertReplace4, insertReplace5),
			}
			testutils.AssertBufferContents(tocfile.StatisticsEntries, buffer, expected...)
		})
	})
	Describe("GenerateTupleStatisticsQuery", func() {
		It("generates tuple statistics query with double quotes and a single quote in the table name and schema name", func() {
			tableTestTable := builtin.Table{Relation: builtin.Relation{Schema: `"""test'schema"""`, Name: `"""test'table"""`}}
			tupleStats := builtin.TupleStatistic{Schema: `"""test'schema"""`, Table: `"""test'table"""`}
			tupleQuery := builtin.GenerateTupleStatisticsQuery(tableTestTable, tupleStats)
			Expect(tupleQuery).To(Equal(`UPDATE pg_class
SET
	relpages = 0::int,
	reltuples = 0.000000::real
WHERE oid = '"""test''schema"""."""test''table"""'::regclass::oid;`))
		})

	})
	Describe("GenerateAttributeStatisticsQueries", func() {
		tableTestTable := builtin.Table{Relation: builtin.Relation{Schema: "testschema", Name: `"test'table"`}}

		It("generates attribute statistics query for array type", func() {
			attStats := builtin.AttributeStatistic{Schema: "testschema", Table: "testtable", AttName: "testatt", Type: "_array", Relid: 2,
				AttNumber: 3, NullFraction: .4, Width: 10, Distinct: .5, Kind1: 20, Operator1: 10,
				Numbers1: pq.StringArray([]string{"1", "2", "3"}), Values1: pq.StringArray([]string{"4", "5", "6"})}
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
				attStats.Kind5 = 10
				attStats.Operator5 = 12
			}

			attStatsQueries := builtin.GenerateAttributeStatisticsQueries(tableTestTable, attStats)
			Expect(attStatsQueries[0]).To(Equal(fmt.Sprintf(`DELETE FROM pg_statistic WHERE starelid = 'testschema."test''table"'::regclass::oid AND staattnum = 3;`)))

			insertReplace1, insertReplace2, insertReplace3, insertReplace4, insertReplace5 := getStatInsertReplace(0, 0)
			Expect(attStatsQueries[1]).To(Equal(fmt.Sprintf(`INSERT INTO pg_statistic VALUES (
	'testschema."test''table"'::regclass::oid,
	3::smallint,%s
	0.400000::real,
	10::integer,
	0.500000::real,
	0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,%s
	0::oid,
	0::oid,
	0::oid,
	0::oid,%s
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],%s
	NULL,
	NULL,
	NULL,%s
	NULL);`, insertReplace1, insertReplace2, insertReplace3, insertReplace4, insertReplace5)))
		})
		It("generates attribute statistics query for non-array type", func() {
			attStats := builtin.AttributeStatistic{Schema: "testschema", Table: "testtable", AttName: "testatt", Type: "testtype", Relid: 2,
				AttNumber: 3, NullFraction: .4, Width: 10, Distinct: .5, Kind1: 20, Operator1: 10,
				Numbers1: pq.StringArray([]string{"1", "2", "3"}), Values1: pq.StringArray([]string{"4", "5", "6"})}
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
				attStats.Kind5 = 10
				attStats.Operator5 = 12
			}

			attStatsQueries := builtin.GenerateAttributeStatisticsQueries(tableTestTable, attStats)

			Expect(attStatsQueries[0]).To(Equal(fmt.Sprintf(`DELETE FROM pg_statistic WHERE starelid = 'testschema."test''table"'::regclass::oid AND staattnum = 3;`)))

			insertReplace1, insertReplace2, insertReplace3, insertReplace4, insertReplace5 := getStatInsertReplace(10, 12)
			Expect(attStatsQueries[1]).To(Equal(fmt.Sprintf(`INSERT INTO pg_statistic VALUES (
	'testschema."test''table"'::regclass::oid,
	3::smallint,%s
	0.400000::real,
	10::integer,
	0.500000::real,
	20::smallint,
	0::smallint,
	0::smallint,
	0::smallint,%s
	10::oid,
	0::oid,
	0::oid,
	0::oid,%s
	'{"1","2","3"}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],%s
	array_in('{"4","5","6"}', 'testtype'::regtype::oid, -1),
	NULL,
	NULL,%s
	NULL);`, insertReplace1, insertReplace2, insertReplace3, insertReplace4, insertReplace5)))
		})
	})
	Describe("AnyValues", func() {
		It("returns properly casted string when length of anyvalues is greater than 0", func() {
			castedString := builtin.AnyValues([]string{"1", "2"}, "int")
			Expect(castedString).To(Equal(`array_in('{"1","2"}', 'int'::regtype::oid, -1)`))
		})
		It("returns NULL if anyvalues is of length 0", func() {
			castedString := builtin.AnyValues([]string{}, "int")
			Expect(castedString).To(Equal(`NULL`))
		})
	})
	Describe("SliceToPostgresArray", func() {
		It("returns properly quoted string representing a Postgres array", func() {
			arrayString := builtin.SliceToPostgresArray([]string{"ab'c", "ab\\c", "ab\"c", "ef\\'\"g"})
			Expect(arrayString).To(Equal("'{\"ab''c\",\"ab\\\\c\",\"ab\\\"c\",\"ef\\\\''\\\"g\"}'"))
		})
	})
})
