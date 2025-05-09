package integration

import (
	"database/sql"
	"math"
	"sort"

	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"

	// "github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/cloudberrydb/cbcopy/internal/testhelper"

	// "github.com/greenplum-db/gpbackup/backup"
	"github.com/cloudberrydb/cbcopy/meta/builtin"

	// "github.com/greenplum-db/gpbackup/testutils"
	"github.com/cloudberrydb/cbcopy/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("cbcopy integration tests", func() {
	Describe("GetAllUserTables", func() {
		/*		*/
		It("returns user table information for basic heap tables", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foo(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.foo")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema CASCADE")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.testtable(t text)")

			//includeRelationsQuoted, err := option.QuoteTableNames(connectionPool, builtin.MustGetFlagStringArray(option.INCLUDE_RELATION))
			includeRelationsQuoted, err := utils.QuoteTableNames(connectionPool, make([]string, 0))

			Expect(err).NotTo(HaveOccurred())

			tables := builtin.GetIncludedUserTableRelations(connectionPool, includeRelationsQuoted)

			tableFoo := builtin.Relation{Schema: "public", Name: "foo"}

			tableTestTable := builtin.Relation{Schema: "testschema", Name: "testtable"}

			Expect(tables).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&tableFoo, &tables[0], "SchemaOid", "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&tableTestTable, &tables[1], "SchemaOid", "Oid")
		})

		Context("leaf-partition-data flag", func() {
			It("returns both parent and leaf partition tables if the leaf-partition-data flag is set and there are no include tables", func() {
				//_ = backupCmdFlags.Set(option.LEAF_PARTITION_DATA, "true")
				createStmt := `CREATE TABLE public.rank (id int, rank int, year int, gender
				char(1), count int )
				DISTRIBUTED BY (id)
				PARTITION BY LIST (gender)
				( PARTITION girls VALUES ('F'),
				  PARTITION boys VALUES ('M'),
				  DEFAULT PARTITION other );`
				testhelper.AssertQueryRuns(connectionPool, createStmt)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.rank")

				tables := builtin.GetIncludedUserTableRelations(connectionPool, []string{})

				expectedTableNames := []string{"public.rank", "public.rank_1_prt_boys", "public.rank_1_prt_girls", "public.rank_1_prt_other"}
				tableNames := make([]string, 0)
				for _, table := range tables {
					tableNames = append(tableNames, table.FQN())
				}
				sort.Strings(tableNames)

				Expect(tables).To(HaveLen(4))
				Expect(tableNames).To(Equal(expectedTableNames))
			})

		})
		/*		*/
		It("returns user table information for table in specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foo(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.foo")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.foo(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE testschema.foo")

			//_ = backupCmdFlags.Set(option.INCLUDE_SCHEMA, "testschema")
			builtin.SetSchemaFilter(option.SCHEMA, "testschema")

			tables := builtin.GetIncludedUserTableRelations(connectionPool, []string{})

			tableFoo := builtin.Relation{Schema: "testschema", Name: "foo"}

			Expect(tables).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchIncluding(&tableFoo, &tables[0], "Name", "Schema")
		})
		It("returns user table information for tables in includeTables", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foo(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.foo")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.foo(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE testschema.foo")

			//_ = backupCmdFlags.Set(option.INCLUDE_RELATION, "testschema.foo")
			builtin.SetRelationFilter(option.INCLUDE_TABLE, "testschema.foo")

			//includeRelationsQuoted, err := option.QuoteTableNames(connectionPool, builtin.MustGetFlagStringArray(option.INCLUDE_RELATION))
			includeRelationsQuoted, err := utils.QuoteTableNames(connectionPool, []string{"testschema.foo"})

			Expect(err).NotTo(HaveOccurred())
			tables := builtin.GetIncludedUserTableRelations(connectionPool, includeRelationsQuoted)

			tableFoo := builtin.Relation{Schema: "testschema", Name: "foo"}

			Expect(tables).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchIncluding(&tableFoo, &tables[0], "Name", "Schema")
		})
		It("returns user table information for tables not in excludeTables", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foo(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.foo")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.foo(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE testschema.foo")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.\"user\"(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE testschema.\"user\"")

			//_ = backupCmdFlags.Set(option.EXCLUDE_RELATION, "testschema.foo")
			//_ = backupCmdFlags.Set(option.EXCLUDE_RELATION, "testschema.user")
			builtin.SetRelationFilter(option.EXCLUDE_TABLE, "testschema.foo")
			builtin.SetRelationFilter(option.EXCLUDE_TABLE, "testschema.user")

			tables := builtin.GetIncludedUserTableRelations(connectionPool, []string{})

			tableFoo := builtin.Relation{Schema: "public", Name: "foo"}

			Expect(tables).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchIncluding(&tableFoo, &tables[0], "Name", "Schema")
		})
		It("returns user table information for tables in includeSchema but not in excludeTables", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foo(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.foo")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.foo(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE testschema.foo")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.bar(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE testschema.bar")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.\"user\"(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE testschema.\"user\"")

			//_ = backupCmdFlags.Set(option.INCLUDE_SCHEMA, "testschema")
			//_ = backupCmdFlags.Set(option.EXCLUDE_RELATION, "testschema.foo")
			//_ = backupCmdFlags.Set(option.EXCLUDE_RELATION, "testschema.user")
			builtin.SetSchemaFilter(option.SCHEMA, "testschema")
			builtin.SetRelationFilter(option.EXCLUDE_TABLE, "testschema.foo")
			builtin.SetRelationFilter(option.EXCLUDE_TABLE, "testschema.user")

			tables := builtin.GetIncludedUserTableRelations(connectionPool, []string{})

			tableFoo := builtin.Relation{Schema: "testschema", Name: "bar"}
			Expect(tables).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchIncluding(&tableFoo, &tables[0], "Name", "Schema")
		})
		It("returns user table information for tables even with an non existent excludeTable", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foo(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.foo")

			//_ = backupCmdFlags.Set(option.EXCLUDE_RELATION, "testschema.nonexistant")
			builtin.SetRelationFilter(option.EXCLUDE_TABLE, "testschema.nonexistant")

			tables := builtin.GetIncludedUserTableRelations(connectionPool, []string{})

			tableFoo := builtin.Relation{Schema: "public", Name: "foo"}

			Expect(tables).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchIncluding(&tableFoo, &tables[0], "Name", "Schema")
		})

	})
	Describe("GetAllSequenceRelations", func() {
		It("returns a slice of all sequences", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SEQUENCE public.my_sequence START 10")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.my_sequence")
			testhelper.AssertQueryRuns(connectionPool, "COMMENT ON SEQUENCE public.my_sequence IS 'this is a sequence comment'")

			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema CASCADE")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SEQUENCE testschema.my_sequence2")

			sequences := builtin.GetAllSequences(connectionPool)

			mySequence := builtin.Relation{Schema: "public", Name: "my_sequence"}
			mySequence2 := builtin.Relation{Schema: "testschema", Name: "my_sequence2"}

			Expect(sequences).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchIncluding(&mySequence, &sequences[0], "Name", "Schema")
			structmatcher.ExpectStructsToMatchIncluding(&mySequence2, &sequences[1], "Name", "Schema")
		})
		/*		*/
		It("returns a slice of all sequences in a specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SEQUENCE public.my_sequence START 10")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.my_sequence")

			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema CASCADE")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SEQUENCE testschema.my_sequence")
			mySequence := builtin.Relation{Schema: "testschema", Name: "my_sequence"}

			//_ = backupCmdFlags.Set(option.INCLUDE_SCHEMA, "testschema")
			builtin.SetSchemaFilter(option.SCHEMA, "testschema")

			sequences := builtin.GetAllSequences(connectionPool)

			Expect(sequences).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchIncluding(&mySequence, &sequences[0], "Name", "Schema")
		})
		It("does not return sequences owned by included tables", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SEQUENCE public.my_sequence START 10")

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.seq_table(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.seq_table")
			testhelper.AssertQueryRuns(connectionPool, "ALTER SEQUENCE public.my_sequence OWNED BY public.seq_table.i")

			//_ = backupCmdFlags.Set(option.INCLUDE_RELATION, "public.seq_table")
			builtin.SetRelationFilter(option.INCLUDE_TABLE, "public.seq_table")

			sequences := builtin.GetAllSequences(connectionPool)

			Expect(sequences).To(BeEmpty())
		})
		It("returns sequences owned by excluded tables if the sequence is not excluded", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SEQUENCE public.my_sequence START 10")

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.seq_table(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.seq_table")
			testhelper.AssertQueryRuns(connectionPool, "ALTER SEQUENCE public.my_sequence OWNED BY public.seq_table.i")
			mySequence := builtin.Relation{Schema: "public", Name: "my_sequence"}

			//_ = backupCmdFlags.Set(option.EXCLUDE_RELATION, "public.seq_table")
			builtin.SetRelationFilter(option.EXCLUDE_TABLE, "public.seq_table")

			sequences := builtin.GetAllSequences(connectionPool)

			Expect(sequences).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchIncluding(&mySequence, &sequences[0], "Name", "Schema")
		})
		It("does not return an excluded sequence", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SEQUENCE public.sequence1 START 10")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.sequence1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SEQUENCE public.sequence2 START 10")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.sequence2")

			sequence2 := builtin.Relation{Schema: "public", Name: "sequence2"}

			//_ = backupCmdFlags.Set(option.EXCLUDE_RELATION, "public.sequence1")
			builtin.SetRelationFilter(option.EXCLUDE_TABLE, "public.sequence1")

			sequences := builtin.GetAllSequences(connectionPool)

			Expect(sequences).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchIncluding(&sequence2, &sequences[0], "Name", "Schema")
		})
		It("returns only the included sequence", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SEQUENCE public.sequence1 START 10")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.sequence1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SEQUENCE public.sequence2 START 10")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.sequence2")

			sequence1 := builtin.Relation{Schema: "public", Name: "sequence1"}

			//_ = backupCmdFlags.Set(option.INCLUDE_RELATION, "public.sequence1")
			builtin.SetRelationFilter(option.INCLUDE_TABLE, "public.sequence1")

			sequences := builtin.GetAllSequences(connectionPool)

			Expect(sequences).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchIncluding(&sequence1, &sequences[0], "Name", "Schema")
		})

	})
	Describe("GetSequenceDefinition", func() {
		var dataType string

		BeforeEach(func() {
			dataType = ""
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily() {
				dataType = "bigint"
			}
		})

		It("returns sequence information for sequence with default values", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SEQUENCE public.my_sequence")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.my_sequence")

			resultSequenceDef := builtin.GetSequenceDefinition(connectionPool, "public.my_sequence")

			expectedSequence := builtin.SequenceDefinition{LastVal: 1, Type: dataType, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 1}
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
				expectedSequence.StartVal = 1
			}
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily() {
				// In GPDB 7+, default cache value is 20
				expectedSequence.CacheVal = 20
			}

			structmatcher.ExpectStructsToMatch(&expectedSequence, &resultSequenceDef)
		})
		It("returns sequence information for a complex sequence", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.with_sequence(a int, b char(20))")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.with_sequence")
			testhelper.AssertQueryRuns(connectionPool,
				"CREATE SEQUENCE public.my_sequence INCREMENT BY 5 MINVALUE 20 MAXVALUE 1000 START 100 OWNED BY public.with_sequence.a CACHE 1")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.my_sequence")
			testhelper.AssertQueryRuns(connectionPool, "INSERT INTO public.with_sequence VALUES (nextval('public.my_sequence'), 'acme')")
			testhelper.AssertQueryRuns(connectionPool, "INSERT INTO public.with_sequence VALUES (nextval('public.my_sequence'), 'beta')")

			resultSequenceDef := builtin.GetSequenceDefinition(connectionPool, "public.my_sequence")

			expectedSequence := builtin.SequenceDefinition{LastVal: 105, Type: dataType, Increment: 5, MaxVal: 1000, MinVal: 20, CacheVal: 1, IsCycled: false, IsCalled: true}
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
				expectedSequence.StartVal = 100
			}

			structmatcher.ExpectStructsToMatch(&expectedSequence, &resultSequenceDef)
		})
	})
	Describe("Get sequence owner information", func() {
		It("returns sequence information for sequences owned by columns", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.without_sequence(a int, b char(20));")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.without_sequence")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.with_sequence(a int, b char(20));")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.with_sequence")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SEQUENCE public.my_sequence OWNED BY public.with_sequence.a;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.my_sequence")

			sequences := builtin.GetAllSequences(connectionPool)

			Expect(sequences).To(HaveLen(1))
			Expect(sequences[0].OwningTable).To(Equal("public.with_sequence"))
			Expect(sequences[0].OwningColumn).To(Equal("public.with_sequence.a"))
		})
		/* comment out, due to CBDB/PG14 - GP7/PG12 behavior diff
		It("does not return sequence owner columns if the owning table is not backed up", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.my_table(a int, b char(20));")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.my_table")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SEQUENCE public.my_sequence OWNED BY public.my_table.a;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.my_sequence")

			//_ = backupCmdFlags.Set(option.EXCLUDE_RELATION, "public.my_table")
			builtin.SetRelationFilter(option.EXCLUDE_TABLE, "public.my_table")

			sequences := builtin.GetAllSequences(connectionPool)

			Expect(sequences).To(HaveLen(1))
			Expect(sequences[0].OwningTable).To(Equal(""))
			Expect(sequences[0].OwningColumn).To(Equal(""))
		})

		*/
		It("returns sequence owner if both table and sequence are backed up", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.my_table(a int, b char(20));")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.my_table")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SEQUENCE public.my_sequence OWNED BY public.my_table.a;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.my_sequence")

			//_ = backupCmdFlags.Set(option.INCLUDE_RELATION, "public.my_sequence")
			//_ = backupCmdFlags.Set(option.INCLUDE_RELATION, "public.my_table")
			builtin.SetRelationFilter(option.INCLUDE_TABLE, "public.my_sequence")
			builtin.SetRelationFilter(option.INCLUDE_TABLE, "public.my_table")

			sequences := builtin.GetAllSequences(connectionPool)

			Expect(sequences).To(HaveLen(1))
			Expect(sequences[0].OwningTable).To(Equal("public.my_table"))
			Expect(sequences[0].OwningColumn).To(Equal("public.my_table.a"))
		})
		It("returns sequence owner if only the table is backed up", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.my_table(a int, b char(20));")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.my_table")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SEQUENCE public.my_sequence OWNED BY public.my_table.a;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.my_sequence")

			//_ = backupCmdFlags.Set(option.INCLUDE_RELATION, "public.my_table")
			builtin.SetRelationFilter(option.INCLUDE_TABLE, "public.my_table")

			sequences := builtin.GetAllSequences(connectionPool)
			Expect(sequences).To(HaveLen(0))
		})

	})
	Describe("GetAllSequences", func() {
		// TODO: add test for IsIdentity field for GPDB7
		It("returns a slice of definitions for all sequences", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SEQUENCE public.seq_one START 3")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.seq_one")
			testhelper.AssertQueryRuns(connectionPool, "COMMENT ON SEQUENCE public.seq_one IS 'this is a sequence comment'")
			startValOne := int64(0)
			startValTwo := int64(0)
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
				startValOne = 3
				startValTwo = 7
			}

			testhelper.AssertQueryRuns(connectionPool, "CREATE SEQUENCE public.seq_two START 7")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.seq_two")

			seqOneRelation := builtin.Relation{Schema: "public", Name: "seq_one"}

			seqOneDef := builtin.SequenceDefinition{LastVal: 3, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 1, StartVal: startValOne}
			seqTwoRelation := builtin.Relation{Schema: "public", Name: "seq_two"}
			seqTwoDef := builtin.SequenceDefinition{LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 1, StartVal: startValTwo}
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily() {
				// In GPDB 7+, default cache value is 20
				seqOneDef.CacheVal = 20
				seqOneDef.Type = "bigint"
				seqTwoDef.CacheVal = 20
				seqTwoDef.Type = "bigint"
			}

			results := builtin.GetAllSequences(connectionPool)

			structmatcher.ExpectStructsToMatchExcluding(&seqOneRelation, &results[0].Relation, "SchemaOid", "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&seqOneDef, &results[0].Definition)
			structmatcher.ExpectStructsToMatchExcluding(&seqTwoRelation, &results[1].Relation, "SchemaOid", "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&seqTwoDef, &results[1].Definition)
		})
	})
	Describe("GetAllViews", func() {
		var viewDef sql.NullString
		BeforeEach(func() {
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("6") {
				viewDef = sql.NullString{String: "SELECT 1;", Valid: true}
			} else if connectionPool.Version.IsGPDB() && connectionPool.Version.Is("6") {
				viewDef = sql.NullString{String: " SELECT 1;", Valid: true}
			} else { // GPDB7+
				viewDef = sql.NullString{String: " SELECT 1 AS \"?column?\";", Valid: true}
			}
		})
		It("returns a slice for a basic view", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE VIEW public.simpleview AS SELECT 1")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP VIEW public.simpleview")

			results := builtin.GetAllViews(connectionPool)
			view := builtin.View{Oid: 1, Schema: "public", Name: "simpleview", Definition: viewDef}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&view, &results[0], "Oid")
		})
		/*		*/
		It("returns a slice for view in a specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE VIEW public.simpleview AS SELECT 1")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP VIEW public.simpleview")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE VIEW testschema.simpleview AS SELECT 1")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP VIEW testschema.simpleview")

			//_ = backupCmdFlags.Set(option.INCLUDE_SCHEMA, "testschema")
			builtin.SetSchemaFilter(option.SCHEMA, "testschema")

			results := builtin.GetAllViews(connectionPool)
			view := builtin.View{Oid: 1, Schema: "testschema", Name: "simpleview", Definition: viewDef}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&view, &results[0], "Oid")
		})

		It("PANIC on views with anyarray typecasts in its view definition", func() {
			// The view definition gets incorrectly converted and stored as
			// `SELECT '{1}'::anyarray = NULL::anyarray`. This issue is fixed
			// in later versions of GPDB.
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("5.28.6") && connectionPool.Version.Before("6")) ||
				(connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6.14.1")) {
				Skip("test only applicable to GPDB 4.3.X, GPDB 5.0.0 - 5.28.5, and GPDB 6.0.0 - 6.14.0")
			}
			testhelper.AssertQueryRuns(connectionPool, "CREATE VIEW public.opexpr_array_typecasting AS SELECT '{1}'::int[] = NULL::int[]")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP VIEW public.opexpr_array_typecasting")

			defer testhelper.ShouldPanicWithMessage(`[CRITICAL]:-Detected anyarray type cast in view definition for View 'public.opexpr_array_typecasting': Drop the view or recreate the view without explicit array type casts.`)
			_ = builtin.GetAllViews(connectionPool)
		})
		It("returns a slice for a view with options", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE VIEW public.simpleview WITH (security_barrier=true) AS SELECT 1")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP VIEW public.simpleview")

			results := builtin.GetAllViews(connectionPool)
			view := builtin.View{Oid: 1, Schema: "public", Name: "simpleview", Definition: viewDef, Options: " WITH (security_barrier=true)"}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&view, &results[0], "Oid")
		})
		It("returns a slice for materialized views", func() {
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("6.2") {
				Skip("test only applicable to GPDB 6.2 and above")
			}
			testhelper.AssertQueryRuns(connectionPool, "CREATE MATERIALIZED VIEW public.simplematerialview AS SELECT 1 AS a DISTRIBUTED BY (a)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP MATERIALIZED VIEW public.simplematerialview")

			results := builtin.GetAllViews(connectionPool)
			materialView := builtin.View{Oid: 1, Schema: "public", Name: "simplematerialview", Definition: sql.NullString{String: " SELECT 1 AS a;", Valid: true}, IsMaterialized: true, DistPolicy: "DISTRIBUTED BY (a)"}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&materialView, &results[0], "Oid")
		})
		It("returns a slice for materialized views with storage parameters", func() {
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("6.2") {
				Skip("test only applicable to GPDB 6.2 and above")
			}
			testhelper.AssertQueryRuns(connectionPool, "CREATE MATERIALIZED VIEW public.simplematerialview WITH (fillfactor=50, autovacuum_enabled=false) AS SELECT 1 AS a DISTRIBUTED BY (a)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP MATERIALIZED VIEW public.simplematerialview")

			results := builtin.GetAllViews(connectionPool)
			materialView := builtin.View{Oid: 1, Schema: "public", Name: "simplematerialview", Definition: sql.NullString{String: " SELECT 1 AS a;", Valid: true}, Options: " WITH (fillfactor=50, autovacuum_enabled=false)", IsMaterialized: true, DistPolicy: "DISTRIBUTED BY (a)"}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&materialView, &results[0], "Oid")
		})
		It("returns a slice for materialized views with tablespaces", func() {
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("6.2") {
				Skip("test only applicable to GPDB 6.2 and above")
			}
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir'")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLESPACE test_tablespace")
			testhelper.AssertQueryRuns(connectionPool, "CREATE MATERIALIZED VIEW public.simplematerialview TABLESPACE test_tablespace AS SELECT 1 AS a DISTRIBUTED BY (a)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP MATERIALIZED VIEW public.simplematerialview")

			results := builtin.GetAllViews(connectionPool)
			materialView := builtin.View{Oid: 1, Schema: "public", Name: "simplematerialview", Definition: sql.NullString{String: " SELECT 1 AS a;", Valid: true}, Tablespace: "test_tablespace", IsMaterialized: true, DistPolicy: "DISTRIBUTED BY (a)"}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&materialView, &results[0], "Oid")
		})
	})
})
