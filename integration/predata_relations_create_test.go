package integration

import (
	"database/sql"
	"fmt"
	"math"

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

var _ = Describe("cbcopy integration create statement tests", func() {
	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintRegularTableCreateStatement", func() {
		var (
			extTableEmpty                 builtin.ExternalTableDefinition
			testTable                     builtin.Table
			partitionPartFalseExpectation = "false"
		)
		BeforeEach(func() {
			extTableEmpty = builtin.ExternalTableDefinition{Oid: 0, Type: -2, Protocol: -2, Location: sql.NullString{String: "", Valid: true}, ExecLocation: "ALL_SEGMENTS", FormatType: "t", FormatOpts: "", Command: "", RejectLimit: 0, RejectLimitType: "", ErrTableName: "", ErrTableSchema: "", Encoding: "UTF-8", Writable: false, URIs: nil}
			testTable = builtin.Table{
				Relation:        builtin.Relation{Schema: "public", Name: "testtable"},
				TableDefinition: builtin.TableDefinition{DistPolicy: "DISTRIBUTED RANDOMLY", ExtTableDef: extTableEmpty, Inherits: []string{}},
			}
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
				partitionPartFalseExpectation = "'false'"
				testTable.ReplicaIdentity = "d"
			}
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE IF EXISTS public.testtable")
		})
		It("creates a table with no attributes", func() {
			builtin.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTable := builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")
		})
		It("creates a table of a type", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TYPE public.some_type AS (i text, j numeric)`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP TYPE public.some_type CASCADE`)

			rowOne := builtin.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			rowTwo := builtin.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: false, HasDefault: false, Type: "numeric", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			testTable.ColumnDefs = []builtin.ColumnDefinition{rowOne, rowTwo}

			testTable.TableType = "public.some_type"
			builtin.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTable := builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})[0]

			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")
		})

		It("creates a basic heap table", func() {
			rowOne := builtin.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			rowTwo := builtin.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: false, HasDefault: false, Type: "character varying(20)", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			testTable.ColumnDefs = []builtin.ColumnDefinition{rowOne, rowTwo}

			builtin.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTable := builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")
		})
		It("creates a complex heap table", func() {
			rowOneDefault := builtin.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: true, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "(42)", Comment: ""}
			rowNotNullDefault := builtin.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: true, HasDefault: true, Type: "character varying(20)", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "('bar'::text)", Comment: ""}
			rowNonDefaultStorageAndStats := builtin.ColumnDefinition{Oid: 0, Num: 3, Name: "k", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: 3, StorageType: "PLAIN", DefaultVal: "", Comment: ""}
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX')")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.some_coll CASCADE")
				rowNonDefaultStorageAndStats.Collation = "public.some_coll"
			}
			testTable.DistPolicy = "DISTRIBUTED BY (i, j)"
			testTable.ColumnDefs = []builtin.ColumnDefinition{rowOneDefault, rowNotNullDefault, rowNonDefaultStorageAndStats}

			builtin.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTable := builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")
		})
		/* comment out, due to CBDB/PG14 - GP7/PG12 behavior diff
		It("creates a basic append-optimized column-oriented table", func() {
			rowOne := builtin.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "integer", Encoding: "compresstype=zlib,blocksize=32768,compresslevel=1", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			rowTwo := builtin.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: false, HasDefault: false, Type: "character varying(20)", Encoding: "compresstype=zlib,blocksize=32768,compresslevel=1", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			testTable.StorageOpts = "appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1"
			if connectionPool.Version.AtLeast("7") {
				// Apparently, fillfactor is not backwards compatible with GPDB 7
				testTable.StorageOpts = "appendonly=true, orientation=column, compresstype=zlib, blocksize=32768, compresslevel=1"
			}
			testTable.ColumnDefs = []builtin.ColumnDefinition{rowOne, rowTwo}

			builtin.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTable := builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})[0]
			if connectionPool.Version.AtLeast("7") {
				// For GPDB 7+, the storage options no longer store the appendonly and orientation field
				testTable.TableDefinition.StorageOpts = "compresstype=zlib, blocksize=32768, compresslevel=1, checksum=true"
				testTable.TableDefinition.AccessMethodName = "ao_column"
			}
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")
		})

		*/
		It("creates a basic GPDB 7+ append-optimized table", func() {
			testutils.SkipIfBefore7(connectionPool)
			testTable.TableDefinition.StorageOpts = "blocksize=32768, compresslevel=0, compresstype=none, checksum=true"
			testTable.TableDefinition.AccessMethodName = "ao_column"

			builtin.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTable := builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")
		})
		It("creates a one-level partition table", func() {
			rowOne := builtin.ColumnDefinition{Oid: 0, Num: 1, Name: "region", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			rowTwo := builtin.ColumnDefinition{Oid: 0, Num: 2, Name: "gender", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}

			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("7") {
				testTable.PartDef = fmt.Sprintf(`PARTITION BY LIST(gender) `+`
          (
          PARTITION girls VALUES('F') WITH (tablename='public.rank_1_prt_girls', appendonly=%[1]s ), `+`
          PARTITION boys VALUES('M') WITH (tablename='public.rank_1_prt_boys', appendonly=%[1]s ), `+`
          DEFAULT PARTITION other  WITH (tablename='public.rank_1_prt_other', appendonly=%[1]s )
          )`, partitionPartFalseExpectation)
			} else {
				testTable.PartitionKeyDef = "LIST (gender)"
			}

			testTable.ColumnDefs = []builtin.ColumnDefinition{rowOne, rowTwo}
			testTable.PartitionLevelInfo.Level = "p"

			builtin.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTable.PartitionLevelInfo.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTable := builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")
		})
		It("creates a two-level partition table", func() {
			/*
			 * The spacing is very specific here and is output from the postgres function
			 * The only difference between the below statements is spacing
			 */
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("6") {
				testTable.PartDef = `PARTITION BY LIST(gender)
          SUBPARTITION BY LIST(region) ` + `
          (
          PARTITION girls VALUES('F') WITH (tablename='public.rank_1_prt_girls', appendonly=false ) ` + `
                  (
                  SUBPARTITION usa VALUES('usa') WITH (tablename='public.rank_1_prt_girls_2_prt_usa', appendonly=false ), ` + `
                  SUBPARTITION asia VALUES('asia') WITH (tablename='public.rank_1_prt_girls_2_prt_asia', appendonly=false ), ` + `
                  SUBPARTITION europe VALUES('europe') WITH (tablename='public.rank_1_prt_girls_2_prt_europe', appendonly=false ), ` + `
                  DEFAULT SUBPARTITION other_regions  WITH (tablename='public.rank_1_prt_girls_2_prt_other_regions', appendonly=false )
                  ), ` + `
          PARTITION boys VALUES('M') WITH (tablename='rank_1_prt_boys', appendonly=false ) ` + `
                  (
                  SUBPARTITION usa VALUES('usa') WITH (tablename='public.rank_1_prt_boys_2_prt_usa', appendonly=false ), ` + `
                  SUBPARTITION asia VALUES('asia') WITH (tablename='public.rank_1_prt_boys_2_prt_asia', appendonly=false ), ` + `
                  SUBPARTITION europe VALUES('europe') WITH (tablename='public.rank_1_prt_boys_2_prt_europe', appendonly=false ), ` + `
                  DEFAULT SUBPARTITION other_regions  WITH (tablename='public.rank_1_prt_boys_2_prt_other_regions', appendonly=false )
                  ), ` + `
          DEFAULT PARTITION other  WITH (tablename='public.rank_1_prt_other', appendonly=false ) ` + `
                  (
                  SUBPARTITION usa VALUES('usa') WITH (tablename='public.rank_1_prt_other_2_prt_usa', appendonly=false ), ` + `
                  SUBPARTITION asia VALUES('asia') WITH (tablename='public.rank_1_prt_other_2_prt_asia', appendonly=false ), ` + `
                  SUBPARTITION europe VALUES('europe') WITH (tablename='public.rank_1_prt_other_2_prt_europe', appendonly=false ), ` + `
                  DEFAULT SUBPARTITION other_regions  WITH (tablename='public.rank_1_prt_other_2_prt_other_regions', appendonly=false )
                  )
          )`
				testTable.PartTemplateDef = `ALTER TABLE public.testtable ` + `
SET SUBPARTITION TEMPLATE  ` + `
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='testtable'), ` + `
          SUBPARTITION asia VALUES('asia') WITH (tablename='testtable'), ` + `
          SUBPARTITION europe VALUES('europe') WITH (tablename='testtable'), ` + `
          DEFAULT SUBPARTITION other_regions  WITH (tablename='testtable')
          )
`
			} else if connectionPool.Version.IsGPDB() && connectionPool.Version.Is("6") {
				testTable.PartDef = fmt.Sprintf(`PARTITION BY LIST(gender)
          SUBPARTITION BY LIST(region) `+`
          (
          PARTITION girls VALUES('F') WITH (tablename='public.rank_1_prt_girls', appendonly=%[1]s )`+`
                  (
                  SUBPARTITION usa VALUES('usa') WITH (tablename='public.rank_1_prt_girls_2_prt_usa', appendonly=%[1]s ), `+`
                  SUBPARTITION asia VALUES('asia') WITH (tablename='public.rank_1_prt_girls_2_prt_asia', appendonly=%[1]s ), `+`
                  SUBPARTITION europe VALUES('europe') WITH (tablename='public.rank_1_prt_girls_2_prt_europe', appendonly=%[1]s ), `+`
                  DEFAULT SUBPARTITION other_regions  WITH (tablename='public.rank_1_prt_girls_2_prt_other_regions', appendonly=%[1]s )
                  ), `+`
          PARTITION boys VALUES('M') WITH (tablename='rank_1_prt_boys', appendonly=%[1]s )`+`
                  (
                  SUBPARTITION usa VALUES('usa') WITH (tablename='public.rank_1_prt_boys_2_prt_usa', appendonly=%[1]s ), `+`
                  SUBPARTITION asia VALUES('asia') WITH (tablename='public.rank_1_prt_boys_2_prt_asia', appendonly=%[1]s ), `+`
                  SUBPARTITION europe VALUES('europe') WITH (tablename='public.rank_1_prt_boys_2_prt_europe', appendonly=%[1]s ), `+`
                  DEFAULT SUBPARTITION other_regions  WITH (tablename='public.rank_1_prt_boys_2_prt_other_regions', appendonly=%[1]s )
                  ), `+`
          DEFAULT PARTITION other  WITH (tablename='public.rank_1_prt_other', appendonly=%[1]s )`+`
                  (
                  SUBPARTITION usa VALUES('usa') WITH (tablename='public.rank_1_prt_other_2_prt_usa', appendonly=%[1]s ), `+`
                  SUBPARTITION asia VALUES('asia') WITH (tablename='public.rank_1_prt_other_2_prt_asia', appendonly=%[1]s ), `+`
                  SUBPARTITION europe VALUES('europe') WITH (tablename='public.rank_1_prt_other_2_prt_europe', appendonly=%[1]s ), `+`
                  DEFAULT SUBPARTITION other_regions  WITH (tablename='public.rank_1_prt_other_2_prt_other_regions', appendonly=%[1]s )
                  )
          )`, partitionPartFalseExpectation)
				testTable.PartTemplateDef = `ALTER TABLE public.testtable ` + `
SET SUBPARTITION TEMPLATE ` + `
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='testtable'), ` + `
          SUBPARTITION asia VALUES('asia') WITH (tablename='testtable'), ` + `
          SUBPARTITION europe VALUES('europe') WITH (tablename='testtable'), ` + `
          DEFAULT SUBPARTITION other_regions  WITH (tablename='testtable')
          )
`
			} else {
				testTable.PartitionKeyDef = "LIST (gender)"
			}

			rowOne := builtin.ColumnDefinition{Oid: 0, Num: 1, Name: "region", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			rowTwo := builtin.ColumnDefinition{Oid: 0, Num: 2, Name: "gender", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			testTable.ColumnDefs = []builtin.ColumnDefinition{rowOne, rowTwo}
			testTable.PartitionLevelInfo.Level = "p"

			builtin.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTable.PartitionLevelInfo.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTable := builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")

		})
		It("creates a GPDB 7+ root table", func() {
			testutils.SkipIfBefore7(connectionPool)

			testTable.PartitionKeyDef = "RANGE (b)"
			rowOne := builtin.ColumnDefinition{Oid: 0, Num: 1, Name: "a", NotNull: false, HasDefault: false, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			rowTwo := builtin.ColumnDefinition{Oid: 0, Num: 2, Name: "b", NotNull: false, HasDefault: false, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			testTable.ColumnDefs = []builtin.ColumnDefinition{rowOne, rowTwo}
			testTable.PartitionLevelInfo.Level = "p"

			builtin.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			testTable.PartitionLevelInfo.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTable := builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")
		})
		It("creates a table with a non-default tablespace", func() {
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("6") {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLESPACE test_tablespace FILESPACE test_dir")
			} else {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir'")
			}
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLESPACE test_tablespace")
			testTable.TablespaceName = "test_tablespace"

			builtin.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTable := builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")

		})
		It("creates a table that inherits from one table", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.parent (i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.parent")
			testTable.ColumnDefs = []builtin.ColumnDefinition{}
			testTable.Inherits = []string{"public.parent"}

			builtin.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})

			Expect(testTable.Inherits).To(ConsistOf("public.parent"))
		})
		It("creates a table that inherits from two tables", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.parent_one (i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.parent_one")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.parent_two (j character varying(20))")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.parent_two")
			testTable.ColumnDefs = []builtin.ColumnDefinition{}
			testTable.Inherits = []string{"public.parent_one", "public.parent_two"}

			builtin.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})

			Expect(testTable.Inherits).To(Equal([]string{"public.parent_one", "public.parent_two"}))
		})
		It("creates an unlogged table", func() {
			testutils.SkipIfBefore6(connectionPool)
			rowOne := builtin.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			rowTwo := builtin.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: false, HasDefault: false, Type: "character varying(20)", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			testTable.ColumnDefs = []builtin.ColumnDefinition{rowOne, rowTwo}
			testTable.IsUnlogged = true

			builtin.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTable := builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")

		})
		It("creates a foreign table", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE FOREIGN DATA WRAPPER dummy;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN DATA WRAPPER dummy")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SERVER sc FOREIGN DATA WRAPPER dummy;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SERVER sc")

			testTable.TableDefinition = builtin.TableDefinition{DistPolicy: "", ExtTableDef: extTableEmpty, Inherits: []string{}}
			rowOne := builtin.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", FdwOptions: "option1 'value1', option2 'value2'"}
			testTable.ColumnDefs = []builtin.ColumnDefinition{rowOne}
			testTable.ForeignDef = builtin.ForeignTableDefinition{Oid: 0, Options: "", Server: "sc"}
			builtin.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			metadata := testutils.DefaultMetadata("TABLE", true, true, true, true)
			builtin.PrintPostCreateTableStatements(backupfile, tocfile, testTable, metadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN TABLE public.testtable")

			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			testTable.ForeignDef.Oid = testTable.Oid
			resultTable := builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")
		})
	})
	Describe("PrintPostCreateTableStatements", func() {
		var (
			extTableEmpty = builtin.ExternalTableDefinition{Oid: 0, Type: -2, Protocol: -2, Location: sql.NullString{String: "", Valid: true}, ExecLocation: "ALL_SEGMENTS", FormatType: "t", FormatOpts: "", Command: "", RejectLimit: 0, RejectLimitType: "", ErrTableName: "", ErrTableSchema: "", Encoding: "UTF-8", Writable: false, URIs: nil}
			tableRow      = builtin.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			testTable     builtin.Table
			tableMetadata builtin.ObjectMetadata
		)
		BeforeEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			tableMetadata = builtin.ObjectMetadata{Privileges: []builtin.ACL{}, ObjectType: "RELATION"}
			testTable = builtin.Table{
				Relation:        builtin.Relation{Schema: "public", Name: "testtable"},
				TableDefinition: builtin.TableDefinition{DistPolicy: "DISTRIBUTED BY (i)", ColumnDefs: []builtin.ColumnDefinition{tableRow}, ExtTableDef: extTableEmpty, Inherits: []string{}},
			}
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
				testTable.ReplicaIdentity = "d"
			}
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")
		})
		It("prints only owner for a table with no comment or column comments", func() {
			tableMetadata.Owner = "testrole"
			builtin.PrintPostCreateTableStatements(backupfile, tocfile, testTable, tableMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTableUniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			testTable.Oid = testTableUniqueID.Oid

			resultMetadata := builtin.GetMetadataForObjectType(connectionPool, builtin.TYPE_RELATION)
			resultTableMetadata := resultMetadata[testTableUniqueID]

			structmatcher.ExpectStructsToMatch(&tableMetadata, &resultTableMetadata)
			resultTable := builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(&testTable.TableDefinition, &resultTable.TableDefinition, "ColumnDefs.Oid", "ColumnDefs.ACL", "ExtTableDef")
		})
		It("prints table comment, table privileges, table owner, table security label, and column comments for a table", func() {
			tableMetadata = testutils.DefaultMetadata("TABLE", true, true, true, includeSecurityLabels)
			testTable.ColumnDefs[0].Comment = "This is a column comment."
			builtin.PrintPostCreateTableStatements(backupfile, tocfile, testTable, tableMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			testTableUniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTable := builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(&testTable.TableDefinition, &resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")

			resultMetadata := builtin.GetMetadataForObjectType(connectionPool, builtin.TYPE_RELATION)
			resultTableMetadata := resultMetadata[testTableUniqueID]
			structmatcher.ExpectStructsToMatch(&tableMetadata, &resultTableMetadata)
		})
		It("prints column level privileges", func() {
			testutils.SkipIfBefore6(connectionPool)
			privilegesColumnOne := builtin.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", StatTarget: -1, Privileges: sql.NullString{String: "testrole=r/testrole", Valid: true}}
			tableMetadata.Owner = "testrole"
			testTable.ColumnDefs = []builtin.ColumnDefinition{privilegesColumnOne}
			builtin.PrintPostCreateTableStatements(backupfile, tocfile, testTable, tableMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTable := builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})[0]
			resultColumnOne := resultTable.ColumnDefs[0]
			structmatcher.ExpectStructsToMatchExcluding(privilegesColumnOne, resultColumnOne, "Oid")
		})
		It("prints column level security label", func() {
			testutils.SkipIfBefore6(connectionPool)
			securityLabelColumnOne := builtin.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", StatTarget: -1, SecurityLabelProvider: "dummy", SecurityLabel: "unclassified"}
			testTable.ColumnDefs = []builtin.ColumnDefinition{securityLabelColumnOne}
			builtin.PrintPostCreateTableStatements(backupfile, tocfile, testTable, tableMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTable := builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})[0]
			resultColumnOne := resultTable.ColumnDefs[0]
			structmatcher.ExpectStructsToMatchExcluding(securityLabelColumnOne, resultColumnOne, "Oid")
		})
		It("prints table replica identity value", func() {
			testutils.SkipIfBefore6(connectionPool)

			testTable.ReplicaIdentity = "f"
			builtin.PrintPostCreateTableStatements(backupfile, tocfile, testTable, tableMetadata)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTable := builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})[0]
			Expect(resultTable.ReplicaIdentity).To(Equal("f"))
		})
		It("prints a GPDB 7+ ALTER statement to ATTACH a child table to it's root", func() {
			testutils.SkipIfBefore7(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testroottable(i int) PARTITION BY RANGE (i) DISTRIBUTED BY (i); ")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testroottable;")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testchildtable(i int) DISTRIBUTED BY (i);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testchildtable;")
			tableMetadata = builtin.ObjectMetadata{Privileges: []builtin.ACL{}, ObjectType: "RELATION"}
			testChildTable := builtin.Table{
				Relation: builtin.Relation{Schema: "public", Name: "testChildTable"},
				TableDefinition: builtin.TableDefinition{
					DistPolicy:  "DISTRIBUTED BY (i)",
					ColumnDefs:  []builtin.ColumnDefinition{tableRow},
					ExtTableDef: extTableEmpty,
					Inherits:    []string{"public.testroottable"},
					AttachPartitionInfo: builtin.AttachPartitionInfo{
						Relname: "public.testchildtable",
						Parent:  "public.testroottable",
						Expr:    "FOR VALUES FROM (1) TO (2)",
					},
				},
			}

			builtin.PrintPostCreateTableStatements(backupfile, tocfile, testChildTable, tableMetadata)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			attachPartitionInfoMap := builtin.GetAttachPartitionInfo(connectionPool)
			childTableOid := testutils.OidFromObjectName(connectionPool, "public", "testchildtable", builtin.TYPE_RELATION)
			testChildTable.AttachPartitionInfo.Oid = childTableOid
			structmatcher.ExpectStructsToMatch(&testChildTable.AttachPartitionInfo, attachPartitionInfoMap[childTableOid])
		})
		It("prints an ALTER statement to force row level security on the table owner", func() {
			testutils.SkipIfBefore7(connectionPool)

			testTable.ForceRowSecurity = true
			builtin.PrintPostCreateTableStatements(backupfile, tocfile, testTable, tableMetadata)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTable := builtin.ConstructDefinitionsForTables(connectionPool, []builtin.Relation{testTable.Relation})[0]
			Expect(resultTable.ForceRowSecurity).To(Equal(true))
		})
	})
	Describe("PrintCreateViewStatements", func() {
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
		It("creates a view with privileges, owner, security label, and comment", func() {
			view := builtin.View{Oid: 1, Schema: "public", Name: "simpleview", Definition: viewDef}
			viewMetadata := testutils.DefaultMetadata("VIEW", true, true, true, includeSecurityLabels)

			builtin.PrintCreateViewStatement(backupfile, tocfile, view, viewMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP VIEW public.simpleview")

			resultViews := builtin.GetAllViews(connectionPool)
			resultMetadataMap := builtin.GetMetadataForObjectType(connectionPool, builtin.TYPE_RELATION)

			view.Oid = testutils.OidFromObjectName(connectionPool, "public", "simpleview", builtin.TYPE_RELATION)
			Expect(resultViews).To(HaveLen(1))
			resultMetadata := resultMetadataMap[view.GetUniqueID()]
			structmatcher.ExpectStructsToMatch(&view, &resultViews[0])
			structmatcher.ExpectStructsToMatch(&viewMetadata, &resultMetadata)
		})
		It("creates a view with options", func() {
			testutils.SkipIfBefore6(connectionPool)
			view := builtin.View{Oid: 1, Schema: "public", Name: "simpleview", Options: " WITH (security_barrier=true)", Definition: viewDef}

			builtin.PrintCreateViewStatement(backupfile, tocfile, view, builtin.ObjectMetadata{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP VIEW public.simpleview")

			resultViews := builtin.GetAllViews(connectionPool)

			view.Oid = testutils.OidFromObjectName(connectionPool, "public", "simpleview", builtin.TYPE_RELATION)
			Expect(resultViews).To(HaveLen(1))
			structmatcher.ExpectStructsToMatch(&view, &resultViews[0])
		})
	})
	Describe("PrintMaterializedCreateViewStatements", func() {
		BeforeEach(func() {
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("6.2") {
				Skip("test only applicable to GPDB 6.2 and above")
			}
		})
		It("creates a view with privileges, owner, security label, and comment", func() {
			view := builtin.View{Oid: 1, Schema: "public", Name: "simplemview", Definition: sql.NullString{String: " SELECT 1 AS a;", Valid: true}, IsMaterialized: true, DistPolicy: "DISTRIBUTED BY (a)"}
			viewMetadata := testutils.DefaultMetadata("MATERIALIZED VIEW", true, true, true, includeSecurityLabels)

			builtin.PrintCreateViewStatement(backupfile, tocfile, view, viewMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP MATERIALIZED VIEW public.simplemview")

			resultViews := builtin.GetAllViews(connectionPool)
			resultMetadataMap := builtin.GetMetadataForObjectType(connectionPool, builtin.TYPE_RELATION)

			view.Oid = testutils.OidFromObjectName(connectionPool, "public", "simplemview", builtin.TYPE_RELATION)
			Expect(resultViews).To(HaveLen(1))
			resultMetadata := resultMetadataMap[view.GetUniqueID()]
			structmatcher.ExpectStructsToMatch(&view, &resultViews[0])
			structmatcher.ExpectStructsToMatch(&viewMetadata, &resultMetadata)
		})
		It("creates a materialized view with options", func() {
			view := builtin.View{Oid: 1, Schema: "public", Name: "simplemview", Options: " WITH (fillfactor=10)", Definition: sql.NullString{String: " SELECT 1 AS a;", Valid: true}, IsMaterialized: true, DistPolicy: "DISTRIBUTED BY (a)"}

			builtin.PrintCreateViewStatement(backupfile, tocfile, view, builtin.ObjectMetadata{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP MATERIALIZED VIEW public.simplemview")

			resultViews := builtin.GetAllViews(connectionPool)

			view.Oid = testutils.OidFromObjectName(connectionPool, "public", "simplemview", builtin.TYPE_RELATION)
			Expect(resultViews).To(HaveLen(1))
			structmatcher.ExpectStructsToMatch(&view, &resultViews[0])
		})
	})
	Describe("PrintCreateSequenceStatements", func() {
		var (
			sequenceRel         builtin.Relation
			sequence            builtin.Sequence
			sequenceMetadataMap builtin.MetadataMap
			dataType            string
		)
		BeforeEach(func() {
			sequenceRel = builtin.Relation{SchemaOid: 0, Oid: 1, Schema: "public", Name: "my_sequence"}
			sequence = builtin.Sequence{Relation: sequenceRel}
			sequenceMetadataMap = builtin.MetadataMap{}

			dataType = ""
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily() {
				dataType = "bigint"
			}
		})
		It("creates a basic sequence", func() {
			startValue := int64(0)
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
				startValue = 1
			}
			sequence.Definition = builtin.SequenceDefinition{LastVal: 1, Type: dataType, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 1, StartVal: startValue}
			builtin.PrintCreateSequenceStatements(backupfile, tocfile, []builtin.Sequence{sequence}, sequenceMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.my_sequence")

			resultSequences := builtin.GetAllSequences(connectionPool)

			Expect(resultSequences).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&sequenceRel, &resultSequences[0].Relation, "SchemaOid", "Oid")
			structmatcher.ExpectStructsToMatch(&sequence.Definition, &resultSequences[0].Definition)
		})
		It("creates a complex sequence", func() {
			startValue := int64(0)
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
				startValue = 105
			}
			sequence.Definition = builtin.SequenceDefinition{LastVal: 105, Type: dataType, Increment: 5, MaxVal: 1000, MinVal: 20, CacheVal: 1, IsCycled: false, IsCalled: true, StartVal: startValue}
			builtin.PrintCreateSequenceStatements(backupfile, tocfile, []builtin.Sequence{sequence}, sequenceMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.my_sequence")

			resultSequences := builtin.GetAllSequences(connectionPool)

			Expect(resultSequences).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&sequenceRel, &resultSequences[0].Relation, "SchemaOid", "Oid")
			structmatcher.ExpectStructsToMatch(&sequence.Definition, &resultSequences[0].Definition)
		})
		It("creates a sequence with privileges, owner, and comment", func() {
			startValue := int64(0)
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
				startValue = 1
			}
			sequence.Definition = builtin.SequenceDefinition{LastVal: 1, Type: dataType, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 1, StartVal: startValue}
			sequenceMetadata := testutils.DefaultMetadata("SEQUENCE", true, true, true, includeSecurityLabels)
			sequenceMetadataMap[builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: 1}] = sequenceMetadata
			builtin.PrintCreateSequenceStatements(backupfile, tocfile, []builtin.Sequence{sequence}, sequenceMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.my_sequence")

			resultSequences := builtin.GetAllSequences(connectionPool)

			Expect(resultSequences).To(HaveLen(1))
			resultMetadataMap := builtin.GetMetadataForObjectType(connectionPool, builtin.TYPE_RELATION)
			uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "my_sequence", builtin.TYPE_RELATION)
			resultMetadata := resultMetadataMap[uniqueID]
			structmatcher.ExpectStructsToMatchExcluding(&sequenceRel, &resultSequences[0].Relation, "SchemaOid", "Oid")
			structmatcher.ExpectStructsToMatch(&sequence.Definition, &resultSequences[0].Definition)
			structmatcher.ExpectStructsToMatch(&sequenceMetadata, &resultMetadata)
		})
		It("doesn't create identity sequences", func() {
			testutils.SkipIfBefore7(connectionPool)
			startValue := int64(0)
			sequence.Definition = builtin.SequenceDefinition{LastVal: 1, Type: dataType, MinVal: math.MinInt64, MaxVal: math.MaxInt64, Increment: 1, CacheVal: 1, StartVal: startValue}

			identitySequenceRel := builtin.Relation{SchemaOid: 0, Oid: 1, Schema: "public", Name: "my_identity_sequence"}
			identitySequence := builtin.Sequence{Relation: identitySequenceRel, IsIdentity: true}
			identitySequence.Definition = builtin.SequenceDefinition{LastVal: 1, Type: dataType, MinVal: math.MinInt64, MaxVal: math.MaxInt64, Increment: 1, CacheVal: 20, StartVal: startValue}

			builtin.PrintCreateSequenceStatements(backupfile, tocfile, []builtin.Sequence{sequence, identitySequence}, sequenceMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.my_sequence")

			resultSequences := builtin.GetAllSequences(connectionPool)

			Expect(resultSequences).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&sequenceRel, &resultSequences[0].Relation, "SchemaOid", "Oid")
			structmatcher.ExpectStructsToMatch(&sequence.Definition, &resultSequences[0].Definition)
		})
	})
	Describe("PrintAlterSequenceStatements", func() {
		It("creates a sequence owned by a table column", func() {
			startValue := int64(0)
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
				startValue = 1
			}
			sequence := builtin.Sequence{Relation: builtin.Relation{SchemaOid: 0, Oid: 1, Schema: "public", Name: "my_sequence"}}
			sequence.OwningColumn = "public.sequence_table.a"

			sequence.Definition = builtin.SequenceDefinition{LastVal: 1, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 1, StartVal: startValue}
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily() {
				sequence.Definition.Type = "bigint"
			}

			builtin.PrintCreateSequenceStatements(backupfile, tocfile, []builtin.Sequence{sequence}, builtin.MetadataMap{})
			builtin.PrintAlterSequenceStatements(backupfile, tocfile, []builtin.Sequence{sequence})

			//Create table that sequence can be owned by
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.sequence_table(a int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.sequence_table")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.my_sequence")

			sequences := builtin.GetAllSequences(connectionPool)
			Expect(sequences).To(HaveLen(1))
			Expect(sequences[0].OwningTable).To(Equal("public.sequence_table"))
			Expect(sequences[0].OwningColumn).To(Equal("public.sequence_table.a"))
		})
		It("skips identity sequences", func() {
			testutils.SkipIfBefore7(connectionPool)
			sequenceRel := builtin.Relation{SchemaOid: 0, Oid: 1, Schema: "public", Name: "my_sequence"}
			sequence := builtin.Sequence{Relation: sequenceRel}
			sequence.Definition = builtin.SequenceDefinition{LastVal: 1, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 1, StartVal: 1, Type: "bigint"}

			identitySequenceRel := builtin.Relation{SchemaOid: 0, Oid: 1, Schema: "public", Name: "my_identity_sequence"}
			identitySequence := builtin.Sequence{Relation: identitySequenceRel, IsIdentity: true}
			identitySequence.Definition = builtin.SequenceDefinition{LastVal: 1, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 20, StartVal: 1, Type: "bigint"}

			builtin.PrintCreateSequenceStatements(backupfile, tocfile, []builtin.Sequence{sequence, identitySequence}, builtin.MetadataMap{})
			builtin.PrintAlterSequenceStatements(backupfile, tocfile, []builtin.Sequence{sequence, identitySequence})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.my_sequence")

			sequences := builtin.GetAllSequences(connectionPool)
			Expect(sequences).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&sequenceRel, &sequences[0].Relation, "SchemaOid", "Oid")
			structmatcher.ExpectStructsToMatch(&sequence.Definition, &sequences[0].Definition)
		})
	})
})
