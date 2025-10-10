package integration

import (
	"database/sql"
	"os"

	"github.com/apache/cloudberry-go-libs/structmatcher"
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
	Describe("PrintExternalTableCreateStatement", func() {
		var (
			extTable  builtin.ExternalTableDefinition
			testTable builtin.Table
		)
		BeforeEach(func() {
			extTable = builtin.ExternalTableDefinition{Oid: 0, Type: 0, Protocol: builtin.FILE, Location: sql.NullString{String: "file://tmp/ext_table_file", Valid: true},
				ExecLocation: "ALL_SEGMENTS", FormatType: "t", FormatOpts: "delimiter '	' null '\\N' escape '\\'",
				Command: "", RejectLimit: 0, RejectLimitType: "", ErrTableName: "", ErrTableSchema: "", Encoding: "UTF8",
				Writable: false, URIs: []string{"file://tmp/ext_table_file"}}
			testTable = builtin.Table{
				Relation:        builtin.Relation{Schema: "public", Name: "testtable"},
				TableDefinition: builtin.TableDefinition{IsExternal: true},
			}
			_, _ = os.Create("/tmp/ext_table_file")
		})
		AfterEach(func() {
			_ = os.Remove("/tmp/ext_table_file")
			testhelper.AssertQueryRuns(connectionPool, "DROP EXTERNAL TABLE public.testtable")
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE IF EXISTS public.err_table")
			testhelper.AssertQueryRuns(connectionPool, `DROP TABLE IF EXISTS public."err_table%percent"`)
		})
		It("creates a READABLE EXTERNAL table", func() {
			extTable.Type = builtin.READABLE
			extTable.Writable = false
			extTable.LogErrors = true
			extTable.RejectLimit = 2
			extTable.RejectLimitType = "r"
			testTable.ExtTableDef = extTable

			builtin.PrintExternalTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			oid := testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTableDefs := builtin.GetExternalTableDefinitions(connectionPool)
			resultTableDef := resultTableDefs[oid]
			resultTableDef.Type, resultTableDef.Protocol = builtin.DetermineExternalTableCharacteristics(resultTableDef)

			structmatcher.ExpectStructsToMatchExcluding(&extTable, &resultTableDef, "Oid")
		})
		It("creates a READABLE WEB EXTERNAL table with an EXECUTE statement containing special characters", func() {
			extTable.Type = builtin.READABLE_WEB
			extTable.Writable = false
			extTable.Location = sql.NullString{String: "", Valid: true}
			extTable.Protocol = builtin.HTTP
			extTable.URIs = nil
			extTable.Command = `bash % someone's \.custom_script.sh`
			testTable.ExtTableDef = extTable
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily() {
				// The query for GPDB 7+ will have a NULL value instead of ""
				extTable.Location.Valid = false
			}

			builtin.PrintExternalTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			oid := testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTableDefs := builtin.GetExternalTableDefinitions(connectionPool)
			resultTableDef := resultTableDefs[oid]
			resultTableDef.Type, resultTableDef.Protocol = builtin.DetermineExternalTableCharacteristics(resultTableDef)

			structmatcher.ExpectStructsToMatchExcluding(&extTable, &resultTableDef, "Oid")
		})
		It("creates a READABLE EXTERNAL table with CSV FORMAT options", func() {
			extTable.Type = builtin.READABLE
			extTable.Writable = false
			extTable.FormatType = "c"
			extTable.FormatOpts = `delimiter '|' null '' escape ''' quote ''' force not null i`
			testTable.ExtTableDef = extTable

			builtin.PrintExternalTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			oid := testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTableDefs := builtin.GetExternalTableDefinitions(connectionPool)
			resultTableDef := resultTableDefs[oid]
			resultTableDef.Type, resultTableDef.Protocol = builtin.DetermineExternalTableCharacteristics(resultTableDef)

			structmatcher.ExpectStructsToMatchExcluding(&extTable, &resultTableDef, "Oid")
		})
		It("creates a READABLE EXTERNAL table with CUSTOM formatter", func() {
			extTable.Type = builtin.READABLE
			extTable.Writable = false
			extTable.FormatType = "b"
			extTable.FormatOpts = "formatter 'fixedwidth_out' i '20' "
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily() {
				extTable.FormatOpts = "formatter 'fixedwidth_out'i '20'"
			}
			testTable.ExtTableDef = extTable

			builtin.PrintExternalTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			oid := testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTableDefs := builtin.GetExternalTableDefinitions(connectionPool)
			resultTableDef := resultTableDefs[oid]
			resultTableDef.Type, resultTableDef.Protocol = builtin.DetermineExternalTableCharacteristics(resultTableDef)

			structmatcher.ExpectStructsToMatchExcluding(&extTable, &resultTableDef, "Oid")
		})
		It("creates a READABLE EXTERNAL table with LOG ERRORS INTO", func() {
			testutils.SkipIfNot4(connectionPool)
			extTable.Type = builtin.READABLE
			extTable.Writable = false
			extTable.ErrTableName = `"err_table%percent"`
			extTable.ErrTableSchema = "public"
			extTable.RejectLimit = 2
			extTable.RejectLimitType = "r"
			testTable.ExtTableDef = extTable

			builtin.PrintExternalTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			oid := testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTableDefs := builtin.GetExternalTableDefinitions(connectionPool)
			resultTableDef := resultTableDefs[oid]
			resultTableDef.Type, resultTableDef.Protocol = builtin.DetermineExternalTableCharacteristics(resultTableDef)

			structmatcher.ExpectStructsToMatchExcluding(&extTable, &resultTableDef, "Oid")
		})
		It("creates a READABLE EXTERNAL table with FORMAT delimiter", func() {
			extTable.Type = builtin.READABLE
			extTable.Writable = false
			extTable.FormatOpts = "delimiter '%' null '' escape 'OFF'"
			extTable.FormatType = "t"
			testTable.ExtTableDef = extTable

			builtin.PrintExternalTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			oid := testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTableDefs := builtin.GetExternalTableDefinitions(connectionPool)
			resultTableDef := resultTableDefs[oid]
			resultTableDef.Type, resultTableDef.Protocol = builtin.DetermineExternalTableCharacteristics(resultTableDef)

			structmatcher.ExpectStructsToMatchExcluding(&extTable, &resultTableDef, "Oid")
		})
		It("creates a WRITABLE EXTERNAL table", func() {
			extTable.Type = builtin.WRITABLE
			extTable.Writable = true
			extTable.Location = sql.NullString{String: "gpfdist://outputhost:8081/data1.out", Valid: true}
			extTable.URIs = []string{"gpfdist://outputhost:8081/data1.out"}
			extTable.Protocol = builtin.GPFDIST
			testTable.ExtTableDef = extTable

			builtin.PrintExternalTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			oid := testutils.OidFromObjectName(connectionPool, "public", "testtable", builtin.TYPE_RELATION)
			resultTableDefs := builtin.GetExternalTableDefinitions(connectionPool)
			resultTableDef := resultTableDefs[oid]
			resultTableDef.Type, resultTableDef.Protocol = builtin.DetermineExternalTableCharacteristics(resultTableDef)

			structmatcher.ExpectStructsToMatchExcluding(&extTable, &resultTableDef, "Oid")
		})
	})
	Describe("PrintCreateExternalProtocolStatement", func() {
		protocolReadOnly := builtin.ExternalProtocol{Oid: 1, Name: "s3_read", Owner: "testrole", Trusted: true, ReadFunction: 2, WriteFunction: 0, Validator: 0}
		protocolWriteOnly := builtin.ExternalProtocol{Oid: 1, Name: "s3_write", Owner: "testrole", Trusted: false, ReadFunction: 0, WriteFunction: 1, Validator: 0}
		protocolReadWrite := builtin.ExternalProtocol{Oid: 1, Name: "s3_read_write", Owner: "testrole", Trusted: false, ReadFunction: 2, WriteFunction: 1, Validator: 0}
		emptyMetadata := builtin.ObjectMetadata{}
		funcInfoMap := map[uint32]builtin.FunctionInfo{
			1: {QualifiedName: "public.write_to_s3", Arguments: sql.NullString{String: "", Valid: true}, IsInternal: false},
			2: {QualifiedName: "public.read_from_s3", Arguments: sql.NullString{String: "", Valid: true}, IsInternal: false},
		}

		It("creates a trusted protocol with a read function, privileges, and an owner", func() {
			protoMetadata := builtin.ObjectMetadata{Privileges: []builtin.ACL{{Grantee: "testrole", Select: true, Insert: true}}, Owner: "testrole"}

			builtin.PrintCreateExternalProtocolStatement(backupfile, tocfile, protocolReadOnly, funcInfoMap, protoMetadata)

			testhelper.AssertQueryRuns(connectionPool, "CREATE OR REPLACE FUNCTION public.read_from_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_import' LANGUAGE C STABLE;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.read_from_s3()")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP PROTOCOL s3_read")

			resultExternalProtocols := builtin.GetExternalProtocols(connectionPool)

			Expect(resultExternalProtocols).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&protocolReadOnly, &resultExternalProtocols[0], "Oid", "ReadFunction", "FuncMap")
		})
		It("creates a protocol with a write function", func() {
			builtin.PrintCreateExternalProtocolStatement(backupfile, tocfile, protocolWriteOnly, funcInfoMap, emptyMetadata)

			testhelper.AssertQueryRuns(connectionPool, "CREATE OR REPLACE FUNCTION public.write_to_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_export' LANGUAGE C STABLE;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.write_to_s3()")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP PROTOCOL s3_write")

			resultExternalProtocols := builtin.GetExternalProtocols(connectionPool)

			Expect(resultExternalProtocols).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&protocolWriteOnly, &resultExternalProtocols[0], "Oid", "WriteFunction", "FuncMap")
		})
		It("creates a protocol with a read and write function", func() {
			builtin.PrintCreateExternalProtocolStatement(backupfile, tocfile, protocolReadWrite, funcInfoMap, emptyMetadata)

			testhelper.AssertQueryRuns(connectionPool, "CREATE OR REPLACE FUNCTION public.read_from_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_import' LANGUAGE C STABLE;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.read_from_s3()")

			testhelper.AssertQueryRuns(connectionPool, "CREATE OR REPLACE FUNCTION public.write_to_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_export' LANGUAGE C STABLE;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.write_to_s3()")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP PROTOCOL s3_read_write")

			resultExternalProtocols := builtin.GetExternalProtocols(connectionPool)

			Expect(resultExternalProtocols).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&protocolReadWrite, &resultExternalProtocols[0], "Oid", "ReadFunction", "WriteFunction", "FuncMap")
		})
	})
	Describe("PrintExchangeExternalPartitionStatements", func() {
		tables := []builtin.Table{
			{Relation: builtin.Relation{Oid: 1, Schema: "public", Name: "part_tbl_ext_part_"}},
			{Relation: builtin.Relation{Oid: 2, Schema: "public", Name: "part_tbl"}},
		}
		emptyPartInfoMap := make(map[uint32]builtin.PartitionInfo)
		BeforeEach(func() {
			// For GPDB 7+, external partitions will have their own ATTACH PARTITION DDL commands.
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily() {
				Skip("Test is not applicable to GPDB 7+")
			}
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.part_tbl")
		})
		It("writes an alter statement for a named list partition", func() {
			externalPartition := builtin.PartitionInfo{
				PartitionRuleOid:       1,
				PartitionParentRuleOid: 0,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "part_tbl",
				RelationOid:            1,
				PartitionName:          "girls",
				PartitionRank:          0,
				IsExternal:             true,
			}
			testhelper.AssertQueryRuns(connectionPool, `
CREATE TABLE public.part_tbl (id int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`)
			testhelper.AssertQueryRuns(connectionPool, `
CREATE EXTERNAL WEB TABLE public.part_tbl_ext_part_ (like public.part_tbl_1_prt_girls)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
			externalPartitions := []builtin.PartitionInfo{externalPartition}

			builtin.PrintExchangeExternalPartitionStatements(backupfile, tocfile, externalPartitions, emptyPartInfoMap, tables)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultExtPartitions, resultPartInfoMap := builtin.GetExternalPartitionInfo(connectionPool)
			Expect(resultExtPartitions).To(HaveLen(1))
			Expect(resultPartInfoMap).To(HaveLen(3))
			structmatcher.ExpectStructsToMatchExcluding(&externalPartition, &resultExtPartitions[0], "PartitionRuleOid", "RelationOid", "ParentRelationOid")
		})
		It("writes an alter statement for an unnamed range partition", func() {
			externalPartition := builtin.PartitionInfo{
				PartitionRuleOid:       1,
				PartitionParentRuleOid: 0,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "part_tbl",
				RelationOid:            1,
				PartitionName:          "",
				PartitionRank:          1,
				IsExternal:             true,
			}
			testhelper.AssertQueryRuns(connectionPool, `
CREATE TABLE public.part_tbl (a int)
DISTRIBUTED BY (a)
PARTITION BY RANGE (a)
(start(1) end(3) every(1));`)
			testhelper.AssertQueryRuns(connectionPool, `
CREATE EXTERNAL WEB TABLE public.part_tbl_ext_part_ (like public.part_tbl_1_prt_1)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
			externalPartitions := []builtin.PartitionInfo{externalPartition}

			builtin.PrintExchangeExternalPartitionStatements(backupfile, tocfile, externalPartitions, emptyPartInfoMap, tables)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultExtPartitions, resultPartInfoMap := builtin.GetExternalPartitionInfo(connectionPool)
			Expect(resultExtPartitions).To(HaveLen(1))
			Expect(resultPartInfoMap).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&externalPartition, &resultExtPartitions[0], "PartitionRuleOid", "RelationOid", "ParentRelationOid")
		})
		It("writes an alter statement for a two level partition", func() {
			testutils.SkipIfBefore5(connectionPool)
			externalPartition := builtin.PartitionInfo{
				PartitionRuleOid:       10,
				PartitionParentRuleOid: 11,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "part_tbl",
				RelationOid:            1,
				PartitionName:          "apj",
				PartitionRank:          0,
				IsExternal:             true,
			}
			externalPartitionParent := builtin.PartitionInfo{
				PartitionRuleOid:       11,
				PartitionParentRuleOid: 0,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "part_tbl",
				RelationOid:            0,
				PartitionName:          "Dec16",
				PartitionRank:          0,
				IsExternal:             false,
			}
			testhelper.AssertQueryRuns(connectionPool, `
CREATE TABLE public.part_tbl (a int,b date,c text,d int)
DISTRIBUTED BY (a)
PARTITION BY RANGE (b)
SUBPARTITION BY LIST (c)
SUBPARTITION TEMPLATE
(SUBPARTITION usa values ('usa'),
SUBPARTITION apj values ('apj'),
SUBPARTITION eur values ('eur'))
( PARTITION Sep16 START (date '2016-09-01') INCLUSIVE ,
  PARTITION Oct16 START (date '2016-10-01') INCLUSIVE ,
  PARTITION Nov16 START (date '2016-11-01') INCLUSIVE ,
  PARTITION Dec16 START (date '2016-12-01') INCLUSIVE
                  END (date '2017-01-01') EXCLUSIVE);
`)

			testhelper.AssertQueryRuns(connectionPool, `CREATE EXTERNAL TABLE public.part_tbl_ext_part_ (a int,b date,c text,d int) LOCATION ('gpfdist://127.0.0.1/apj') FORMAT 'text';`)
			partInfoMap := map[uint32]builtin.PartitionInfo{externalPartitionParent.PartitionRuleOid: externalPartitionParent}
			externalPartitions := []builtin.PartitionInfo{externalPartition}

			builtin.PrintExchangeExternalPartitionStatements(backupfile, tocfile, externalPartitions, partInfoMap, tables)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultExtPartitions, _ := builtin.GetExternalPartitionInfo(connectionPool)
			externalPartition.RelationOid = testutils.OidFromObjectName(connectionPool, "public", "part_tbl_1_prt_dec16_2_prt_apj", builtin.TYPE_RELATION)
			Expect(resultExtPartitions).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&externalPartition, &resultExtPartitions[0], "PartitionRuleOid", "PartitionParentRuleOid", "ParentRelationOid")
		})
		It("writes an alter statement for a three level partition", func() {
			testutils.SkipIfBefore5(connectionPool)
			externalPartition := builtin.PartitionInfo{
				PartitionRuleOid:       10,
				PartitionParentRuleOid: 11,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "part_tbl",
				RelationOid:            1,
				PartitionName:          "europe",
				PartitionRank:          0,
				IsExternal:             true,
			}
			externalPartitionParent1 := builtin.PartitionInfo{
				PartitionRuleOid:       11,
				PartitionParentRuleOid: 12,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "part_tbl",
				RelationOid:            0,
				PartitionName:          "",
				PartitionRank:          1,
				IsExternal:             false,
			}
			externalPartitionParent2 := builtin.PartitionInfo{
				PartitionRuleOid:       12,
				PartitionParentRuleOid: 0,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "part_tbl",
				RelationOid:            0,
				PartitionName:          "",
				PartitionRank:          3,
				IsExternal:             false,
			}
			testhelper.AssertQueryRuns(connectionPool, `
CREATE TABLE public.part_tbl (id int, year int, month int, day int, region text)
DISTRIBUTED BY (id)
PARTITION BY RANGE (year)
    SUBPARTITION BY RANGE (month)
       SUBPARTITION TEMPLATE (
        START (1) END (4) EVERY (1) )
           SUBPARTITION BY LIST (region)
             SUBPARTITION TEMPLATE (
               SUBPARTITION usa VALUES ('usa'),
               SUBPARTITION europe VALUES ('europe'),
               SUBPARTITION asia VALUES ('asia')
		)
( START (2002) END (2005) EVERY (1));
`)

			testhelper.AssertQueryRuns(connectionPool, `CREATE EXTERNAL TABLE public.part_tbl_ext_part_ (like public.part_tbl_1_prt_3_2_prt_1_3_prt_europe) LOCATION ('gpfdist://127.0.0.1/apj') FORMAT 'text';`)
			partInfoMap := map[uint32]builtin.PartitionInfo{externalPartitionParent1.PartitionRuleOid: externalPartitionParent1, externalPartitionParent2.PartitionRuleOid: externalPartitionParent2}
			externalPartitions := []builtin.PartitionInfo{externalPartition}

			builtin.PrintExchangeExternalPartitionStatements(backupfile, tocfile, externalPartitions, partInfoMap, tables)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultExtPartitions, _ := builtin.GetExternalPartitionInfo(connectionPool)
			externalPartition.RelationOid = testutils.OidFromObjectName(connectionPool, "public", "part_tbl_1_prt_3_2_prt_1_3_prt_europe", builtin.TYPE_RELATION)
			Expect(resultExtPartitions).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&externalPartition, &resultExtPartitions[0], "PartitionRuleOid", "PartitionParentRuleOid", "ParentRelationOid")
		})
	})
})
