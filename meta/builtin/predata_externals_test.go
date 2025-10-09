package builtin_test

import (
	"database/sql"

	// "github.com/greenplum-db/gpbackup/backup"
	"github.com/cloudberry-contrib/cbcopy/meta/builtin"

	// "github.com/greenplum-db/gpbackup/testutils"
	"github.com/cloudberry-contrib/cbcopy/testutils"

	//"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/cloudberry-contrib/cbcopy/internal/testhelper"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/predata_externals tests", func() {
	extTableEmpty := builtin.ExternalTableDefinition{Oid: 0, Type: -2, Protocol: -2, ExecLocation: "ALL_SEGMENTS", FormatType: "t", RejectLimit: 0, Encoding: "UTF-8", Writable: false, URIs: nil}

	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("DetermineExternalTableCharacteristics", func() {
		var extTableDef builtin.ExternalTableDefinition
		BeforeEach(func() {
			extTableDef = extTableEmpty
		})
		Context("Type classification", func() {
			It("classifies a READABLE EXTERNAL table correctly", func() {
				extTableDef.Location = sql.NullString{String: "file://host:port/path/file", Valid: true}
				typ, proto := builtin.DetermineExternalTableCharacteristics(extTableDef)
				Expect(typ).To(Equal(builtin.READABLE))
				Expect(proto).To(Equal(builtin.FILE))
			})
			It("classifies a WRITABLE EXTERNAL table correctly", func() {
				extTableDef.Location = sql.NullString{String: "file://host:port/path/file", Valid: true}
				extTableDef.Writable = true
				typ, proto := builtin.DetermineExternalTableCharacteristics(extTableDef)
				Expect(typ).To(Equal(builtin.WRITABLE))
				Expect(proto).To(Equal(builtin.FILE))
			})
			It("classifies a READABLE EXTERNAL WEB table with a LOCATION correctly", func() {
				extTableDef.Location = sql.NullString{String: "http://webhost:port/path/file", Valid: true}
				typ, proto := builtin.DetermineExternalTableCharacteristics(extTableDef)
				Expect(typ).To(Equal(builtin.READABLE_WEB))
				Expect(proto).To(Equal(builtin.HTTP))
			})
			It("classifies a WRITABLE EXTERNAL WEB table with a LOCATION correctly", func() {
				extTableDef.Location = sql.NullString{String: "http://webhost:port/path/file", Valid: true}
				extTableDef.Writable = true
				typ, proto := builtin.DetermineExternalTableCharacteristics(extTableDef)
				Expect(typ).To(Equal(builtin.WRITABLE_WEB))
				Expect(proto).To(Equal(builtin.HTTP))
			})
			It("classifies a READABLE EXTERNAL WEB table with an EXECUTE correctly", func() {
				extTableDef.Command = "hostname"
				typ, proto := builtin.DetermineExternalTableCharacteristics(extTableDef)
				Expect(typ).To(Equal(builtin.READABLE_WEB))
				Expect(proto).To(Equal(builtin.HTTP))
			})
			It("classifies a WRITABLE EXTERNAL WEB table correctly", func() {
				extTableDef.Command = "hostname"
				extTableDef.Writable = true
				typ, proto := builtin.DetermineExternalTableCharacteristics(extTableDef)
				Expect(typ).To(Equal(builtin.WRITABLE_WEB))
				Expect(proto).To(Equal(builtin.HTTP))
			})
		})
		DescribeTable("Protocol classification", func(location string, expectedType int, expectedProto int) {
			extTableDef := extTableEmpty
			extTableDef.Location = sql.NullString{String: location, Valid: true}
			typ, proto := builtin.DetermineExternalTableCharacteristics(extTableDef)
			Expect(typ).To(Equal(expectedType))
			Expect(proto).To(Equal(expectedProto))
		},
			Entry("classifies file:// locations correctly", "file://host:port/path/file", builtin.READABLE, builtin.FILE),
			Entry("classifies gpfdist:// locations correctly", "gpfdist://host:port/file_pattern", builtin.READABLE, builtin.GPFDIST),
			Entry("classifies gpfdists:// locations correctly", "gpfdists://host:port/file_pattern", builtin.READABLE, builtin.GPFDIST),
			Entry("classifies gphdfs:// locations correctly", "gphdfs://host:port/path/file", builtin.READABLE, builtin.GPHDFS),
			Entry("classifies http:// locations correctly", "http://webhost:port/path/file", builtin.READABLE_WEB, builtin.HTTP),
			Entry("classifies https:// locations correctly", "https://webhost:port/path/file", builtin.READABLE_WEB, builtin.HTTP),
			Entry("classifies s3:// locations correctly", "s3://s3_endpoint:port/bucket_name/s3_prefix", builtin.READABLE, builtin.S3),
		)
	})
	Describe("PrintExternalTableCreateStatement", func() {
		var testTable builtin.Table
		var extTableDef builtin.ExternalTableDefinition
		BeforeEach(func() {
			testTable = builtin.Table{
				Relation:        builtin.Relation{Schema: "public", Name: "tablename"},
				TableDefinition: builtin.TableDefinition{DistPolicy: "DISTRIBUTED RANDOMLY", PartDef: "", PartTemplateDef: "", StorageOpts: "", TablespaceName: "", ColumnDefs: []builtin.ColumnDefinition{}, IsExternal: true, ExtTableDef: extTableEmpty}}
			extTableDef = extTableEmpty
		})

		It("prints a CREATE block for a READABLE EXTERNAL table", func() {
			extTableDef.Location = sql.NullString{String: "file://host:port/path/file", Valid: true}
			extTableDef.URIs = []string{"file://host:port/path/file"}
			testTable.ExtTableDef = extTableDef
			builtin.PrintExternalTableCreateStatement(backupfile, tocfile, testTable)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "tablename", "TABLE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE READABLE EXTERNAL TABLE public.tablename (
) LOCATION (
	'file://host:port/path/file'
)
FORMAT 'TEXT'
ENCODING 'UTF-8';`)
		})
		It("prints a CREATE block for a WRITABLE EXTERNAL table", func() {
			extTableDef.Location = sql.NullString{String: "file://host:port/path/file", Valid: true}
			extTableDef.URIs = []string{"file://host:port/path/file"}
			extTableDef.Writable = true
			testTable.ExtTableDef = extTableDef
			builtin.PrintExternalTableCreateStatement(backupfile, tocfile, testTable)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE WRITABLE EXTERNAL TABLE public.tablename (
) LOCATION (
	'file://host:port/path/file'
)
FORMAT 'TEXT'
ENCODING 'UTF-8'
DISTRIBUTED RANDOMLY;`)
		})
		It("prints a CREATE block for a READABLE EXTERNAL WEB table with a LOCATION", func() {
			extTableDef.Location = sql.NullString{String: "http://webhost:port/path/file", Valid: true}
			extTableDef.URIs = []string{"http://webhost:port/path/file"}
			testTable.ExtTableDef = extTableDef
			builtin.PrintExternalTableCreateStatement(backupfile, tocfile, testTable)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE READABLE EXTERNAL WEB TABLE public.tablename (
) LOCATION (
	'http://webhost:port/path/file'
)
FORMAT 'TEXT'
ENCODING 'UTF-8';`)
		})
		It("prints a CREATE block for a READABLE EXTERNAL WEB table with an EXECUTE", func() {
			extTableDef.Command = "hostname"
			testTable.ExtTableDef = extTableDef
			builtin.PrintExternalTableCreateStatement(backupfile, tocfile, testTable)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE READABLE EXTERNAL WEB TABLE public.tablename (
) EXECUTE 'hostname'
FORMAT 'TEXT'
ENCODING 'UTF-8';`)
		})
		It("prints a CREATE block for a WRITABLE EXTERNAL WEB table", func() {
			extTableDef.Command = "hostname"
			extTableDef.Writable = true
			testTable.ExtTableDef = extTableDef
			builtin.PrintExternalTableCreateStatement(backupfile, tocfile, testTable)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE WRITABLE EXTERNAL WEB TABLE public.tablename (
) EXECUTE 'hostname'
FORMAT 'TEXT'
ENCODING 'UTF-8'
DISTRIBUTED RANDOMLY;`)
		})
	})
	Describe("PrintExternalTableStatements", func() {
		var tableName = "public.tablename"
		var extTableDef builtin.ExternalTableDefinition
		BeforeEach(func() {
			extTableDef = extTableEmpty
			extTableDef.Type = builtin.READABLE
			extTableDef.Protocol = builtin.FILE
		})
		Context("EXECUTE options", func() {
			BeforeEach(func() {
				extTableDef = extTableEmpty
				extTableDef.Type = builtin.READABLE_WEB
				extTableDef.Protocol = builtin.HTTP
				extTableDef.Command = "hostname"
				extTableDef.FormatType = "t"
			})

			It("prints a CREATE block for a table with EXECUTE ON ALL", func() {
				builtin.PrintExternalTableStatements(backupfile, tableName, extTableDef)
				testhelper.ExpectRegexp(buffer, `EXECUTE 'hostname'
FORMAT 'TEXT'
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for a table with EXECUTE ON COORDINATOR", func() {
				extTableDef.ExecLocation = "COORDINATOR_ONLY"
				builtin.PrintExternalTableStatements(backupfile, tableName, extTableDef)
				testhelper.ExpectRegexp(buffer, `EXECUTE 'hostname' ON COORDINATOR
FORMAT 'TEXT'
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for a table with EXECUTE ON COORDINATOR", func() {
				extTableDef.ExecLocation = "COORDINATOR_ONLY"
				builtin.PrintExternalTableStatements(backupfile, tableName, extTableDef)
				testhelper.ExpectRegexp(buffer, `EXECUTE 'hostname' ON COORDINATOR
FORMAT 'TEXT'
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for a table with EXECUTE ON [number]", func() {
				extTableDef.ExecLocation = "TOTAL_SEGS:3"
				builtin.PrintExternalTableStatements(backupfile, tableName, extTableDef)
				testhelper.ExpectRegexp(buffer, `EXECUTE 'hostname' ON 3
FORMAT 'TEXT'
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for a table with EXECUTE ON HOST", func() {
				extTableDef.ExecLocation = "PER_HOST"
				builtin.PrintExternalTableStatements(backupfile, tableName, extTableDef)
				testhelper.ExpectRegexp(buffer, `EXECUTE 'hostname' ON HOST
FORMAT 'TEXT'
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for a table with EXECUTE ON HOST [host]", func() {
				extTableDef.ExecLocation = "HOST:localhost"
				builtin.PrintExternalTableStatements(backupfile, tableName, extTableDef)
				testhelper.ExpectRegexp(buffer, `EXECUTE 'hostname' ON HOST 'localhost'
FORMAT 'TEXT'
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for a table with EXECUTE ON SEGMENT [segid]", func() {
				extTableDef.ExecLocation = "SEGMENT_ID:0"
				builtin.PrintExternalTableStatements(backupfile, tableName, extTableDef)
				testhelper.ExpectRegexp(buffer, `EXECUTE 'hostname' ON SEGMENT 0
FORMAT 'TEXT'
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for a table with single quotes in its EXECUTE clause", func() {
				extTableDef.Command = "fake'command"
				builtin.PrintExternalTableStatements(backupfile, tableName, extTableDef)
				testhelper.ExpectRegexp(buffer, `EXECUTE 'fake''command'
FORMAT 'TEXT'
ENCODING 'UTF-8'`)
			})
		})
		Context("Miscellaneous options", func() {
			BeforeEach(func() {
				extTableDef = extTableEmpty
				extTableDef.Type = builtin.READABLE
				extTableDef.Protocol = builtin.FILE
				extTableDef.Location = sql.NullString{String: "file://host:port/path/file", Valid: true}
				extTableDef.URIs = []string{"file://host:port/path/file"}
			})

			It("prints a CREATE block for an S3 table with ON COORDINATOR", func() {
				extTableDef.Protocol = builtin.S3
				extTableDef.Location = sql.NullString{String: "s3://s3_endpoint:port/bucket_name/s3_prefix", Valid: true}
				extTableDef.URIs = []string{"s3://s3_endpoint:port/bucket_name/s3_prefix"}
				extTableDef.ExecLocation = "COORDINATOR_ONLY"
				builtin.PrintExternalTableStatements(backupfile, tableName, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOCATION (
	's3://s3_endpoint:port/bucket_name/s3_prefix'
) ON COORDINATOR
FORMAT 'TEXT'
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for an S3 table with ON COORDINATOR", func() {
				extTableDef.Protocol = builtin.S3
				extTableDef.Location = sql.NullString{String: "s3://s3_endpoint:port/bucket_name/s3_prefix", Valid: true}
				extTableDef.URIs = []string{"s3://s3_endpoint:port/bucket_name/s3_prefix"}
				extTableDef.ExecLocation = "COORDINATOR_ONLY"
				builtin.PrintExternalTableStatements(backupfile, tableName, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOCATION (
	's3://s3_endpoint:port/bucket_name/s3_prefix'
) ON COORDINATOR
FORMAT 'TEXT'
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for a table using error logging with an error table", func() {
				// Error tables were removed in GPDB 5
				if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("5")) || connectionPool.Version.IsCBDBFamily() {
					Skip("Test does not apply for GPDB versions after 5")
				}
				extTableDef.ErrTableName = "error_table"
				extTableDef.ErrTableSchema = "error_table_schema"
				builtin.PrintExternalTableStatements(backupfile, tableName, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOCATION (
	'file://host:port/path/file'
)
FORMAT 'TEXT'
ENCODING 'UTF-8'
LOG ERRORS INTO error_table_schema.error_table`)
			})
			It("prints a CREATE block for a table using error logging without an error table", func() {
				extTableDef.LogErrors = true
				builtin.PrintExternalTableStatements(backupfile, tableName, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOCATION (
	'file://host:port/path/file'
)
FORMAT 'TEXT'
ENCODING 'UTF-8'
LOG ERRORS`)
			})
			It("prints a LOG ERRORS PERSISTENTLY block for an external table using persistent error logging", func() {
				// PERSISTENTLY option is specifically for GPDB 5+
				if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("5")) || connectionPool.Version.IsCBDBFamily() {
					Skip("Test does not apply for GPDB versions after 5")
				}
				extTableDef.LogErrors = true
				extTableDef.LogErrPersist = true
				builtin.PrintExternalTableStatements(backupfile, tableName, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOG ERRORS PERSISTENTLY`)
			})
			It("prints a CREATE block for a table with a row-based reject limit", func() {
				extTableDef.RejectLimit = 2
				extTableDef.RejectLimitType = "r"
				builtin.PrintExternalTableStatements(backupfile, tableName, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOCATION (
	'file://host:port/path/file'
)
FORMAT 'TEXT'
ENCODING 'UTF-8'
SEGMENT REJECT LIMIT 2 ROWS`)
			})
			It("prints a CREATE block for a table with a percent-based reject limit", func() {
				extTableDef.RejectLimit = 2
				extTableDef.RejectLimitType = "p"
				builtin.PrintExternalTableStatements(backupfile, tableName, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOCATION (
	'file://host:port/path/file'
)
FORMAT 'TEXT'
ENCODING 'UTF-8'
SEGMENT REJECT LIMIT 2 PERCENT`)
			})
			It("prints a CREATE block for a table with error logging and a row-based reject limit", func() {
				extTableDef.LogErrors = true
				extTableDef.RejectLimit = 2
				extTableDef.RejectLimitType = "r"
				builtin.PrintExternalTableStatements(backupfile, tableName, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOCATION (
	'file://host:port/path/file'
)
FORMAT 'TEXT'
ENCODING 'UTF-8'
LOG ERRORS
SEGMENT REJECT LIMIT 2 ROWS`)
			})
		})
	})
	Describe("GenerateFormatStatement", func() {
		var extTableDef builtin.ExternalTableDefinition
		BeforeEach(func() {
			extTableDef = builtin.ExternalTableDefinition{}
		})
		Context("TEXT format", func() {
			It("generates a FORMAT statement with no options provided", func() {
				extTableDef.FormatType = "t"

				resultStatement := builtin.GenerateFormatStatement(extTableDef)

				Expect(resultStatement).To(Equal(`FORMAT 'TEXT'`))
			})
			It("generates a FORMAT statment with some options provided", func() {
				extTableDef.FormatType = "t"
				extTableDef.FormatOpts = `delimiter '\t' null '\N' escape '\'`

				resultStatement := builtin.GenerateFormatStatement(extTableDef)

				Expect(resultStatement).To(Equal(`FORMAT 'TEXT' (delimiter E'\\t' null E'\\N' escape E'\\')`))
			})
			It("generates a FORMAT statement with multi-word option", func() {
				extTableDef.FormatType = "t"
				extTableDef.FormatOpts = `delimiter '\t' null '\N' escape '\' fill missing fields`

				resultStatement := builtin.GenerateFormatStatement(extTableDef)

				Expect(resultStatement).To(Equal(`FORMAT 'TEXT' (delimiter E'\\t' null E'\\N' escape E'\\' fill missing fields)`))
			})
			It("generates a FORMAT statement with options containing whitespace", func() {
				extTableDef.FormatType = "t"
				extTableDef.FormatOpts = `delimiter ' ' null '
' escape '	'`

				resultStatement := builtin.GenerateFormatStatement(extTableDef)

				Expect(resultStatement).To(Equal(`FORMAT 'TEXT' (delimiter E' ' null E'
' escape E'	')`))
			})
			It("generates a FORMAT statement delimiter of special char", func() {
				extTableDef.FormatType = "t"
				extTableDef.FormatOpts = `delimiter '%' null '' escape 'OFF'`

				resultStatement := builtin.GenerateFormatStatement(extTableDef)

				Expect(resultStatement).To(Equal(`FORMAT 'TEXT' (delimiter E'%' null E'' escape E'OFF')`))
			})
		})
		Context("CSV format", func() {
			It("generates a FORMAT statement with no options provided", func() {
				extTableDef.FormatType = "c"

				resultStatement := builtin.GenerateFormatStatement(extTableDef)

				Expect(resultStatement).To(Equal(`FORMAT 'CSV'`))
			})
			It("generates a FORMAT statement with some options provided", func() {
				extTableDef.FormatType = "c"
				extTableDef.FormatOpts = `delimiter ',' null '' escape '"' quote '''`

				resultStatement := builtin.GenerateFormatStatement(extTableDef)

				Expect(resultStatement).To(Equal(`FORMAT 'CSV' (delimiter E',' null E'' escape E'"' quote E'\'')`))
			})
			It("generates a FORMAT statement with multi-word option", func() {
				extTableDef.FormatType = "c"
				extTableDef.FormatOpts = `delimiter ',' null '' quote ''' force quote column_name`

				resultStatement := builtin.GenerateFormatStatement(extTableDef)

				Expect(resultStatement).To(Equal(`FORMAT 'CSV' (delimiter E',' null E'' quote E'\'' force quote column_name)`))
			})
		})
		Context("CUSTOM format", func() {
			It("generates a FORMAT statement with formatter provided", func() {
				extTableDef.FormatType = "b"
				extTableDef.FormatOpts = `formatter 'gphdfs_import' other_opt 'foo'`

				resultStatement := builtin.GenerateFormatStatement(extTableDef)

				Expect(resultStatement).To(Equal(`FORMAT 'CUSTOM' (formatter = E'gphdfs_import', other_opt = E'foo')`))
			})
			It("generates a FORMAT statement with options containing whitespace", func() {
				extTableDef.FormatType = "b"
				extTableDef.FormatOpts = `formatter 'gphdfs_import' opt1 '	' opt2 '
'`

				resultStatement := builtin.GenerateFormatStatement(extTableDef)

				Expect(resultStatement).To(Equal(`FORMAT 'CUSTOM' (formatter = E'gphdfs_import', opt1 = E'	', opt2 = E'
')`))
			})
		})
		Context("AVRO format", func() {
			It("generates a FORMAT statement with no options provided", func() {
				extTableDef.FormatType = "a"

				resultStatement := builtin.GenerateFormatStatement(extTableDef)

				Expect(resultStatement).To(Equal("FORMAT 'AVRO'"))
			})
			It("generates a FORMAT statement with some options provided", func() {
				extTableDef.FormatType = "a"
				extTableDef.FormatOpts = `option1 'val1' option2 'val2'`

				resultStatement := builtin.GenerateFormatStatement(extTableDef)

				Expect(resultStatement).To(Equal("FORMAT 'AVRO' (option1 = E'val1', option2 = E'val2')"))
			})
		})
		Context("PARQUET format", func() {
			It("generates a FORMAT statement with no options provided", func() {
				extTableDef.FormatType = "p"

				resultStatement := builtin.GenerateFormatStatement(extTableDef)

				Expect(resultStatement).To(Equal("FORMAT 'PARQUET'"))
			})
			It("generates a FORMAT statement with some options provided", func() {
				extTableDef.FormatType = "p"
				extTableDef.FormatOpts = `option1 'val1' option2 'val2'`

				resultStatement := builtin.GenerateFormatStatement(extTableDef)

				Expect(resultStatement).To(Equal("FORMAT 'PARQUET' (option1 = E'val1', option2 = E'val2')"))
			})
		})
	})
	Describe("PrintExternalProtocolStatements", func() {
		protocolUntrustedReadWrite := builtin.ExternalProtocol{Oid: 1, Name: "s3", Owner: "testrole", Trusted: false, ReadFunction: 1, WriteFunction: 2, Validator: 0}
		protocolUntrustedReadValidator := builtin.ExternalProtocol{Oid: 1, Name: "s3", Owner: "testrole", Trusted: false, ReadFunction: 1, WriteFunction: 0, Validator: 3}
		protocolUntrustedWriteOnly := builtin.ExternalProtocol{Oid: 1, Name: "s3", Owner: "testrole", Trusted: false, ReadFunction: 0, WriteFunction: 2, Validator: 0}
		protocolTrustedReadWriteValidator := builtin.ExternalProtocol{Oid: 1, Name: "s3", Owner: "testrole", Trusted: true, ReadFunction: 1, WriteFunction: 2, Validator: 3}
		emptyMetadata := builtin.ObjectMetadata{}
		funcInfoMap := map[uint32]builtin.FunctionInfo{
			1: {QualifiedName: "public.read_fn_s3", Arguments: sql.NullString{String: "", Valid: true}, IsInternal: false},
			2: {QualifiedName: "public.write_fn_s3", Arguments: sql.NullString{String: "", Valid: true}, IsInternal: false},
			3: {QualifiedName: "public.validator", Arguments: sql.NullString{String: "", Valid: true}, IsInternal: false},
		}

		It("prints untrusted protocol with read and write function", func() {
			builtin.PrintCreateExternalProtocolStatement(backupfile, tocfile, protocolUntrustedReadWrite, funcInfoMap, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "", "", "s3", "PROTOCOL")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE PROTOCOL s3 (readfunc = public.read_fn_s3, writefunc = public.write_fn_s3);`)
		})
		It("prints untrusted protocol with read and validator", func() {
			builtin.PrintCreateExternalProtocolStatement(backupfile, tocfile, protocolUntrustedReadValidator, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE PROTOCOL s3 (readfunc = public.read_fn_s3, validatorfunc = public.validator);`)
		})
		It("prints untrusted protocol with write function only", func() {
			builtin.PrintCreateExternalProtocolStatement(backupfile, tocfile, protocolUntrustedWriteOnly, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE PROTOCOL s3 (writefunc = public.write_fn_s3);`)
		})
		It("prints trusted protocol with read, write, and validator", func() {
			builtin.PrintCreateExternalProtocolStatement(backupfile, tocfile, protocolTrustedReadWriteValidator, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TRUSTED PROTOCOL s3 (readfunc = public.read_fn_s3, writefunc = public.write_fn_s3, validatorfunc = public.validator);`)
		})
		It("prints a protocol with privileges and an owner", func() {
			protoMetadata := builtin.ObjectMetadata{Privileges: []builtin.ACL{{Grantee: "testrole", Select: true, Insert: true}}, Owner: "testrole"}

			builtin.PrintCreateExternalProtocolStatement(backupfile, tocfile, protocolUntrustedReadWrite, funcInfoMap, protoMetadata)
			expectedStatements := []string{
				"CREATE PROTOCOL s3 (readfunc = public.read_fn_s3, writefunc = public.write_fn_s3);",
				"ALTER PROTOCOL s3 OWNER TO testrole;",
				`REVOKE ALL ON PROTOCOL s3 FROM PUBLIC;
REVOKE ALL ON PROTOCOL s3 FROM testrole;
GRANT ALL ON PROTOCOL s3 TO testrole;`}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
		It("prints a protocol ACL even when the protocol's CREATE statement is skipped", func() {
			// The protocol create statement can be skipped if for example the
			// protocol is being created by an extension.

			// Functions belong to pg_catalog
			pgCatalogFuncInfoMap := map[uint32]builtin.FunctionInfo{
				1: {QualifiedName: "public.read_fn_s3", Arguments: sql.NullString{String: "", Valid: true}, IsInternal: true},
				2: {QualifiedName: "public.write_fn_s3", Arguments: sql.NullString{String: "", Valid: true}, IsInternal: true},
				3: {QualifiedName: "public.validator", Arguments: sql.NullString{String: "", Valid: true}, IsInternal: true},
			}
			protoMetadata := builtin.ObjectMetadata{Privileges: []builtin.ACL{{Grantee: "testrole", Select: true, Insert: true}}, Owner: "testrole"}

			builtin.PrintCreateExternalProtocolStatement(backupfile, tocfile, protocolUntrustedReadWrite, pgCatalogFuncInfoMap, protoMetadata)
			expectedStatements := []string{
				"ALTER PROTOCOL s3 OWNER TO testrole;",
				`REVOKE ALL ON PROTOCOL s3 FROM PUBLIC;
REVOKE ALL ON PROTOCOL s3 FROM testrole;
GRANT ALL ON PROTOCOL s3 TO testrole;`}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
	})
	Describe("PrintExchangeExternalPartitionStatements", func() {
		tables := []builtin.Table{
			{Relation: builtin.Relation{Oid: 1, Schema: "public", Name: "partition_table_ext_part_"}},
			{Relation: builtin.Relation{Oid: 2, Schema: "public", Name: "partition_table"}},
		}
		emptyPartInfoMap := make(map[uint32]builtin.PartitionInfo)
		It("writes an alter statement for a named partition", func() {
			externalPartition := builtin.PartitionInfo{
				PartitionRuleOid:       1,
				PartitionParentRuleOid: 0,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "partition_table",
				RelationOid:            1,
				PartitionName:          "partition_name",
				PartitionRank:          0,
				IsExternal:             true,
			}
			externalPartitions := []builtin.PartitionInfo{externalPartition}
			builtin.PrintExchangeExternalPartitionStatements(backupfile, tocfile, externalPartitions, emptyPartInfoMap, tables)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `ALTER TABLE public.partition_table EXCHANGE PARTITION partition_name WITH TABLE public.partition_table_ext_part_ WITHOUT VALIDATION;

DROP TABLE public.partition_table_ext_part_;`)
		})
		It("writes an alter statement using rank for an unnamed partition", func() {
			externalPartition := builtin.PartitionInfo{
				PartitionRuleOid:       1,
				PartitionParentRuleOid: 0,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "partition_table",
				RelationOid:            1,
				PartitionName:          "",
				PartitionRank:          1,
				IsExternal:             true,
			}
			externalPartitions := []builtin.PartitionInfo{externalPartition}
			builtin.PrintExchangeExternalPartitionStatements(backupfile, tocfile, externalPartitions, emptyPartInfoMap, tables)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `ALTER TABLE public.partition_table EXCHANGE PARTITION FOR (RANK(1)) WITH TABLE public.partition_table_ext_part_ WITHOUT VALIDATION;

DROP TABLE public.partition_table_ext_part_;`)
		})
		It("writes an alter statement using rank for a two level partition", func() {
			externalPartition := builtin.PartitionInfo{
				PartitionRuleOid:       10,
				PartitionParentRuleOid: 11,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "partition_table",
				RelationOid:            1,
				PartitionName:          "",
				PartitionRank:          1,
				IsExternal:             true,
			}
			externalPartitionParent := builtin.PartitionInfo{
				PartitionRuleOid:       11,
				PartitionParentRuleOid: 0,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "partition_table",
				RelationOid:            0,
				PartitionName:          "",
				PartitionRank:          3,
				IsExternal:             false,
			}
			partInfoMap := map[uint32]builtin.PartitionInfo{externalPartitionParent.PartitionRuleOid: externalPartitionParent}
			externalPartitions := []builtin.PartitionInfo{externalPartition}
			builtin.PrintExchangeExternalPartitionStatements(backupfile, tocfile, externalPartitions, partInfoMap, tables)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `ALTER TABLE public.partition_table ALTER PARTITION FOR (RANK(3)) EXCHANGE PARTITION FOR (RANK(1)) WITH TABLE public.partition_table_ext_part_ WITHOUT VALIDATION;

DROP TABLE public.partition_table_ext_part_;`)
		})
		It("writes an alter statement using partition name for a two level partition", func() {
			externalPartition := builtin.PartitionInfo{
				PartitionRuleOid:       10,
				PartitionParentRuleOid: 11,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "partition_table",
				RelationOid:            1,
				PartitionName:          "",
				PartitionRank:          1,
				IsExternal:             true,
			}
			externalPartitionParent := builtin.PartitionInfo{
				PartitionRuleOid:       11,
				PartitionParentRuleOid: 0,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "partition_table",
				RelationOid:            3,
				PartitionName:          "partition_name",
				PartitionRank:          0,
				IsExternal:             false,
			}
			partInfoMap := map[uint32]builtin.PartitionInfo{externalPartitionParent.PartitionRuleOid: externalPartitionParent}
			externalPartitions := []builtin.PartitionInfo{externalPartition}
			builtin.PrintExchangeExternalPartitionStatements(backupfile, tocfile, externalPartitions, partInfoMap, tables)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `ALTER TABLE public.partition_table ALTER PARTITION partition_name EXCHANGE PARTITION FOR (RANK(1)) WITH TABLE public.partition_table_ext_part_ WITHOUT VALIDATION;

DROP TABLE public.partition_table_ext_part_;`)
		})
		It("writes an alter statement for a three level partition", func() {
			externalPartition := builtin.PartitionInfo{
				PartitionRuleOid:       10,
				PartitionParentRuleOid: 11,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "partition_table",
				RelationOid:            1,
				PartitionName:          "",
				PartitionRank:          1,
				IsExternal:             true,
			}
			externalPartitionParent1 := builtin.PartitionInfo{
				PartitionRuleOid:       11,
				PartitionParentRuleOid: 12,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "partition_table",
				RelationOid:            0,
				PartitionName:          "partition_name",
				PartitionRank:          0,
				IsExternal:             false,
			}
			externalPartitionParent2 := builtin.PartitionInfo{
				PartitionRuleOid:       12,
				PartitionParentRuleOid: 0,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "partition_table",
				RelationOid:            0,
				PartitionName:          "",
				PartitionRank:          3,
				IsExternal:             false,
			}
			partInfoMap := map[uint32]builtin.PartitionInfo{externalPartitionParent1.PartitionRuleOid: externalPartitionParent1, externalPartitionParent2.PartitionRuleOid: externalPartitionParent2}
			externalPartitions := []builtin.PartitionInfo{externalPartition}
			builtin.PrintExchangeExternalPartitionStatements(backupfile, tocfile, externalPartitions, partInfoMap, tables)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `ALTER TABLE public.partition_table ALTER PARTITION FOR (RANK(3)) ALTER PARTITION partition_name EXCHANGE PARTITION FOR (RANK(1)) WITH TABLE public.partition_table_ext_part_ WITHOUT VALIDATION;

DROP TABLE public.partition_table_ext_part_;`)
		})
	})
})
