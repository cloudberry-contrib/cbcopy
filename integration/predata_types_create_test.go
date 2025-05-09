package integration

import (
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

var _ = Describe("cbcopy integration create statement tests", func() {
	var (
		emptyMetadata    builtin.ObjectMetadata
		emptyMetadataMap builtin.MetadataMap
	)
	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
		emptyMetadata = builtin.ObjectMetadata{}
		emptyMetadataMap = builtin.MetadataMap{}
	})
	Describe("PrintTypeStatements", func() {
		var (
			shellType builtin.ShellType
			baseType  builtin.BaseType
			rangeType builtin.RangeType
		)
		BeforeEach(func() {
			shellType = builtin.ShellType{Schema: "public", Name: "shell_type"}
			baseType = builtin.BaseType{
				Schema: "public", Name: "base_type", Input: "public.base_fn_in", Output: "public.base_fn_out", Receive: "",
				Send: "", ModIn: "", ModOut: "", InternalLength: 4, IsPassedByValue: true, Alignment: "i", Storage: "p",
				DefaultVal: "default", Element: "text", Category: "U", Preferred: false, Delimiter: ";", StorageOptions: "compresstype=zlib, compresslevel=1, blocksize=32768",
			}
			rangeType = builtin.RangeType{
				Oid:            0,
				Schema:         "public",
				Name:           "textrange",
				SubType:        "text",
				Collation:      "public.some_coll",
				SubTypeOpClass: "pg_catalog.text_ops",
			}
		})
		Describe("PrintCreateShellTypeStatements", func() {
			It("creates shell types for base, shell and range types", func() {
				builtin.PrintCreateShellTypeStatements(backupfile, tocfile, []builtin.ShellType{shellType}, []builtin.BaseType{baseType}, []builtin.RangeType{rangeType})

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.shell_type")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.base_type")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.textrange")

				shells := builtin.GetShellTypes(connectionPool)
				Expect(shells).To(HaveLen(3))
				Expect(shells[0].Name).To(Equal("base_type"))
				Expect(shells[1].Name).To(Equal("shell_type"))
				Expect(shells[2].Name).To(Equal("textrange"))
			})
		})

		Describe("PrintCreateCompositeTypeStatement", func() {
			var compositeType builtin.CompositeType
			BeforeEach(func() {
				atts := []builtin.Attribute{{Name: "att1", Type: "text"}, {Name: "att2", Type: "integer"}}
				compositeType = builtin.CompositeType{
					Schema: "public", Name: "composite_type", Attributes: atts,
				}
			})
			It("creates composite types", func() {
				builtin.PrintCreateCompositeTypeStatement(backupfile, tocfile, compositeType, emptyMetadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.composite_type")

				resultTypes := builtin.GetCompositeTypes(connectionPool)

				Expect(resultTypes).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&compositeType, &resultTypes[0], "Oid", "Attributes.CompositeTypeOid")
			})
			It("creates composite types with a collation", func() {
				testutils.SkipIfBefore6(connectionPool)
				testhelper.AssertQueryRuns(connectionPool, `CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.some_coll")
				compositeType.Attributes[0].Collation = "public.some_coll"
				builtin.PrintCreateCompositeTypeStatement(backupfile, tocfile, compositeType, emptyMetadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.composite_type")

				resultTypes := builtin.GetCompositeTypes(connectionPool)

				Expect(resultTypes).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&compositeType, &resultTypes[0], "Oid", "Attributes.CompositeTypeOid")
			})
			It("creates composite types with attribute comments", func() {
				compositeType.Attributes[0].Comment = "'comment for att1'"
				builtin.PrintCreateCompositeTypeStatement(backupfile, tocfile, compositeType, emptyMetadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.composite_type")

				resultTypes := builtin.GetCompositeTypes(connectionPool)

				Expect(resultTypes).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&compositeType, &resultTypes[0], "Oid", "Attributes.CompositeTypeOid")
			})
		})
		Describe("PrintCreateBaseTypeStatement", func() {
			/* comment out, due to CBDB/PG14 - GP7/PG12 behavior diff
			It("creates base types", func() {
				if connectionPool.Version.AtLeast("6") {
					baseType.Category = "N"
					baseType.Preferred = true
					baseType.Collatable = true
				}
				metadata := testutils.DefaultMetadata("TYPE", false, true, true, includeSecurityLabels)
				builtin.PrintCreateBaseTypeStatement(backupfile, tocfile, baseType, metadata)

				//Run queries to set up the database state so we can successfully create base types
				testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.base_type")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.base_type CASCADE")
				testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.base_fn_in(cstring) RETURNS public.base_type AS 'boolin' LANGUAGE internal")
				testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.base_fn_out(public.base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")

				testhelper.AssertQueryRuns(connectionPool, buffer.String())

				resultTypes := builtin.GetBaseTypes(connectionPool)

				Expect(resultTypes).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&baseType, &resultTypes[0], "Oid")
			})
			*/
		})
		Describe("PrintCreateEnumTypeStatements", func() {
			It("creates enum types", func() {
				testutils.SkipIfBefore5(connectionPool)
				enumType := builtin.EnumType{Schema: "public", Name: "enum_type", EnumLabels: "'enum_labels'"}
				enums := []builtin.EnumType{enumType}
				builtin.PrintCreateEnumTypeStatements(backupfile, tocfile, enums, emptyMetadataMap)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.enum_type")

				resultTypes := builtin.GetEnumTypes(connectionPool)

				Expect(resultTypes).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&resultTypes[0], &enumType, "Oid")
			})
		})
		Describe("PrintCreateDomainStatement", func() {
			domainType := builtin.Domain{
				Oid: 1, Schema: "public", Name: "domain_type", BaseType: "character(8)", DefaultVal: "'abc'::bpchar", NotNull: true, Collation: ""}
			It("creates domain types", func() {
				constraints := make([]builtin.Constraint, 0)
				if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
					testhelper.AssertQueryRuns(connectionPool, "CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX')")
					defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.some_coll")
					domainType.Collation = "public.some_coll"
				}
				metadata := testutils.DefaultMetadata("DOMAIN", false, true, true, includeSecurityLabels)
				builtin.PrintCreateDomainStatement(backupfile, tocfile, domainType, metadata, constraints)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.domain_type")

				resultTypes := builtin.GetDomainTypes(connectionPool)

				Expect(resultTypes).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchIncluding(&domainType, &resultTypes[0], "Schema", "Name", "Type", "DefaultVal", "BaseType", "NotNull", "Collation")
			})
		})
		Describe("PrintCreateRangeTypeStatement", func() {
			It("creates a range type with a collation and opclass", func() {
				testutils.SkipIfBefore6(connectionPool)
				testhelper.AssertQueryRuns(connectionPool, "CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.some_coll")

				metadata := testutils.DefaultMetadata("TYPE", false, true, true, includeSecurityLabels)
				builtin.PrintCreateRangeTypeStatement(backupfile, tocfile, rangeType, metadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.textrange")

				resultTypes := builtin.GetRangeTypes(connectionPool)

				Expect(len(resultTypes)).To(Equal(1))
				structmatcher.ExpectStructsToMatchExcluding(&rangeType, &resultTypes[0], "Oid")
			})
			It("creates a range type in a specific schema with a subtype diff function", func() {
				testutils.SkipIfBefore6(connectionPool)
				rangeType := builtin.RangeType{
					Oid:            0,
					Schema:         "testschema",
					Name:           "timerange",
					SubType:        "time without time zone",
					SubTypeOpClass: "pg_catalog.time_ops",
					SubTypeDiff:    "testschema.time_subtype_diff",
				}
				testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema;")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema CASCADE;")
				testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION testschema.time_subtype_diff(x time, y time) RETURNS float8 AS 'SELECT EXTRACT(EPOCH FROM (x - y))' LANGUAGE sql STRICT IMMUTABLE;")

				builtin.PrintCreateRangeTypeStatement(backupfile, tocfile, rangeType, emptyMetadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())

				resultTypes := builtin.GetRangeTypes(connectionPool)

				Expect(len(resultTypes)).To(Equal(1))
				structmatcher.ExpectStructsToMatchExcluding(&rangeType, &resultTypes[0], "Oid")
			})
		})
	})
	Describe("PrintCreateCollationStatement", func() {
		It("creates a basic collation", func() {
			testutils.SkipIfBefore6(connectionPool)
			collation := builtin.Collation{Oid: 1, Schema: "public", Name: "testcollation", Collate: "POSIX", Ctype: "POSIX"}
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily() {
				collation.IsDeterministic = "true"
				collation.Provider = "c"
			}
			builtin.PrintCreateCollationStatements(backupfile, tocfile, []builtin.Collation{collation}, builtin.MetadataMap{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.testcollation")

			resultCollations := builtin.GetCollations(connectionPool)

			Expect(resultCollations).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&collation, &resultCollations[0], "Oid")
		})
		It("creates a basic collation with comment and owner", func() {
			testutils.SkipIfBefore6(connectionPool)
			collation := builtin.Collation{Oid: 1, Schema: "public", Name: "testcollation", Collate: "POSIX", Ctype: "POSIX"}
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily() {
				collation.IsDeterministic = "true"
				collation.Provider = "c"
			}
			collationMetadataMap := testutils.DefaultMetadataMap("COLLATION", false, true, true, false)
			collationMetadata := collationMetadataMap[collation.GetUniqueID()]

			builtin.PrintCreateCollationStatements(backupfile, tocfile, []builtin.Collation{collation}, collationMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.testcollation")

			resultCollations := builtin.GetCollations(connectionPool)
			resultMetadataMap := builtin.GetMetadataForObjectType(connectionPool, builtin.TYPE_COLLATION)

			Expect(resultCollations).To(HaveLen(1))
			uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testcollation", builtin.TYPE_COLLATION)
			resultMetadata := resultMetadataMap[uniqueID]
			structmatcher.ExpectStructsToMatchExcluding(&collation, &resultCollations[0], "Oid")
			structmatcher.ExpectStructsToMatch(&collationMetadata, &resultMetadata)

		})
		/* comment out, due to CBDB/PG14 - GP7/PG12 behavior diff
		It("creates a specific collation", func() {
			testutils.SkipIfBefore7(connectionPool)
			collation := builtin.Collation{Oid: 1, Schema: "public", Name: "testcollation", Collate: "de_DE", Ctype: "de_DE", Provider: "c", IsDeterministic: "true"}
			builtin.PrintCreateCollationStatements(backupfile, tocfile, []builtin.Collation{collation}, builtin.MetadataMap{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.testcollation")

			resultCollations := builtin.GetCollations(connectionPool)

			Expect(resultCollations).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&collation, &resultCollations[0], "Oid")
		})
		*/
	})
})
