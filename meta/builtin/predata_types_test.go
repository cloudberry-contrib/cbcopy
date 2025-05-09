package builtin_test

import (
	"database/sql"
	"fmt"

	// "github.com/greenplum-db/gpbackup/backup"
	"github.com/cloudberrydb/cbcopy/meta/builtin"

	// "github.com/greenplum-db/gpbackup/testutils"
	"github.com/cloudberrydb/cbcopy/testutils"

	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("backup/predata_types tests", func() {
	emptyMetadata := builtin.ObjectMetadata{}
	emptyMetadataMap := builtin.MetadataMap{}
	typeMetadata := testutils.DefaultMetadata("TYPE", false, true, true, true)
	typeMetadataMap := testutils.DefaultMetadataMap("TYPE", false, true, true, true)

	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintCreateEnumTypeStatements", func() {
		enumOne := builtin.EnumType{Oid: 1, Schema: "public", Name: "enum_type", EnumLabels: "'bar',\n\t'baz',\n\t'foo'"}
		enumTwo := builtin.EnumType{Oid: 1, Schema: "public", Name: "enum_type", EnumLabels: "'bar',\n\t'baz',\n\t'foo'"}

		It("prints an enum type with multiple attributes", func() {
			builtin.PrintCreateEnumTypeStatements(backupfile, tocfile, []builtin.EnumType{enumOne}, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "enum_type", "TYPE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.enum_type AS ENUM (
	'bar',
	'baz',
	'foo'
);`)
		})
		It("prints an enum type with comment, security label, and owner", func() {
			builtin.PrintCreateEnumTypeStatements(backupfile, tocfile, []builtin.EnumType{enumTwo}, typeMetadataMap)
			expectedStatements := []string{`CREATE TYPE public.enum_type AS ENUM (
	'bar',
	'baz',
	'foo'
);`,
				"COMMENT ON TYPE public.enum_type IS 'This is a type comment.';",
				"ALTER TYPE public.enum_type OWNER TO testrole;",
				"SECURITY LABEL FOR dummy ON TYPE public.enum_type IS 'unclassified';"}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
	})
	Describe("PrintCreateCompositeTypeStatement", func() {
		var compType builtin.CompositeType
		var oneAtt, oneAttWithCollation, twoAtts, attWithComment []builtin.Attribute
		BeforeEach(func() {
			compType = builtin.CompositeType{Oid: 1, Schema: "public", Name: "composite_type"}
			oneAtt = []builtin.Attribute{{Name: "foo", Type: "integer"}}
			oneAttWithCollation = []builtin.Attribute{{Name: "foo", Type: "integer", Collation: "public.some_coll"}}
			twoAtts = []builtin.Attribute{{Name: "foo", Type: "integer"}, {Name: "bar", Type: "text"}}
			attWithComment = []builtin.Attribute{{Name: "foo", Type: "integer", Comment: "'attribute comment'"}}
		})

		It("prints a composite type with one attribute", func() {
			compType.Attributes = oneAtt
			builtin.PrintCreateCompositeTypeStatement(backupfile, tocfile, compType, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "composite_type", "TYPE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.composite_type AS (
	foo integer
);`)
		})
		It("prints a composite type with one attribute with a collation", func() {
			compType.Attributes = oneAttWithCollation
			builtin.PrintCreateCompositeTypeStatement(backupfile, tocfile, compType, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "composite_type", "TYPE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.composite_type AS (
	foo integer COLLATE public.some_coll
);`)
		})
		It("prints a composite type with multiple attributes", func() {
			compType.Attributes = twoAtts
			builtin.PrintCreateCompositeTypeStatement(backupfile, tocfile, compType, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.composite_type AS (
	foo integer,
	bar text
);`)
		})
		It("prints a composite type with comment, security label, and owner", func() {
			compType.Attributes = twoAtts
			builtin.PrintCreateCompositeTypeStatement(backupfile, tocfile, compType, typeMetadata)
			expectedEntries := []string{`CREATE TYPE public.composite_type AS (
	foo integer,
	bar text
);`,
				"COMMENT ON TYPE public.composite_type IS 'This is a type comment.';",
				"ALTER TYPE public.composite_type OWNER TO testrole;",
				"SECURITY LABEL FOR dummy ON TYPE public.composite_type IS 'unclassified';"}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedEntries...)
		})
		It("prints a composite type with attribute comment", func() {
			compType.Attributes = attWithComment
			builtin.PrintCreateCompositeTypeStatement(backupfile, tocfile, compType, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.composite_type AS (
	foo integer
);`, "COMMENT ON COLUMN public.composite_type.foo IS 'attribute comment';")
		})
	})
	Describe("PrintCreateBaseTypeStatement", func() {
		baseSimple := builtin.BaseType{Oid: 1, Schema: "public", Name: "base_type", Input: "input_fn", Output: "output_fn", Receive: "", Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "c", Storage: "p", DefaultVal: "", Element: "", Category: "U", Preferred: false, Delimiter: ""}
		basePartial := builtin.BaseType{Oid: 1, Schema: "public", Name: "base_type", Input: "input_fn", Output: "output_fn", Receive: "receive_fn", Send: "send_fn", ModIn: "modin_fn", ModOut: "modout_fn", InternalLength: -1, IsPassedByValue: false, Alignment: "c", Storage: "p", DefaultVal: "42", Element: "int4", Category: "U", Delimiter: ","}
		baseFull := builtin.BaseType{Oid: 1, Schema: "public", Name: "base_type", Input: "input_fn", Output: "output_fn", Receive: "receive_fn", Send: "send_fn", ModIn: "modin_fn", ModOut: "modout_fn", InternalLength: 16, IsPassedByValue: true, Alignment: "s", Storage: "e", DefaultVal: "42", Element: "int4", Category: "N", Preferred: true, Delimiter: ",", StorageOptions: "compresstype=zlib, compresslevel=1, blocksize=32768", Collatable: true}
		basePermOne := builtin.BaseType{Oid: 1, Schema: "public", Name: "base_type", Input: "input_fn", Output: "output_fn", Receive: "", Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "d", Storage: "m", DefaultVal: "", Element: "", Category: "U", Delimiter: ""}
		basePermTwo := builtin.BaseType{Oid: 1, Schema: "public", Name: "base_type", Input: "input_fn", Output: "output_fn", Receive: "", Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "i", Storage: "x", DefaultVal: "", Element: "", Category: "U", Delimiter: ""}

		It("prints a base type with no optional arguments", func() {
			builtin.PrintCreateBaseTypeStatement(backupfile, tocfile, baseSimple, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "base_type", "TYPE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn
);`)
		})
		It("prints a base type where all optional arguments have default values where possible", func() {
			expectedArgsReplace := ""
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("5")) || connectionPool.Version.IsCBDBFamily() {
				expectedArgsReplace = `
	TYPMOD_IN = modin_fn,
	TYPMOD_OUT = modout_fn,`
			}

			builtin.PrintCreateBaseTypeStatement(backupfile, tocfile, basePartial, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, fmt.Sprintf(`CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	RECEIVE = receive_fn,
	SEND = send_fn,%s
	DEFAULT = '42',
	ELEMENT = int4,
	DELIMITER = ','
);`, expectedArgsReplace))
		})
		It("prints a base type with all optional arguments provided", func() {
			expectedArgsReplace := ""
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("5")) || connectionPool.Version.IsCBDBFamily() {
				expectedArgsReplace = `
	TYPMOD_IN = modin_fn,
	TYPMOD_OUT = modout_fn,`
			}

			builtin.PrintCreateBaseTypeStatement(backupfile, tocfile, baseFull, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, fmt.Sprintf(`CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	RECEIVE = receive_fn,
	SEND = send_fn,%s
	INTERNALLENGTH = 16,
	PASSEDBYVALUE,
	ALIGNMENT = int2,
	STORAGE = external,
	DEFAULT = '42',
	ELEMENT = int4,
	DELIMITER = ',',
	CATEGORY = 'N',
	PREFERRED = true,
	COLLATABLE = true
);

ALTER TYPE public.base_type
	SET DEFAULT ENCODING (compresstype=zlib, compresslevel=1, blocksize=32768);`, expectedArgsReplace))
		})
		It("prints a base type with double alignment and main storage", func() {
			builtin.PrintCreateBaseTypeStatement(backupfile, tocfile, basePermOne, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	ALIGNMENT = double,
	STORAGE = main
);`)
		})
		It("prints a base type with int4 alignment and external storage", func() {
			builtin.PrintCreateBaseTypeStatement(backupfile, tocfile, basePermTwo, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	ALIGNMENT = int4,
	STORAGE = extended
);`)
		})
		It("prints a base type with comment, security label, and owner", func() {
			builtin.PrintCreateBaseTypeStatement(backupfile, tocfile, baseSimple, typeMetadata)
			expectedEntries := []string{`CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn
);`,
				"COMMENT ON TYPE public.base_type IS 'This is a type comment.';",
				"ALTER TYPE public.base_type OWNER TO testrole;",
				"SECURITY LABEL FOR dummy ON TYPE public.base_type IS 'unclassified';"}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedEntries...)
		})
	})
	Describe("PrintCreateShellTypeStatements", func() {
		shellOne := builtin.ShellType{Oid: 1, Schema: "public", Name: "shell_type1"}
		baseOne := builtin.BaseType{Oid: 1, Schema: "public", Name: "base_type1", Input: "input_fn", Output: "output_fn", Receive: "", Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "c", Storage: "p", DefaultVal: "", Element: "", Category: "U", Delimiter: ""}
		baseTwo := builtin.BaseType{Oid: 1, Schema: "public", Name: "base_type2", Input: "input_fn", Output: "output_fn", Receive: "", Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "c", Storage: "p", DefaultVal: "", Element: "", Category: "U", Delimiter: ""}
		rangeOne := builtin.RangeType{Oid: 1, Schema: "public", Name: "range_type1"}
		It("prints shell type for a shell type", func() {
			builtin.PrintCreateShellTypeStatements(backupfile, tocfile, []builtin.ShellType{shellOne}, []builtin.BaseType{}, []builtin.RangeType{})
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "shell_type1", "TYPE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, "CREATE TYPE public.shell_type1;")
		})
		It("prints shell type for a base type", func() {
			builtin.PrintCreateShellTypeStatements(backupfile, tocfile, []builtin.ShellType{}, []builtin.BaseType{baseOne, baseTwo}, []builtin.RangeType{})
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "base_type1", "TYPE")
			testutils.ExpectEntry(tocfile.PredataEntries, 1, "public", "", "base_type2", "TYPE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, "CREATE TYPE public.base_type1;", "CREATE TYPE public.base_type2;")
		})
		It("prints shell type for a range type", func() {
			builtin.PrintCreateShellTypeStatements(backupfile, tocfile, []builtin.ShellType{}, []builtin.BaseType{}, []builtin.RangeType{rangeOne})
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "range_type1", "TYPE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, "CREATE TYPE public.range_type1;")
		})
	})
	Describe("PrintCreateDomainStatement", func() {
		emptyConstraint := make([]builtin.Constraint, 0)
		checkConstraint := []builtin.Constraint{{Name: "domain1_check", Def: sql.NullString{String: "CHECK (VALUE > 2)", Valid: true}, OwningObject: "public.domain1"}}
		domainOne := builtin.Domain{Oid: 1, Schema: "public", Name: "domain1", DefaultVal: "4", BaseType: "numeric", NotNull: true, Collation: "public.mycollation"}
		domainTwo := builtin.Domain{Oid: 1, Schema: "public", Name: "domain2", DefaultVal: "", BaseType: "varchar", NotNull: false, Collation: ""}
		It("prints a domain with a constraint", func() {
			builtin.PrintCreateDomainStatement(backupfile, tocfile, domainOne, emptyMetadata, checkConstraint)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "domain1", "DOMAIN")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE DOMAIN public.domain1 AS numeric DEFAULT 4 COLLATE public.mycollation NOT NULL
	CONSTRAINT domain1_check CHECK (VALUE > 2);`)
		})
		It("prints a domain without constraint", func() {
			builtin.PrintCreateDomainStatement(backupfile, tocfile, domainOne, emptyMetadata, emptyConstraint)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE DOMAIN public.domain1 AS numeric DEFAULT 4 COLLATE public.mycollation NOT NULL;`)
		})
		It("prints a domain without constraint with comment, security label, and owner", func() {
			domainMetadata := testutils.DefaultMetadata("DOMAIN", false, true, true, true)
			builtin.PrintCreateDomainStatement(backupfile, tocfile, domainTwo, domainMetadata, emptyConstraint)
			expectedEntries := []string{"CREATE DOMAIN public.domain2 AS varchar;",
				"COMMENT ON DOMAIN public.domain2 IS 'This is a domain comment.';",
				"ALTER DOMAIN public.domain2 OWNER TO testrole;",
				"SECURITY LABEL FOR dummy ON DOMAIN public.domain2 IS 'unclassified';"}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedEntries...)
		})
	})
	Describe("PrintCreateRangeTypeStatement", func() {
		basicRangeType := builtin.RangeType{
			Name:    "rangetype",
			Schema:  "public",
			SubType: "test_subtype_schema.test_subtype",
		}
		complexRangeType := builtin.RangeType{
			Name: "rangetype", Schema: "public",
			SubType:        "test_subtype_schema.test_subtype",
			SubTypeOpClass: "opclass_schema.test_opclass",
			Collation:      "collation_schema.test_collation",
			Canonical:      "canonical_schema.test_canonical",
			SubTypeDiff:    "diff_schema.test_diff",
		}
		It("prints a basic range type", func() {
			builtin.PrintCreateRangeTypeStatement(backupfile, tocfile, basicRangeType, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "rangetype", "TYPE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.rangetype AS RANGE (
	SUBTYPE = test_subtype_schema.test_subtype
);`)
		})
		It("prints a complex range type", func() {
			builtin.PrintCreateRangeTypeStatement(backupfile, tocfile, complexRangeType, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "rangetype", "TYPE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.rangetype AS RANGE (
	SUBTYPE = test_subtype_schema.test_subtype,
	SUBTYPE_OPCLASS = opclass_schema.test_opclass,
	COLLATION = collation_schema.test_collation,
	CANONICAL = canonical_schema.test_canonical,
	SUBTYPE_DIFF = diff_schema.test_diff
);`)
		})
		It("prints a range type with an owner, security label, and a comment", func() {
			builtin.PrintCreateRangeTypeStatement(backupfile, tocfile, basicRangeType, typeMetadata)
			expectedStatements := []string{`CREATE TYPE public.rangetype AS RANGE (
	SUBTYPE = test_subtype_schema.test_subtype
);`,
				"COMMENT ON TYPE public.rangetype IS 'This is a type comment.';",
				"ALTER TYPE public.rangetype OWNER TO testrole;",
				"SECURITY LABEL FOR dummy ON TYPE public.rangetype IS 'unclassified';"}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
	})
	Describe("PrintCreateCollationStatement", func() {
		It("prints a create collation statement", func() {
			collation := builtin.Collation{Oid: 1, Name: "collation1", Collate: "collate1", Ctype: "ctype1", Schema: "schema1"}
			builtin.PrintCreateCollationStatements(backupfile, tocfile, []builtin.Collation{collation}, emptyMetadataMap)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE COLLATION schema1.collation1 (LC_COLLATE = 'collate1', LC_CTYPE = 'ctype1');`)
		})
		It("prints a create collation statement with owner and comment", func() {
			collation := builtin.Collation{Oid: 1, Name: "collation1", Collate: "collate1", Ctype: "ctype1", Schema: "schema1"}
			collationMetadataMap := testutils.DefaultMetadataMap("COLLATION", false, true, true, false)
			builtin.PrintCreateCollationStatements(backupfile, tocfile, []builtin.Collation{collation}, collationMetadataMap)
			expectedStatements := []string{
				"CREATE COLLATION schema1.collation1 (LC_COLLATE = 'collate1', LC_CTYPE = 'ctype1');",
				"COMMENT ON COLLATION schema1.collation1 IS 'This is a collation comment.';",
				"ALTER COLLATION schema1.collation1 OWNER TO testrole;"}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
	})
})
