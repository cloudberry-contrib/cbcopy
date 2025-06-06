package builtin_test

import (

	// "github.com/greenplum-db/gpbackup/backup"
	"github.com/cloudberrydb/cbcopy/meta/builtin"

	// "github.com/greenplum-db/gpbackup/testutils"
	"github.com/cloudberrydb/cbcopy/testutils"

	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("backup/predata_textsearch tests", func() {
	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintCreateTextSearchParserStatements", func() {
		It("prints a basic text search parser", func() {
			parser := builtin.TextSearchParser{Oid: 0, Schema: "public", Name: "testparser", StartFunc: "start_func", TokenFunc: "token_func", EndFunc: "end_func", LexTypesFunc: "lextypes_func"}
			builtin.PrintCreateTextSearchParserStatement(backupfile, tocfile, parser, builtin.ObjectMetadata{})
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "testparser", "TEXT SEARCH PARSER")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TEXT SEARCH PARSER public.testparser (
	START = start_func,
	GETTOKEN = token_func,
	END = end_func,
	LEXTYPES = lextypes_func
);`)
		})
		It("prints a text search parser with a headline and comment", func() {
			parser := builtin.TextSearchParser{Oid: 1, Schema: "public", Name: "testparser", StartFunc: "start_func", TokenFunc: "token_func", EndFunc: "end_func", LexTypesFunc: "lextypes_func", HeadlineFunc: "headline_func"}
			metadata := testutils.DefaultMetadata("TEXT SEARCH PARSER", false, false, true, false)
			builtin.PrintCreateTextSearchParserStatement(backupfile, tocfile, parser, metadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TEXT SEARCH PARSER public.testparser (
	START = start_func,
	GETTOKEN = token_func,
	END = end_func,
	LEXTYPES = lextypes_func,
	HEADLINE = headline_func
);`, `COMMENT ON TEXT SEARCH PARSER public.testparser IS 'This is a text search parser comment.';`)
		})
	})
	Describe("PrintCreateTextSearchTemplateStatement", func() {
		It("prints a basic text search template with comment", func() {
			template := builtin.TextSearchTemplate{Oid: 1, Schema: "public", Name: "testtemplate", InitFunc: "dsimple_init", LexizeFunc: "dsimple_lexize"}
			metadata := testutils.DefaultMetadata("TEXT SEARCH TEMPLATE", false, false, true, false)
			builtin.PrintCreateTextSearchTemplateStatement(backupfile, tocfile, template, metadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "testtemplate", "TEXT SEARCH TEMPLATE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TEXT SEARCH TEMPLATE public.testtemplate (
	INIT = dsimple_init,
	LEXIZE = dsimple_lexize
);`, `COMMENT ON TEXT SEARCH TEMPLATE public.testtemplate IS 'This is a text search template comment.';`)
		})
	})
	Describe("PrintCreateTextSearchDictionaryStatement", func() {
		It("prints a basic text search dictionary with comment", func() {
			dictionary := builtin.TextSearchDictionary{Oid: 1, Schema: "public", Name: "testdictionary", Template: "testschema.snowball", InitOption: "language = 'russian', stopwords = 'russian'"}
			metadata := testutils.DefaultMetadata("TEXT SEARCH DICTIONARY", false, true, true, false)
			builtin.PrintCreateTextSearchDictionaryStatement(backupfile, tocfile, dictionary, metadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "testdictionary", "TEXT SEARCH DICTIONARY")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TEXT SEARCH DICTIONARY public.testdictionary (
	TEMPLATE = testschema.snowball,
	language = 'russian', stopwords = 'russian'
);`, `COMMENT ON TEXT SEARCH DICTIONARY public.testdictionary IS 'This is a text search dictionary comment.';`, `ALTER TEXT SEARCH DICTIONARY public.testdictionary OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateTextSearchConfigurationStatement", func() {
		It("prints a basic text search configuration", func() {
			configurations := builtin.TextSearchConfiguration{Oid: 0, Schema: "public", Name: "testconfiguration", Parser: `pg_catalog."default"`, TokenToDicts: map[string][]string{}}
			builtin.PrintCreateTextSearchConfigurationStatement(backupfile, tocfile, configurations, builtin.ObjectMetadata{})
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TEXT SEARCH CONFIGURATION public.testconfiguration (
	PARSER = pg_catalog."default"
);`)
		})
		It("prints a text search configuration with multiple mappings and comment", func() {
			tokenToDicts := map[string][]string{"int": {"simple", "english_stem"}, "asciiword": {"english_stem"}}
			configurations := builtin.TextSearchConfiguration{Oid: 1, Schema: "public", Name: "testconfiguration", Parser: `pg_catalog."default"`, TokenToDicts: tokenToDicts}
			metadata := testutils.DefaultMetadata("TEXT SEARCH CONFIGURATION", false, true, true, false)
			builtin.PrintCreateTextSearchConfigurationStatement(backupfile, tocfile, configurations, metadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "testconfiguration", "TEXT SEARCH CONFIGURATION")
			expectedStatements := []string{`CREATE TEXT SEARCH CONFIGURATION public.testconfiguration (
	PARSER = pg_catalog."default"
);`, `ALTER TEXT SEARCH CONFIGURATION public.testconfiguration
	ADD MAPPING FOR "asciiword" WITH english_stem;`,
				`ALTER TEXT SEARCH CONFIGURATION public.testconfiguration
	ADD MAPPING FOR "int" WITH simple, english_stem;`,
				`COMMENT ON TEXT SEARCH CONFIGURATION public.testconfiguration IS 'This is a text search configuration comment.';`,
				`ALTER TEXT SEARCH CONFIGURATION public.testconfiguration OWNER TO testrole;`}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
	})
})
