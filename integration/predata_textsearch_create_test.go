package integration

import (
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
		testutils.SkipIfBefore5(connectionPool)
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintCreateTextSearchParserStatements", func() {
		parser := builtin.TextSearchParser{Oid: 0, Schema: "public", Name: "testparser", StartFunc: "prsd_start", TokenFunc: "prsd_nexttoken", EndFunc: "prsd_end", LexTypesFunc: "prsd_lextype", HeadlineFunc: "prsd_headline"}
		It("creates a basic text search parser", func() {
			builtin.PrintCreateTextSearchParserStatement(backupfile, tocfile, parser, builtin.ObjectMetadata{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH PARSER public.testparser")

			resultParsers := builtin.GetTextSearchParsers(connectionPool)

			Expect(resultParsers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&parser, &resultParsers[0], "Oid")
		})
		It("creates a basic text search parser with a comment", func() {
			parserMetadata := testutils.DefaultMetadata("TEXT SEARCH PARSER", false, false, true, false)

			builtin.PrintCreateTextSearchParserStatement(backupfile, tocfile, parser, parserMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH PARSER public.testparser")

			resultParsers := builtin.GetTextSearchParsers(connectionPool)
			resultMetadataMap := builtin.GetCommentsForObjectType(connectionPool, builtin.TYPE_TSPARSER)

			Expect(resultParsers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&parser, &resultParsers[0], "Oid")
			resultMetadata := resultMetadataMap[resultParsers[0].GetUniqueID()]
			structmatcher.ExpectStructsToMatch(&parserMetadata, &resultMetadata)
		})
	})
	Describe("PrintCreateTextSearchTemplateStatement", func() {
		template := builtin.TextSearchTemplate{Oid: 1, Schema: "public", Name: "testtemplate", InitFunc: "dsimple_init", LexizeFunc: "dsimple_lexize"}
		It("creates a basic text search template", func() {
			builtin.PrintCreateTextSearchTemplateStatement(backupfile, tocfile, template, builtin.ObjectMetadata{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH TEMPLATE public.testtemplate")

			resultTemplates := builtin.GetTextSearchTemplates(connectionPool)

			Expect(resultTemplates).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&template, &resultTemplates[0], "Oid")
		})
		It("creates a basic text search template with a comment", func() {
			templateMetadata := testutils.DefaultMetadata("TEXT SEARCH TEMPLATE", false, false, true, false)

			builtin.PrintCreateTextSearchTemplateStatement(backupfile, tocfile, template, templateMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH TEMPLATE public.testtemplate")

			resultTemplates := builtin.GetTextSearchTemplates(connectionPool)
			resultMetadataMap := builtin.GetCommentsForObjectType(connectionPool, builtin.TYPE_TSTEMPLATE)

			Expect(resultTemplates).To(HaveLen(1))
			uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testtemplate", builtin.TYPE_TSTEMPLATE)
			resultMetadata := resultMetadataMap[uniqueID]
			structmatcher.ExpectStructsToMatchExcluding(&template, &resultTemplates[0], "Oid")
			structmatcher.ExpectStructsToMatch(&templateMetadata, &resultMetadata)
		})
	})
	Describe("PrintCreateTextSearchDictionaryStatement", func() {
		dictionary := builtin.TextSearchDictionary{Oid: 1, Schema: "public", Name: "testdictionary", Template: "pg_catalog.snowball", InitOption: "language = 'russian', stopwords = 'russian'"}
		It("creates a basic text search dictionary", func() {

			builtin.PrintCreateTextSearchDictionaryStatement(backupfile, tocfile, dictionary, builtin.ObjectMetadata{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH DICTIONARY public.testdictionary")

			resultDictionaries := builtin.GetTextSearchDictionaries(connectionPool)

			Expect(resultDictionaries).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&dictionary, &resultDictionaries[0], "Oid")
		})
		It("creates a basic text search dictionary with a comment and owner", func() {
			dictionaryMetadata := testutils.DefaultMetadata("TEXT SEARCH DICTIONARY", false, true, true, false)

			builtin.PrintCreateTextSearchDictionaryStatement(backupfile, tocfile, dictionary, dictionaryMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH DICTIONARY public.testdictionary")

			resultDictionaries := builtin.GetTextSearchDictionaries(connectionPool)
			resultMetadataMap := builtin.GetMetadataForObjectType(connectionPool, builtin.TYPE_TSDICTIONARY)

			Expect(resultDictionaries).To(HaveLen(1))
			uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testdictionary", builtin.TYPE_TSDICTIONARY)
			resultMetadata := resultMetadataMap[uniqueID]
			structmatcher.ExpectStructsToMatchExcluding(&dictionary, &resultDictionaries[0], "Oid")
			structmatcher.ExpectStructsToMatch(&dictionaryMetadata, &resultMetadata)
		})
	})
	Describe("PrintCreateTextSearchConfigurationStatement", func() {
		configuration := builtin.TextSearchConfiguration{Oid: 1, Schema: "public", Name: "testconfiguration", Parser: `pg_catalog."default"`, TokenToDicts: map[string][]string{}}
		It("creates a basic text search configuration", func() {
			builtin.PrintCreateTextSearchConfigurationStatement(backupfile, tocfile, configuration, builtin.ObjectMetadata{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH CONFIGURATION public.testconfiguration")

			resultConfigurations := builtin.GetTextSearchConfigurations(connectionPool)

			Expect(resultConfigurations).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&configuration, &resultConfigurations[0], "Oid")
		})
		It("creates a basic text search configuration with a comment and owner", func() {
			configurationMetadata := testutils.DefaultMetadata("TEXT SEARCH CONFIGURATION", false, true, true, false)

			builtin.PrintCreateTextSearchConfigurationStatement(backupfile, tocfile, configuration, configurationMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH CONFIGURATION public.testconfiguration")

			resultConfigurations := builtin.GetTextSearchConfigurations(connectionPool)
			resultMetadataMap := builtin.GetMetadataForObjectType(connectionPool, builtin.TYPE_TSCONFIGURATION)

			Expect(resultConfigurations).To(HaveLen(1))
			uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testconfiguration", builtin.TYPE_TSCONFIGURATION)
			resultMetadata := resultMetadataMap[uniqueID]
			structmatcher.ExpectStructsToMatchExcluding(&configuration, &resultConfigurations[0], "Oid")
			structmatcher.ExpectStructsToMatch(&configurationMetadata, &resultMetadata)
		})
	})
})
