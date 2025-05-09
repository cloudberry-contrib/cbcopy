package integration

import (
	"database/sql"
	"strings"

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
	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintCreateIndexStatements", func() {
		var (
			indexMetadataMap builtin.MetadataMap
			index            builtin.IndexDefinition
		)
		BeforeEach(func() {
			indexMetadataMap = builtin.MetadataMap{}
			index = builtin.IndexDefinition{
				Oid:          0,
				Name:         "index1",
				OwningSchema: "public",
				OwningTable:  "testtable",
				Def:          sql.NullString{String: "CREATE INDEX index1 ON public.testtable USING btree (i)", Valid: true},
			}
		})
		It("creates a basic index", func() {
			indexes := []builtin.IndexDefinition{{Oid: 0, Name: "index1", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: "CREATE INDEX index1 ON public.testtable USING btree (i)", Valid: true}}}
			builtin.PrintCreateIndexStatements(backupfile, tocfile, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultIndexes := builtin.GetIndexes(connectionPool)
			Expect(resultIndexes).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
		})
		It("creates an index used for clustering", func() {
			indexes := []builtin.IndexDefinition{{Oid: 0, Name: "index1", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: "CREATE INDEX index1 ON public.testtable USING btree (i)", Valid: true}, IsClustered: true}}
			builtin.PrintCreateIndexStatements(backupfile, tocfile, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultIndexes := builtin.GetIndexes(connectionPool)
			Expect(resultIndexes).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
		})
		It("creates an index with a comment", func() {
			indexes := []builtin.IndexDefinition{{Oid: 1, Name: "index1", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: "CREATE INDEX index1 ON public.testtable USING btree (i)", Valid: true}}}
			indexMetadataMap = testutils.DefaultMetadataMap("INDEX", false, false, true, false)
			indexMetadata := indexMetadataMap[indexes[0].GetUniqueID()]
			builtin.PrintCreateIndexStatements(backupfile, tocfile, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultIndexes := builtin.GetIndexes(connectionPool)
			resultMetadataMap := builtin.GetCommentsForObjectType(connectionPool, builtin.TYPE_INDEX)
			resultMetadata := resultMetadataMap[resultIndexes[0].GetUniqueID()]
			Expect(resultIndexes).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
			structmatcher.ExpectStructsToMatch(&resultMetadata, &indexMetadata)
		})
		It("creates an index in a non-default tablespace", func() {
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("6") {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLESPACE test_tablespace FILESPACE test_dir")
			} else {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir'")
			}
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLESPACE test_tablespace")
			indexes := []builtin.IndexDefinition{{Oid: 0, Name: "index1", OwningSchema: "public", OwningTable: "testtable", Tablespace: "test_tablespace", Def: sql.NullString{String: "CREATE INDEX index1 ON public.testtable USING btree (i)", Valid: true}}}
			builtin.PrintCreateIndexStatements(backupfile, tocfile, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultIndexes := builtin.GetIndexes(connectionPool)
			Expect(resultIndexes).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
		})
		It("creates a unique index used as replica identity", func() {
			testutils.SkipIfBefore6(connectionPool)
			index.Def = sql.NullString{String: "CREATE UNIQUE INDEX index1 ON public.testtable USING btree (i)", Valid: true}
			index.IsReplicaIdentity = true
			indexes := []builtin.IndexDefinition{index}
			builtin.PrintCreateIndexStatements(backupfile, tocfile, indexes, indexMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int NOT NULL)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultIndexes := builtin.GetIndexes(connectionPool)
			Expect(resultIndexes).To(HaveLen(1))
			resultIndex := resultIndexes[0]
			structmatcher.ExpectStructsToMatchExcluding(&resultIndex, &index, "Oid")
		})
		It("creates an index with statistics on expression columns", func() {
			testutils.SkipIfBefore7(connectionPool)
			indexes := []builtin.IndexDefinition{{Oid: 0, Name: "testtable_index", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: "CREATE INDEX testtable_index ON public.testtable USING btree (i, ((i + 1)), ((j * 2)))", Valid: true}, StatisticsColumns: "2,3", StatisticsValues: "5000,600"}}
			builtin.PrintCreateIndexStatements(backupfile, tocfile, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int, j int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultIndexes := builtin.GetIndexes(connectionPool)
			Expect(resultIndexes).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
		})
		It("creates a parition index and attaches it to the parent index", func() {
			testutils.SkipIfBefore7(connectionPool)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foopart_new (a integer, b integer) PARTITION BY RANGE (b) DISTRIBUTED BY (a)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.foopart_new")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foopart_new_p1 (a integer, b integer) DISTRIBUTED BY (a)")
			testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.foopart_new ATTACH PARTITION public.foopart_new_p1 FOR VALUES FROM (0) TO (1)")
			testhelper.AssertQueryRuns(connectionPool, "CREATE INDEX fooidx ON ONLY public.foopart_new USING btree (b)")

			partitionIndex := builtin.IndexDefinition{Oid: 0, Name: "foopart_new_p1_b_idx", OwningSchema: "public", OwningTable: "foopart_new_p1", Def: sql.NullString{String: "CREATE INDEX foopart_new_p1_b_idx ON public.foopart_new_p1 USING btree (b)", Valid: true}, ParentIndexFQN: "public.fooidx"}

			indexes := []builtin.IndexDefinition{partitionIndex}
			builtin.PrintCreateIndexStatements(backupfile, tocfile, indexes, indexMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			partitionIndex.Oid = testutils.OidFromObjectName(connectionPool, "", "foopart_new_p1_b_idx", builtin.TYPE_INDEX)
			partitionIndex.ParentIndex = testutils.OidFromObjectName(connectionPool, "", "fooidx", builtin.TYPE_INDEX)

			resultIndexes := builtin.GetIndexes(connectionPool)
			Expect(resultIndexes).To(HaveLen(2))
			resultIndex := resultIndexes[1]

			structmatcher.ExpectStructsToMatch(&resultIndex, &partitionIndex)
		})
	})
	Describe("PrintCreateRuleStatements", func() {
		var (
			ruleMetadataMap builtin.MetadataMap
			ruleDef         string
		)
		BeforeEach(func() {
			ruleMetadataMap = builtin.MetadataMap{}
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("6") {
				ruleDef = "CREATE RULE update_notify AS ON UPDATE TO public.testtable DO NOTIFY testtable;"
			} else {
				ruleDef = "CREATE RULE update_notify AS\n    ON UPDATE TO public.testtable DO\n NOTIFY testtable;"
			}
		})
		It("creates a basic rule", func() {
			rules := []builtin.RuleDefinition{{Oid: 0, Name: "update_notify", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: ruleDef, Valid: true}}}
			builtin.PrintCreateRuleStatements(backupfile, tocfile, rules, ruleMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultRules := builtin.GetRules(connectionPool)
			Expect(resultRules).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultRules[0], &rules[0], "Oid")
		})
		It("creates a rule with a comment", func() {
			rules := []builtin.RuleDefinition{{Oid: 1, Name: "update_notify", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: ruleDef, Valid: true}}}
			ruleMetadataMap = testutils.DefaultMetadataMap("RULE", false, false, true, false)
			ruleMetadata := ruleMetadataMap[rules[0].GetUniqueID()]
			builtin.PrintCreateRuleStatements(backupfile, tocfile, rules, ruleMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			rules[0].Oid = testutils.OidFromObjectName(connectionPool, "", "update_notify", builtin.TYPE_RULE)
			resultRules := builtin.GetRules(connectionPool)
			resultMetadataMap := builtin.GetCommentsForObjectType(connectionPool, builtin.TYPE_RULE)
			resultMetadata := resultMetadataMap[resultRules[0].GetUniqueID()]
			Expect(resultRules).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultRules[0], &rules[0], "Oid")
			structmatcher.ExpectStructsToMatch(&resultMetadata, &ruleMetadata)
		})
	})
	Describe("PrintCreateTriggerStatements", func() {
		var (
			triggerMetadataMap builtin.MetadataMap
		)
		BeforeEach(func() {
			triggerMetadataMap = builtin.MetadataMap{}
		})
		It("creates a basic trigger", func() {
			triggerDef := `CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON public.testtable FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily() {
				triggerDef = `CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON public.testtable FOR EACH ROW EXECUTE FUNCTION "RI_FKey_check_ins"()`
			}
			triggers := []builtin.TriggerDefinition{{Oid: 0, Name: "sync_testtable", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: triggerDef, Valid: true}}}
			builtin.PrintCreateTriggerStatements(backupfile, tocfile, triggers, triggerMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultTriggers := builtin.GetTriggers(connectionPool)
			Expect(resultTriggers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultTriggers[0], &triggers[0], "Oid")
		})
		It("creates a trigger with a comment", func() {
			triggerDef := `CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON public.testtable FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily() {
				triggerDef = `CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON public.testtable FOR EACH ROW EXECUTE FUNCTION "RI_FKey_check_ins"()`
			}
			triggers := []builtin.TriggerDefinition{{Oid: 1, Name: "sync_testtable", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: triggerDef, Valid: true}}}
			triggerMetadataMap = testutils.DefaultMetadataMap("RULE", false, false, true, false)
			triggerMetadata := triggerMetadataMap[triggers[0].GetUniqueID()]
			builtin.PrintCreateTriggerStatements(backupfile, tocfile, triggers, triggerMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			triggers[0].Oid = testutils.OidFromObjectName(connectionPool, "", "sync_testtable", builtin.TYPE_TRIGGER)
			resultTriggers := builtin.GetTriggers(connectionPool)
			resultMetadataMap := builtin.GetCommentsForObjectType(connectionPool, builtin.TYPE_TRIGGER)
			resultMetadata := resultMetadataMap[resultTriggers[0].GetUniqueID()]
			Expect(resultTriggers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultTriggers[0], &triggers[0], "Oid")
			structmatcher.ExpectStructsToMatch(&resultMetadata, &triggerMetadata)
		})
	})
	Describe("PrintCreateEventTriggerStatements", func() {
		BeforeEach(func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION abort_any_command()
RETURNS event_trigger LANGUAGE plpgsql
AS $$ BEGIN RAISE EXCEPTION 'exception'; END; $$;`)
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, `DROP FUNCTION abort_any_command()`)
		})
		It("creates a basic event trigger", func() {
			eventTriggers := []builtin.EventTrigger{{Oid: 1, Name: "testeventtrigger1", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O"}}
			eventTriggerMetadataMap := builtin.MetadataMap{}
			builtin.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, eventTriggerMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EVENT TRIGGER testeventtrigger1")

			results := builtin.GetEventTriggers(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&eventTriggers[0], &results[0], "Oid")
		})
		It("creates an event trigger with multiple filter tags", func() {
			eventTriggers := []builtin.EventTrigger{{Oid: 1, Name: "testeventtrigger1", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O", EventTags: `'DROP FUNCTION', 'DROP TABLE'`}}
			eventTriggerMetadataMap := builtin.MetadataMap{}
			builtin.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, eventTriggerMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EVENT TRIGGER testeventtrigger1")

			results := builtin.GetEventTriggers(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&eventTriggers[0], &results[0], "Oid")
		})
		It("creates an event trigger with a single filter tag and enable option", func() {
			eventTriggers := []builtin.EventTrigger{{Oid: 1, Name: "testeventtrigger1", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "R", EventTags: `'DROP FUNCTION'`}}
			eventTriggerMetadataMap := builtin.MetadataMap{}
			builtin.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, eventTriggerMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EVENT TRIGGER testeventtrigger1")

			results := builtin.GetEventTriggers(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&eventTriggers[0], &results[0], "Oid")
		})
		It("creates an event trigger with comment, security label, and owner", func() {
			eventTriggers := []builtin.EventTrigger{{Oid: 1, Name: "test_event_trigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O"}}
			eventTriggerMetadataMap := testutils.DefaultMetadataMap("EVENT TRIGGER", false, true, true, includeSecurityLabels)
			eventTriggerMetadata := eventTriggerMetadataMap[eventTriggers[0].GetUniqueID()]

			builtin.PrintCreateEventTriggerStatements(backupfile, tocfile, []builtin.EventTrigger{eventTriggers[0]}, eventTriggerMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EVENT TRIGGER test_event_trigger")

			resultEventTriggers := builtin.GetEventTriggers(connectionPool)
			resultMetadataMap := builtin.GetMetadataForObjectType(connectionPool, builtin.TYPE_EVENTTRIGGER)

			Expect(resultEventTriggers).To(HaveLen(1))
			uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "test_event_trigger", builtin.TYPE_EVENTTRIGGER)
			resultMetadata := resultMetadataMap[uniqueID]
			structmatcher.ExpectStructsToMatchExcluding(&eventTriggers[0], &resultEventTriggers[0], "Oid")
			structmatcher.ExpectStructsToMatch(&eventTriggerMetadata, &resultMetadata)

		})
	})
	Describe("PrintCreateExtendedStatistics", func() {
		BeforeEach(func() {
			testutils.SkipIfBefore7(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE SCHEMA schema1;`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE schema1.table_for_ext_stats (m int, n int);`)
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, `DROP TABLE schema1.table_for_ext_stats;`)
			testhelper.AssertQueryRuns(connectionPool, `DROP SCHEMA schema1;`)
		})
		It("creates an extended statistics", func() {
			extStats := []builtin.StatisticExt{{Oid: 1, Name: "myextstatistics", Namespace: "public", Owner: "testrole", TableSchema: "schema1", TableName: "table_for_ext_stats", Definition: "CREATE STATISTICS public.myextstatistics (dependencies) ON m, n FROM schema1.table_for_ext_stats"}}

			statisticsMetadataMap := builtin.MetadataMap{}
			builtin.PrintCreateExtendedStatistics(backupfile, tocfile, extStats, statisticsMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP STATISTICS public.myextstatistics")

			results := builtin.GetExtendedStatistics(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&extStats[0], &results[0], "Oid")
		})
	})
	Describe("PrintExchangedPartitionIndexes", func() {
		BeforeEach(func() {
			if !(connectionPool.Version.IsGPDB() && connectionPool.Version.Is("6")) {
				Skip("Test only applicable to GPDB6")
			}
			testhelper.AssertQueryRuns(connectionPool, `CREATE SCHEMA schemaone;`)
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, `DROP SCHEMA schemaone CASCADE;`)
		})

		It("creates exchanged partition table indexes correctly", func() {
			testhelper.AssertQueryRuns(connectionPool, `
                    CREATE TABLE schemaone.pt_heap_tab(a INT, b TEXT, c INT , d INT, e NUMERIC, success BOOL) WITH (appendonly=false)
                    DISTRIBUTED BY (a)
                    PARTITION BY list(b)
                    (
                             PARTITION abc VALUES ('abc','abc1','abc2') WITH (appendonly=false),
                             PARTITION def VALUES ('def','def1','def3') WITH (appendonly=true, compresslevel=1), 
                             PARTITION ghi VALUES ('ghi','ghi1','ghi2') WITH (appendonly=true),
                             default partition dft
                    );

					CREATE INDEX heap_idx1 ON schemaone.pt_heap_tab(a) WHERE c > 10;
					ALTER TABLE schemaone.pt_heap_tab DROP default partition;

					CREATE TABLE schemaone.heap_can(LIKE schemaone.pt_heap_tab INCLUDING INDEXES);

					ALTER TABLE schemaone.pt_heap_tab ADD PARTITION pqr VALUES ('pqr','pqr1','pqr2') WITH (appendonly=true, orientation=column, compresslevel=5);
					ALTER TABLE schemaone.pt_heap_tab EXCHANGE PARTITION FOR ('pqr') WITH table schemaone.heap_can;`)

			indexes := builtin.GetIndexes(connectionPool)
			builtin.RenameExchangedPartitionIndexes(connectionPool, &indexes)
			indexesMetadataMap := builtin.MetadataMap{}
			builtin.PrintCreateIndexStatements(backupfile, tocfile, indexes, indexesMetadataMap)

			// Automatically-generated index names end in "_idx" in 6+ and "_key" in earlier versions.
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
				Expect(strings.Contains(buffer.String(), `CREATE INDEX heap_can_a_idx`)).To(BeTrue())
				Expect(strings.Contains(buffer.String(), `CREATE INDEX pt_heap_tab_1_prt_pqr_a_idx`)).To(BeFalse())
			} else {
				Expect(strings.Contains(buffer.String(), `CREATE INDEX heap_can_a_key`)).To(BeTrue())
				Expect(strings.Contains(buffer.String(), `CREATE INDEX pt_heap_tab_1_prt_pqr_a_key`)).To(BeFalse())
			}
		})
	})
})
