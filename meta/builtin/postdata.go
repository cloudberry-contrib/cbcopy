package builtin

/*
 * This file contains structs and functions related to backing up "post-data" metadata
 * on the master, which is any metadata that needs to be restored after data is
 * restored, such as indexes and rules.
 */

import (
	"strings"

	"github.com/cloudberrydb/cbcopy/meta/builtin/toc"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
)

// currently, this function (PrintCreateIndexStatements) has some diff as gpbackup, copy whole function from gpbackup first, to see if any issue in test
func PrintCreateIndexStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC, indexes []IndexDefinition, indexMetadata MetadataMap) {
	for _, index := range indexes {
		start := metadataFile.ByteCount
		if !index.SupportsConstraint {
			section, entry := index.GetMetadataEntry()

			metadataFile.MustPrintf("\n\n%s;", index.Def.String)
			toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)

			// Start INDEX metadata
			entry.ReferenceObject = index.FQN()
			entry.ObjectType = "INDEX METADATA"
			if index.Tablespace != "" {
				start := metadataFile.ByteCount
				metadataFile.MustPrintf("\nALTER INDEX %s SET TABLESPACE %s;", index.FQN(), index.Tablespace)
				toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
			}
			if index.ParentIndexFQN != "" && ((srcDBVersion.IsGPDB() && srcDBVersion.AtLeast("7")) || srcDBVersion.IsCBDBFamily()) {
				start := metadataFile.ByteCount
				metadataFile.MustPrintf("\nALTER INDEX %s ATTACH PARTITION %s;", index.ParentIndexFQN, index.FQN())
				toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
			}
			tableFQN := utils.MakeFQN(index.OwningSchema, index.OwningTable)
			if index.IsClustered {
				start := metadataFile.ByteCount
				metadataFile.MustPrintf("\nALTER TABLE %s CLUSTER ON %s;", tableFQN, index.Name)
				toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
			}
			if index.IsReplicaIdentity {
				start := metadataFile.ByteCount
				metadataFile.MustPrintf("\nALTER TABLE %s REPLICA IDENTITY USING INDEX %s;", tableFQN, index.Name)
				toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
			}
			if index.StatisticsColumns != "" && index.StatisticsValues != "" {
				cols := strings.Split(index.StatisticsColumns, ",")
				vals := strings.Split(index.StatisticsValues, ",")
				if len(cols) != len(vals) {
					gplog.Fatal(errors.Errorf("Index StatisticsColumns(%d) and StatisticsValues(%d) count don't match\n", len(cols), len(vals)), "")
				}
				for i := 0; i < len(cols); i++ {
					start := metadataFile.ByteCount
					metadataFile.MustPrintf("\nALTER INDEX %s ALTER COLUMN %s SET STATISTICS %s;", index.FQN(), cols[i], vals[i])
					toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
				}
			}
		}
		PrintObjectMetadata(metadataFile, toc, indexMetadata[index.GetUniqueID()], index, "")
	}
}

func PrintCreateRuleStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC, rules []RuleDefinition, ruleMetadata MetadataMap) {
	for _, rule := range rules {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\n%s", rule.Def.String)

		section, entry := rule.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		tableFQN := utils.MakeFQN(rule.OwningSchema, rule.OwningTable)
		PrintObjectMetadata(metadataFile, toc, ruleMetadata[rule.GetUniqueID()], rule, tableFQN)
	}
}

func PrintCreateTriggerStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC, triggers []TriggerDefinition, triggerMetadata MetadataMap) {
	for _, trigger := range triggers {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\n%s;", trigger.Def.String)

		section, entry := trigger.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		tableFQN := utils.MakeFQN(trigger.OwningSchema, trigger.OwningTable)
		PrintObjectMetadata(metadataFile, toc, triggerMetadata[trigger.GetUniqueID()], trigger, tableFQN)
	}
}

// https://github.com/greenplum-db/gpbackup/commit/4f79e0e68156852ba903b3f875c9d4a882feb7ba
// https://github.com/greenplum-db/gpbackup/commit/e25fc4f873f66897b62aa5c494fc0b77f3b0fefc
func PrintCreateEventTriggerStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC, eventTriggers []EventTrigger, eventTriggerMetadata MetadataMap) {
	for _, eventTrigger := range eventTriggers {
		start := metadataFile.ByteCount
		section, entry := eventTrigger.GetMetadataEntry()

		metadataFile.MustPrintf("\n\nCREATE EVENT TRIGGER %s\nON %s", eventTrigger.Name, eventTrigger.Event)
		if eventTrigger.EventTags != "" {
			metadataFile.MustPrintf("\nWHEN TAG IN (%s)", eventTrigger.EventTags)
		}
		if (srcDBVersion.IsGPDB() && srcDBVersion.AtLeast("7")) || srcDBVersion.IsCBDBFamily() {
			metadataFile.MustPrintf("\nEXECUTE FUNCTION %s();", eventTrigger.FunctionName)
		} else {
			metadataFile.MustPrintf("\nEXECUTE PROCEDURE %s();", eventTrigger.FunctionName)
		}
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)

		// Start EVENT TRIGGER metadata
		entry.ReferenceObject = eventTrigger.Name
		entry.ObjectType = "EVENT TRIGGER METADATA"
		if eventTrigger.Enabled != "O" {
			var enableOption string
			switch eventTrigger.Enabled {
			case "D":
				enableOption = "DISABLE"
			case "A":
				enableOption = "ENABLE ALWAYS"
			case "R":
				enableOption = "ENABLE REPLICA"
			default:
				enableOption = "ENABLE"
			}
			start := metadataFile.ByteCount
			metadataFile.MustPrintf("\nALTER EVENT TRIGGER %s %s;", eventTrigger.Name, enableOption)
			toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		}
		PrintObjectMetadata(metadataFile, toc, eventTriggerMetadata[eventTrigger.GetUniqueID()], eventTrigger, "")
	}
}

func PrintCreatePolicyStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC, policies []RLSPolicy, policyMetadata MetadataMap) {
	for _, policy := range policies {
		start := metadataFile.ByteCount
		section, entry := policy.GetMetadataEntry()

		tableFQN := utils.MakeFQN(policy.Schema, policy.Table)
		metadataFile.MustPrintf("\n\nALTER TABLE %s ENABLE ROW LEVEL SECURITY;", tableFQN)
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)

		permissiveOption := ""
		if policy.Permissive == "false" {
			permissiveOption = " AS RESTRICTIVE"
		}
		cmdOption := ""
		if policy.Cmd != "" {
			switch policy.Cmd {
			case "*":
				cmdOption = ""
			case "r":
				cmdOption = " FOR SELECT"
			case "a":
				cmdOption = " FOR INSERT"
			case "w":
				cmdOption = " FOR UPDATE"
			case "d":
				cmdOption = " FOR DELETE"
			default:
				gplog.Fatal(errors.Errorf("Unexpected policy command: expected '*|r|a|w|d' got '%s'\n", policy.Cmd), "")
			}
		}
		start = metadataFile.ByteCount
		metadataFile.MustPrintf("\nCREATE POLICY %s\nON %s%s%s", policy.Name, tableFQN, permissiveOption, cmdOption)

		if policy.Roles != "" {
			metadataFile.MustPrintf("\n TO %s", policy.Roles)
		}
		if policy.Qual != "" {
			metadataFile.MustPrintf("\n USING (%s)", policy.Qual)
		}
		if policy.WithCheck != "" {
			metadataFile.MustPrintf("\n WITH CHECK (%s)", policy.WithCheck)
		}
		metadataFile.MustPrintf(";\n")
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, toc, policyMetadata[policy.GetUniqueID()], policy, "")
	}
}

// https://github.com/greenplum-db/gpbackup/commit/7072d534d48ba32946c4112ad03f52fbef372c8c
// https://github.com/greenplum-db/gpbackup/commit/6e0407650ca6ba5f7db53e381c0bddfe997c394c
func PrintCreateExtendedStatistics(metadataFile *utils.FileWithByteCount, toc *toc.TOC, statExtObjects []StatisticExt, statMetadata MetadataMap) {
	for _, stat := range statExtObjects {
		start := metadataFile.ByteCount
		metadataFile.MustPrintln()

		metadataFile.MustPrintf("\n%s;", stat.Definition)
		section, entry := stat.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, toc, statMetadata[stat.GetUniqueID()], stat, "")
	}
}
