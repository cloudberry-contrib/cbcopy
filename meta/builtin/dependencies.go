package builtin

import (
	"fmt"
	"strings"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/meta/builtin/toc"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
)

/* This file contains functions to sort objects that have dependencies among themselves.
 *  For example, functions and types can be dependent on one another, we cannot simply
 *  dump all functions and then all types.
 *  The following objects are included the dependency sorting logic:
 *   - Functions
 *   - Types
 *   - Tables
 *   - Protocols
 */
func AddProtocolDependenciesForGPDB4(depMap DependencyMap, tables []Table, protocols []ExternalProtocol) {
	protocolMap := make(map[string]UniqueID, len(protocols))
	for _, p := range protocols {
		protocolMap[p.Name] = p.GetUniqueID()
	}
	for _, table := range tables {
		extTableDef := table.ExtTableDef
		if extTableDef.Location.Valid && extTableDef.Location.String != "" {
			protocolName := extTableDef.Location.String[0:strings.Index(extTableDef.Location.String, "://")]
			if protocolEntry, ok := protocolMap[protocolName]; ok {
				tableEntry := table.GetUniqueID()
				if _, ok := depMap[tableEntry]; !ok {
					depMap[tableEntry] = make(map[UniqueID]bool)
				}
				depMap[tableEntry][protocolEntry] = true
			}
		}
	}
}

var (
	PG_AGGREGATE_OID            uint32 = 1255
	PG_AUTHID_OID               uint32 = 1260
	PG_CAST_OID                 uint32 = 2605
	PG_CLASS_OID                uint32 = 1259
	PG_COLLATION_OID            uint32 = 3456
	PG_CONSTRAINT_OID           uint32 = 2606
	PG_CONVERSION_OID           uint32 = 2607
	PG_DATABASE_OID             uint32 = 1262
	PG_EVENT_TRIGGER            uint32 = 3466
	PG_EXTENSION_OID            uint32 = 3079
	PG_EXTPROTOCOL_OID          uint32 = 7175
	PG_FOREIGN_DATA_WRAPPER_OID uint32 = 2328
	PG_FOREIGN_SERVER_OID       uint32 = 1417
	PG_INDEX_OID                uint32 = 2610
	PG_LANGUAGE_OID             uint32 = 2612
	PG_TRANSFORM_OID            uint32 = 3576 // https://github.com/greenplum-db/gpbackup/commit/be64bd87bd1c61e3b6ff78268748618ab44488ad
	PG_NAMESPACE_OID            uint32 = 2615
	PG_OPCLASS_OID              uint32 = 2616
	PG_OPERATOR_OID             uint32 = 2617
	PG_OPFAMILY_OID             uint32 = 2753
	PG_PROC_OID                 uint32 = 1255
	PG_RESGROUP_OID             uint32 = 6436
	PG_RESQUEUE_OID             uint32 = 6026
	PG_REWRITE_OID              uint32 = 2618
	PG_STATISTIC_EXT_OID        uint32 = 3381 // https://github.com/greenplum-db/gpbackup/commit/7072d534d48ba32946c4112ad03f52fbef372c8c
	PG_TABLESPACE_OID           uint32 = 1213
	PG_TRIGGER_OID              uint32 = 2620
	PG_TS_CONFIG_OID            uint32 = 3602
	PG_TS_DICT_OID              uint32 = 3600
	PG_TS_PARSER_OID            uint32 = 3601
	PG_TS_TEMPLATE_OID          uint32 = 3764
	PG_TYPE_OID                 uint32 = 1247
	PG_USER_MAPPING_OID         uint32 = 1418

	FIRST_NORMAL_OBJECT_ID uint32 = 16384 // https://github.com/greenplum-db/gpbackup/commit/f4e8e94fd99ee3861415b2c653af1141ba592634
)

/*
 * Structs and functions for topological sort
 */

type Sortable interface {
	FQN() string
	GetUniqueID() UniqueID
}

func TopologicalSort(slice []Sortable, dependencies DependencyMap) []Sortable {
	inDegrees := make(map[UniqueID]int)
	dependencyIndexes := make(map[UniqueID]int)
	isDependentOn := make(map[UniqueID][]UniqueID)
	queue := make([]Sortable, 0)
	sorted := make([]Sortable, 0)
	notVisited := make(map[UniqueID]bool)
	nameForUniqueID := make(map[UniqueID]string)
	for i, item := range slice {
		uniqueID := item.GetUniqueID()
		nameForUniqueID[uniqueID] = item.FQN()
		deps := dependencies[uniqueID]
		notVisited[uniqueID] = true
		inDegrees[uniqueID] = len(deps)
		for dep := range deps {
			isDependentOn[dep] = append(isDependentOn[dep], uniqueID)
		}
		dependencyIndexes[uniqueID] = i
		if len(deps) == 0 {
			queue = append(queue, item)
		}
	}
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		sorted = append(sorted, item)
		notVisited[item.GetUniqueID()] = false
		for _, dep := range isDependentOn[item.GetUniqueID()] {
			inDegrees[dep]--
			if inDegrees[dep] == 0 {
				queue = append(queue, slice[dependencyIndexes[dep]])
			}
		}
	}
	if len(slice) != len(sorted) {
		gplog.Verbose("Failed to sort dependencies.")
		gplog.Verbose("Not yet visited:")
		for _, item := range slice {
			if notVisited[item.GetUniqueID()] {
				gplog.Verbose("Object: %s %+v ", item.FQN(), item.GetUniqueID())
				gplog.Verbose("Dependencies: ")
				for uniqueID := range dependencies[item.GetUniqueID()] {
					gplog.Verbose("\t%s %+v", nameForUniqueID[uniqueID], uniqueID)
				}
			}
		}
		gplog.Fatal(errors.Errorf("Dependency resolution failed; see log file %s for details. This is a bug, please report.", gplog.GetLogFilePath()), "")
	}
	return sorted
}

type DependencyMap map[UniqueID]map[UniqueID]bool

type UniqueID struct {
	ClassID uint32
	Oid     uint32
}

// https://github.com/greenplum-db/gpbackup/commit/e00d2a1f6c027b2f634177d4b022fe3b70404619
type SortableDependency struct {
	ClassID    uint32
	ObjID      uint32
	RefClassID uint32
	RefObjID   uint32
}

// This function only returns dependencies that are referenced in the backup set
func GetDependencies(connectionPool *dbconn.DBConn, backupSet map[UniqueID]bool, tables []Table) DependencyMap {
	query := `
SELECT
	coalesce(id1.refclassid, d.classid) AS classid,
	coalesce(id1.refobjid, d.objid) AS objid,
	coalesce(id2.refclassid, d.refclassid) AS refclassid,
	coalesce(id2.refobjid, d.refobjid) AS refobjid
FROM pg_depend d
-- link implicit objects, using objid and refobjid, to the objects that created them
LEFT JOIN pg_depend id1 ON (d.objid = id1.objid and d.classid = id1.classid and id1.deptype='i')
LEFT JOIN pg_depend id2 ON (d.refobjid = id2.objid and d.refclassid = id2.classid and id2.deptype='i')
WHERE d.classid != 0
AND d.deptype != 'i'
UNION
-- this converts function dependencies on array types to the underlying type
-- this is needed because pg_depend in 4.3.x doesn't have the info we need
SELECT
	d.classid,
	d.objid,
	d.refclassid,
	t.typelem AS refobjid
FROM pg_depend d
JOIN pg_type t ON d.refobjid = t.oid
WHERE d.classid = 'pg_proc'::regclass::oid
AND typelem != 0`

	gplog.Debug("GetDependencies, query is %v", query)
	pgDependDeps := make([]SortableDependency, 0)
	err := connectionPool.Select(&pgDependDeps, query)
	gplog.FatalOnError(err)

	// In GP7 restoring a child table to a parent when the parent already has a
	// constraint applied will error.  Our solution is to add additional
	// "synthetic" dependencies to the backup, requiring all child tables to be
	// attached to the parent before the constraints are applied.
	if ((connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily()) && len(tables) > 0 {
		tableOids := make([]string, len(tables))
		for idx, table := range tables {
			tableOids[idx] = fmt.Sprintf("%d", table.Oid)
		}

		synthConstrDepQuery := fmt.Sprintf(`
			WITH constr_cte AS (
                SELECT
                    dep.refobjid,
                    con.conname,
                    con.connamespace
                FROM
                    pg_depend dep
                    INNER JOIN pg_constraint con ON dep.objid = con.oid
					INNER JOIN pg_class cls ON dep.refobjid = cls.oid
                WHERE
                    dep.refobjid IN (%s)
					AND cls.relkind in ('r','p', 'f')
					AND dep.deptype = 'n'
                )
              SELECT
                  'pg_constraint'::regclass::oid AS ClassID,
                  con.oid AS ObjID,
                  'pg_class'::regclass::oid AS RefClassID,
                  constr_cte.refobjid AS RefObjID
              FROM
                  pg_constraint con
                  INNER JOIN constr_cte
					ON con.conname = constr_cte.conname
					AND con.connamespace = constr_cte.connamespace
              WHERE
                  con.conislocal = true;`, strings.Join(tableOids, ", "))

		gplog.Debug("GetDependencies, synthConstrDepQuery is %v", synthConstrDepQuery)
		syntheticConstraintDeps := make([]SortableDependency, 0)
		err := connectionPool.Select(&syntheticConstraintDeps, synthConstrDepQuery)
		gplog.FatalOnError(err)

		if len(syntheticConstraintDeps) > 0 {
			pgDependDeps = append(pgDependDeps, syntheticConstraintDeps...)
		}
	}

	dependencyMap := make(DependencyMap)
	for _, dep := range pgDependDeps {
		object := UniqueID{
			ClassID: dep.ClassID,
			Oid:     dep.ObjID,
		}
		referenceObject := UniqueID{
			ClassID: dep.RefClassID,
			Oid:     dep.RefObjID,
		}

		_, objInBackup := backupSet[object]
		_, referenceInBackup := backupSet[referenceObject]

		if object == referenceObject || !objInBackup || !referenceInBackup {
			continue
		}

		if _, ok := dependencyMap[object]; !ok {
			dependencyMap[object] = make(map[UniqueID]bool)
		}

		dependencyMap[object][referenceObject] = true
	}

	breakCircularDependencies(dependencyMap)

	gplog.Debug("dependencyMap is %v", dependencyMap)
	return dependencyMap
}

func breakCircularDependencies(depMap DependencyMap) {
	for entry, deps := range depMap {
		for dep := range deps {
			if _, ok := depMap[dep]; ok && entry.ClassID == PG_TYPE_OID && dep.ClassID == PG_PROC_OID {
				if _, ok := depMap[dep][entry]; ok {
					if len(depMap[dep]) == 1 {
						delete(depMap, dep)
					} else {
						delete(depMap[dep], entry)
					}
				}
			}
		}
	}
}

// https://github.com/greenplum-db/gpbackup/commit/5895076a2678b00e50455e889d7df93d003fd3e8
func PrintDependentObjectStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC, objects []Sortable, metadataMap MetadataMap, domainConstraints []Constraint, funcInfoMap map[uint32]FunctionInfo) {
	domainConMap := make(map[string][]Constraint)
	for _, constraint := range domainConstraints {
		domainConMap[constraint.OwningObject] = append(domainConMap[constraint.OwningObject], constraint)
	}
	for _, object := range objects {
		objMetadata := metadataMap[object.GetUniqueID()]
		switch obj := object.(type) {
		case BaseType:
			PrintCreateBaseTypeStatement(metadataFile, toc, obj, objMetadata)
		case CompositeType:
			PrintCreateCompositeTypeStatement(metadataFile, toc, obj, objMetadata)
		case Domain:
			PrintCreateDomainStatement(metadataFile, toc, obj, objMetadata, domainConMap[obj.FQN()])
		case RangeType:
			PrintCreateRangeTypeStatement(metadataFile, toc, obj, objMetadata)
		case Function:
			PrintCreateFunctionStatement(metadataFile, toc, obj, objMetadata)
		case Table:
			PrintCreateTableStatement(metadataFile, toc, obj, objMetadata)
		case ExternalProtocol:
			PrintCreateExternalProtocolStatement(metadataFile, toc, obj, funcInfoMap, objMetadata)
		case View:
			PrintCreateViewStatement(metadataFile, toc, obj, objMetadata)
		case TextSearchParser:
			PrintCreateTextSearchParserStatement(metadataFile, toc, obj, objMetadata)
		case TextSearchConfiguration:
			PrintCreateTextSearchConfigurationStatement(metadataFile, toc, obj, objMetadata)
		case TextSearchTemplate:
			PrintCreateTextSearchTemplateStatement(metadataFile, toc, obj, objMetadata)
		case TextSearchDictionary:
			PrintCreateTextSearchDictionaryStatement(metadataFile, toc, obj, objMetadata)
		case Operator:
			PrintCreateOperatorStatement(metadataFile, toc, obj, objMetadata)
		case OperatorClass:
			PrintCreateOperatorClassStatement(metadataFile, toc, obj, objMetadata)
		case Aggregate:
			PrintCreateAggregateStatement(metadataFile, toc, obj, funcInfoMap, objMetadata)
		case Cast:
			PrintCreateCastStatement(metadataFile, toc, obj, objMetadata)
		case ForeignDataWrapper:
			PrintCreateForeignDataWrapperStatement(metadataFile, toc, obj, funcInfoMap, objMetadata)
		case ForeignServer:
			PrintCreateServerStatement(metadataFile, toc, obj, objMetadata)
		case UserMapping:
			PrintCreateUserMappingStatement(metadataFile, toc, obj)
		case Constraint:
			PrintConstraintStatement(metadataFile, toc, obj, objMetadata)
		case Transform:
			PrintCreateTransformStatement(metadataFile, toc, obj, funcInfoMap, objMetadata)
		}
		// Remove ACLs from metadataMap for the current object since they have been processed
		delete(metadataMap, object.GetUniqueID())
	}
	//  Process ACLs for left over objects in the metadata map
	printExtensionFunctionACLs(metadataFile, toc, metadataMap, funcInfoMap)
}
