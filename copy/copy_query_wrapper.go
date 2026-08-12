package copy

import (
	"strconv"
	"strings"

	"github.com/cloudberry-contrib/cbcopy/internal/dbconn"
	"github.com/cloudberry-contrib/cbcopy/option"
	"github.com/cloudberry-contrib/cbcopy/utils"
	"github.com/apache/cloudberry-go-libs/gplog"
	"github.com/pkg/errors"
)

// QueryWrapper wraps QueryManager and provides additional data transformation functionality
type QueryWrapper struct {
	queryManager      *QueryManager
	srcPartLeafTable  []PartLeafTable
	destPartLeafTable []PartLeafTable
}

// NewQueryWrapper creates a new QueryWrapper instance
func NewQueryWrapper(qm *QueryManager) *QueryWrapper {
	return &QueryWrapper{
		queryManager: qm,
	}
}

// GetUserDatabases returns all user databases excluding system databases
func (qw *QueryWrapper) GetUserDatabases(conn *dbconn.DBConn) ([]string, error) {
	dbnames, err := qw.queryManager.GetAllDatabases(conn)
	if err != nil {
		return nil, err
	}

	results := make([]string, 0)
	for _, db := range dbnames {
		if !utils.Exists(excludedDb, db) {
			results = append(results, db)
		}
	}

	return results, nil
}

// GetRootPartTables returns a map of root partition table names
// example output:
// map[public.t1:true public.t2:true]
func (qw *QueryWrapper) GetRootPartTables(conn *dbconn.DBConn, isDest bool) (map[string]bool, error) {
	tables, err := qw.GetPartitionLeafTables(conn, isDest)
	if err != nil {
		return nil, err
	}

	nameMap := make(map[string]bool)
	for _, t := range tables {
		nameMap[t.RootName] = true
	}

	return nameMap, nil
}

// excludeTables removes excluded tables from the include tables list
func (qw *QueryWrapper) excludeTables(includeTables, excludeTables map[string]option.TableStatistics) []option.Table {
	results := make([]option.Table, 0)

	for k, v := range includeTables {
		_, exists := excludeTables[k]
		if !exists {
			sl := strings.Split(k, ".")
			results = append(results, option.Table{
				Schema:       sl[0],
				Name:         sl[1],
				Partition:    v.Partition,
				RelTuples:    v.RelTuples,
				IsReplicated: v.IsReplicated,
			})
		}
	}

	return results
}

// redirectSchemaTables redirects tables to their destination schemas based on schema mapping
func (qw *QueryWrapper) redirectSchemaTables(tables []option.Table) []option.Table {
	if len(config.GetDestSchemas()) == 0 {
		return tables
	}

	results := make([]option.Table, 0)
	schemaMap := config.GetSchemaMap()

	for _, v := range tables {
		ds, exists := schemaMap[v.Schema]
		if !exists {
			ds = v.Schema
		}

		results = append(results, option.Table{
			Schema:    ds,
			Name:      v.Name,
			Partition: v.Partition,
			RelTuples: v.RelTuples,
		})
	}

	return results
}

// redirectIncludeTables redirects tables to the first destination schema
func (qw *QueryWrapper) redirectIncludeTables(tables []option.Table) []option.Table {
	if len(config.GetDestSchemas()) == 0 {
		return tables
	}

	results := make([]option.Table, 0)
	ds := config.GetDestSchemas()[0].Schema

	for _, v := range tables {
		results = append(results, option.Table{
			Schema:       ds,
			Name:         v.Name,
			Partition:    v.Partition,
			RelTuples:    v.RelTuples,
			IsReplicated: v.IsReplicated,
		})
	}

	return results
}

// GetUserTables retrieves and processes user tables based on copy mode
func (qw *QueryWrapper) GetUserTables(srcConn, destConn *dbconn.DBConn) ([]option.Table, []option.Table, []option.Table, map[string][]string) {
	// Handle metadata-only mode
	if utils.MustGetFlagBool(option.GLOBAL_METADATA_ONLY) {
		sl := strings.Split(CbcopyTestTable, ".")
		inclTabs := make([]option.Table, 0)
		inclTabs = append(inclTabs, option.Table{Schema: sl[0], Name: sl[1]})
		partNameMap := make(map[string][]string)
		return inclTabs, inclTabs, make([]option.Table, 0), partNameMap
	}

	copyMode := config.GetCopyMode()

	// Get source tables
	gplog.Info("Retrieving user tables on source database \"%v\"...", srcConn.DBName)
	srcTables, err := qw.queryManager.GetUserTables(srcConn)
	gplog.FatalOnError(err)
	gplog.Info("Finished retrieving user tables")

	// Get source partition tables
	gplog.Info("Retrieving partition table on source database \"%v\"...", srcConn.DBName)
	srcDbPartTables, err := qw.GetRootPartTables(srcConn, false)
	gplog.FatalOnError(err)
	gplog.Info("Finished retrieving partition tables")

	// Mark excluded tables
	config.MarkExcludeTables(srcConn.DBName, srcTables, srcDbPartTables)

	// Handle other mode
	exlTabs, _, err := qw.expandPartTables(srcConn, srcTables, config.GetExclTablesByDb(srcConn.DBName))
	gplog.FatalOnError(err)
	if copyMode != option.CopyModeTable {
		srcTables = qw.filterTablesBySchema(srcConn, srcTables)
		results := qw.excludeTables(srcTables, exlTabs)
		// --skip-existing needs the destination inventory to drive its
		// filter (FilterTablesByDestExisting in meta/builtin). Without
		// this call, IsDestTableExisting always returns false in Full /
		// Db / Schema modes and the filter silently passes every table
		// through. See issue #34.
		if utils.MustGetFlagBool(option.SKIP_EXISTING) {
			qw.loadDestInventory(destConn)
		}
		return results, qw.redirectSchemaTables(results), nil, qw.getPartitionTableMapping(srcConn, destConn, false)
	}

	// Handle table mode
	config.MarkIncludeTables(srcConn.DBName, srcTables, srcDbPartTables)
	if len(config.GetDestTablesByDb(destConn.DBName)) == 0 {
		expandedTables, pendingCheckRels, err := qw.expandPartTables(srcConn, srcTables, config.GetIncludeTablesByDb(srcConn.DBName))
		gplog.FatalOnError(err)
		excludedSrcTables := qw.excludeTables(expandedTables, exlTabs)
		excludedPendingCheckRels := qw.excludeTables(pendingCheckRels, exlTabs)

		gplog.Info("Retrieving view, mat-view, sequence, foreigntable \"%v\" on source database...", srcConn.DBName)
		nonPhysicalRels := qw.GetNonPhysicalRelations(srcConn, excludedPendingCheckRels)
		gplog.Info("Finished retrieving view, mat-view, sequence, foreigntable")

		// Same wiring requirement as the Full/Db/Schema branch above:
		// without this call, --skip-existing is a silent no-op for
		// --include-table when --dest-table is not supplied. See
		// issue #34.
		if utils.MustGetFlagBool(option.SKIP_EXISTING) {
			qw.loadDestInventory(destConn)
		}
		return excludedSrcTables, qw.redirectIncludeTables(excludedSrcTables), nonPhysicalRels, qw.getPartitionTableMapping(srcConn, destConn, false)
	}

	return qw.processDestinationTables(srcConn, destConn, srcTables)
}

// FilterTablesBySchema filters tables to keep only those in the specified schemas
func (qw *QueryWrapper) filterTablesBySchema(conn *dbconn.DBConn, tables map[string]option.TableStatistics) map[string]option.TableStatistics {
	if config.GetCopyMode() != option.CopyModeSchema {
		return tables
	}

	schemaMap := make(map[string]bool)

	sourceSchemas := config.GetSourceSchemas()
	for _, schema := range sourceSchemas {
		exists, err := qw.queryManager.SchemaExists(conn, schema.Schema)
		if err != nil {
			gplog.Fatal(errors.Errorf("failed to check schema existence: %v", err), "")
		}
		if !exists {
			gplog.Fatal(errors.Errorf("Schema \"%v\" does not exists on source database \"%v\"", schema.Schema, conn.DBName), "")
		}

		schemaMap[schema.Schema] = true
	}

	for k, _ := range tables {
		sl := strings.Split(k, ".")

		_, exist := schemaMap[sl[0]]
		if !exist {
			delete(tables, k)
		}
	}
	return tables
}

// GetDbNameMap returns a map of database names based on copy mode
func (qw *QueryWrapper) GetDbNameMap(conn *dbconn.DBConn) map[string]string {
	dbMap := make(map[string]string)
	copyMode := config.GetCopyMode()

	sourceDbnames := make([]string, 0)
	destDbnames := make([]string, 0)

	if copyMode == option.CopyModeFull {
		var err error
		sourceDbnames, err = qw.GetUserDatabases(conn)
		gplog.FatalOnError(err)
		destDbnames = sourceDbnames
	} else if copyMode == option.CopyModeDb {
		sourceDbnames = config.GetSourceDbnames()
		destDbnames = sourceDbnames
		if len(config.GetDestDbnames()) > 0 {
			destDbnames = config.GetDestDbnames()
		}
	} else if copyMode == option.CopyModeSchema {
		ss := config.GetSourceSchemas()
		sourceDbnames = append(sourceDbnames, ss[0].Database)
		destDbnames = sourceDbnames

		if len(config.GetDestSchemas()) > 0 {
			destDbnames = make([]string, 0)
			destDbnames = append(destDbnames, config.GetDestSchemas()[0].Database)
		}
	} else {
		sourceDbnames = config.GetTblSourceDbnames()
		destDbnames = sourceDbnames

		if len(config.GetDestSchemas()) > 0 {
			destDbnames = make([]string, 0)
			destDbnames = append(destDbnames, config.GetDestSchemas()[0].Database)
		}
		if len(config.GetDestDbnames()) > 0 {
			destDbnames = config.GetDestDbnames()
		}
		if len(config.GetTblDestDbnames()) > 0 {
			destDbnames = config.GetTblDestDbnames()
		}
	}

	if len(sourceDbnames) != len(destDbnames) {
		gplog.Fatal(errors.Errorf("The number of source database should be equal to dest database"), "")
	}

	for i, dbname := range sourceDbnames {
		dbMap[dbname] = destDbnames[i]
	}

	return dbMap
}

// FormUserTableMap creates a map of user tables with their statistics
func (qw *QueryWrapper) FormUserTableMap(srcTables, destTables []option.Table) map[string]string {
	result := make(map[string]string)

	for i, t := range srcTables {
		isReplicatedStr := "false"
		if t.IsReplicated {
			isReplicatedStr = "true"
		}
		result[destTables[i].Schema+"."+destTables[i].Name] = t.Schema + "." + t.Name + "." + strconv.FormatInt(t.RelTuples, 10) + "." + isReplicatedStr
	}

	return result
}

// RedirectPartitionTables redirects partition table names based on schema mapping
func (qw *QueryWrapper) redirectPartitionTable(schemaMap map[string]string, schema string, tableName string, keepOriginal bool) string {
	if keepOriginal {
		return tableName
	}

	if schema != "" {
		sl := strings.Split(tableName, ".")
		return schema + "." + sl[1]
	}

	if schemaMap != nil {
		sl := strings.Split(tableName, ".")
		s, exists := schemaMap[sl[0]]
		if !exists {
			s = sl[0]
		}

		return s + "." + sl[1]
	}

	return tableName
}

func (qw *QueryWrapper) getPartitionTableMapping(srcConn, destConn *dbconn.DBConn, keepOriginal bool) map[string][]string {
	if config.GetCopyMode() == option.CopyModeTable && len(config.GetDestTables()) > 0 {
		return qw.buildPartitionTableMapping(destConn, true, keepOriginal)
	}

	return qw.buildPartitionTableMapping(srcConn, false, keepOriginal)
}

// buildPartitionTableMapping builds a mapping between root partition tables and their leaf tables
// example output:
// map[public.t1:[public.t1 public.t2] public.t2:[public.t3 public.t4]]
func (qw *QueryWrapper) buildPartitionTableMapping(conn *dbconn.DBConn, isDest bool, keepOriginal bool) map[string][]string {
	var schemaMap map[string]string
	if config.GetCopyMode() == option.CopyModeSchema && len(config.GetDestSchemas()) > 0 {
		schemaMap = config.GetSchemaMap()
	}

	schema := ""
	if config.GetCopyMode() == option.CopyModeTable && len(config.GetDestSchemas()) > 0 {
		schema = config.GetDestSchemas()[0].Schema
	}

	// Get leaf tables
	leafTables, err := qw.GetPartitionLeafTables(conn, isDest)
	gplog.FatalOnError(err)

	// Build partition map
	partMap := make(map[string][]string)
	for _, t := range leafTables {
		rootName := qw.redirectPartitionTable(schemaMap, schema, t.RootName, keepOriginal)

		children, exist := partMap[rootName]
		if !exist {
			children = make([]string, 0)
		}

		leafName := qw.redirectPartitionTable(schemaMap, schema, t.LeafName, keepOriginal)
		children = append(children, leafName)
		partMap[rootName] = children
	}

	return partMap
}

// expandPartTables expands partition tables and returns:
// - expanded table map
// - relations that need further checking (e.g., foreign tables, views)
// - error if any
func (qw *QueryWrapper) expandPartTables(conn *dbconn.DBConn, userTables map[string]option.TableStatistics,
	tables []option.Table) (map[string]option.TableStatistics, map[string]option.TableStatistics, error) {
	pendingCheckRels := make(map[string]option.TableStatistics)
	expandMap := make(map[string]option.TableStatistics)

	// Build table mapping
	tabMap := make(map[string][]option.Table)
	leafTables, err := qw.GetPartitionLeafTables(conn, false)
	if err != nil {
		return nil, nil, err
	}

	for _, t := range leafTables {
		sl := strings.Split(t.LeafName, ".")
		children, exists := tabMap[t.RootName]
		if !exists {
			children = make([]option.Table, 0)
		}
		children = append(children, option.Table{Schema: sl[0], Name: sl[1], Partition: 0, RelTuples: t.RelTuples})
		tabMap[t.RootName] = children
	}

	// Process tables
	for _, t := range tables {
		fqn := t.Schema + "." + t.Name
		if t.Partition == 1 {
			children, exists := tabMap[fqn]
			if exists {
				for _, m := range children {
					k := m.Schema + "." + m.Name
					expandMap[k] = option.TableStatistics{Partition: 0, RelTuples: m.RelTuples}
				}
			} else {
				pendingCheckRels[fqn] = option.TableStatistics{Partition: 0, RelTuples: t.RelTuples}
			}
			continue
		}

		stat, exists := userTables[fqn]
		if exists {
			expandMap[fqn] = option.TableStatistics{
				Partition:    0,
				RelTuples:    stat.RelTuples,
				IsReplicated: stat.IsReplicated,
			}
		} else {
			pendingCheckRels[fqn] = option.TableStatistics{Partition: 0, RelTuples: t.RelTuples}
		}
	}

	return expandMap, pendingCheckRels, nil
}

// ResetCache resets the partition leaf table cache
func (qw *QueryWrapper) ResetCache() {
	qw.srcPartLeafTable = nil
	qw.destPartLeafTable = nil
}

// GetPartitionLeafTables retrieves partition leaf tables with caching support
// example output:
// [{"RootName":"public.t1","LeafName":"public.t1","RelTuples":1000},{"RootName":"public.t2","LeafName":"public.t2","RelTuples":2000}]
func (qw *QueryWrapper) GetPartitionLeafTables(conn *dbconn.DBConn, isDest bool) ([]PartLeafTable, error) {
	// Check cache first
	if isDest && qw.destPartLeafTable != nil {
		return qw.destPartLeafTable, nil
	}
	if !isDest && qw.srcPartLeafTable != nil {
		return qw.srcPartLeafTable, nil
	}

	// Query database if cache is empty
	tables, err := qw.queryManager.GetPartitionLeafTables(conn)
	if err != nil {
		return nil, err
	}

	// Update cache
	if isDest {
		qw.destPartLeafTable = tables
	} else {
		qw.srcPartLeafTable = tables
	}

	return tables, nil
}

// leavesByRoot returns the partition leaf tables whose root equals rootFqn.
func leavesByRoot(leaves []PartLeafTable, rootFqn string) []PartLeafTable {
	results := make([]PartLeafTable, 0)
	for _, l := range leaves {
		if l.RootName == rootFqn {
			results = append(results, l)
		}
	}
	return results
}

// PairAction describes the outcome of a leaf-pairing attempt.
type PairAction string

const (
	PairFull         PairAction = "full"          // all src leaves matched — leaf-to-leaf
	PairPartial      PairAction = "partial"       // some matched, some unmatched — needs allowReshape
	PairRootFallback PairAction = "root_fallback" // zero matches + allowReshape — root→root
	PairFatal        PairAction = "fatal"         // partial or zero matches without allowReshape
)

// decidePairAction returns the action to take based on match counts and the
// --allow-partition-reshape flag.  This is a pure function with no side effects,
// making it straightforward to test.
func decidePairAction(matchedCount, unmatchedCount int, allowReshape bool) PairAction {
	if matchedCount == 0 {
		if allowReshape {
			return PairRootFallback
		}
		return PairFatal
	}
	if unmatchedCount > 0 {
		if allowReshape {
			return PairPartial
		}
		return PairFatal
	}
	return PairFull
}

// matchLeavesByBoundary is the pure, DB-free matching core.  It normalises
// boundary strings on both sides and pairs src leaves to dst leaves by equal
// normalised boundary.  Leaves with empty, duplicate, or unmatched boundaries
// are returned in unmatchedSrc.
func matchLeavesByBoundary(srcLeaves, destLeaves []PartLeafTable) (
	matchedSrc, matchedDst []option.Table, unmatchedSrc []PartLeafTable,
) {
	if len(srcLeaves) == 0 || len(destLeaves) == 0 {
		return nil, nil, nil
	}

	// Build destination lookup by normalised boundary.
	destByNorm := make(map[string]PartLeafTable, len(destLeaves))
	for _, d := range destLeaves {
		nb := normalizeBoundary(d.Boundary)
		if nb == "" {
			continue
		}
		if _, dup := destByNorm[nb]; dup {
			delete(destByNorm, nb)
			continue
		}
		destByNorm[nb] = d
	}

	// Count normalised src boundaries to detect duplicates.
	normCount := make(map[string]int, len(srcLeaves))
	for _, s := range srcLeaves {
		normCount[normalizeBoundary(s.Boundary)]++
	}

	leafSrc := make([]option.Table, 0, len(srcLeaves))
	leafDst := make([]option.Table, 0, len(srcLeaves))
	unmatched := make([]PartLeafTable, 0)

	for _, s := range srcLeaves {
		nb := normalizeBoundary(s.Boundary)
		if nb == "" || normCount[nb] > 1 {
			unmatched = append(unmatched, s)
			continue
		}

		d, found := destByNorm[nb]
		if !found {
			unmatched = append(unmatched, s)
			continue
		}

		ss := strings.Split(s.LeafName, ".")
		ds := strings.Split(d.LeafName, ".")
		if len(ss) != 2 || len(ds) != 2 {
			unmatched = append(unmatched, s)
			continue
		}

		leafSrc = append(leafSrc, option.Table{Schema: ss[0], Name: ss[1], Partition: 0, RelTuples: s.RelTuples})
		leafDst = append(leafDst, option.Table{Schema: ds[0], Name: ds[1], Partition: 0, RelTuples: s.RelTuples})
	}
	return leafSrc, leafDst, unmatched
}

// pairPartitionLeaves orchestrates leaf pairing: fetches leaves from the DB,
// calls matchLeavesByBoundary for the pure matching, then applies the
// decidePairAction gate to determine whether the copy should proceed, fall
// back, or abort.
func (qw *QueryWrapper) pairPartitionLeaves(srcConn, destConn *dbconn.DBConn,
	srcRoot option.Table, srcRootFqn, destRootFqn string, allowReshape bool,
) (matchedSrc, matchedDst []option.Table, unmatchedSrc []PartLeafTable, rootFallback bool) {

	srcLeavesAll, err := qw.GetPartitionLeafTables(srcConn, false)
	if err != nil {
		gplog.Fatal(errors.Errorf("leaf pairing for %q: cannot read source leaves: %v", srcRootFqn, err), "")
	}
	destLeavesAll, err := qw.GetPartitionLeafTables(destConn, true)
	if err != nil {
		gplog.Fatal(errors.Errorf("leaf pairing for %q: cannot read dest leaves: %v", destRootFqn, err), "")
	}

	srcLeaves := leavesByRoot(srcLeavesAll, srcRootFqn)
	destLeaves := leavesByRoot(destLeavesAll, destRootFqn)

	if len(srcLeaves) == 0 {
		gplog.Warn("leaf pairing for %q->%q: src has no partition leaves; falling back to root->root",
			srcRootFqn, destRootFqn)
		return nil, nil, nil, true
	}
	if len(destLeaves) == 0 {
		gplog.Warn("leaf pairing for %q->%q: destination has no partition leaves (plain table); "+
			"falling back to root->root (data will be flattened into a single table)",
			srcRootFqn, destRootFqn)
		return nil, nil, nil, true
	}

	leafSrc, leafDst, unmatched := matchLeavesByBoundary(srcLeaves, destLeaves)

	action := decidePairAction(len(leafSrc), len(unmatched), allowReshape)
	switch action {
	case PairFatal:
		if len(leafSrc) == 0 {
			gplog.Fatal(errors.Errorf("leaf pairing for %q->%q: no boundary matches found (boundary format may differ across versions). "+
				"Use --allow-partition-reshape to allow root->root fallback.", srcRootFqn, destRootFqn), "")
		} else {
			gplog.Fatal(errors.Errorf("leaf pairing for %q->%q: %d of %d src leaves could not be matched by boundary. "+
				"Use --allow-partition-reshape to allow partial leaf-to-leaf copy with unmatched leaves falling back to dst root.",
				srcRootFqn, destRootFqn, len(unmatched), len(srcLeaves)), "")
		}
	case PairRootFallback:
		gplog.Warn("leaf pairing for %q->%q: no boundary matches found; falling back to root->root ON SEGMENT (per --allow-partition-reshape)",
			srcRootFqn, destRootFqn)
		return nil, nil, nil, true
	case PairPartial:
		gplog.Warn("leaf pairing for %q->%q: %d of %d src leaves could not be matched by boundary; "+
			"unmatched leaves will be copied individually to dst root (per --allow-partition-reshape). "+
			"Ensure dst has a DEFAULT partition or that all unmatched values fall within existing dst ranges.",
			srcRootFqn, destRootFqn, len(unmatched), len(srcLeaves))
	case PairFull:
		gplog.Info("partition %q->%q: all %d leaves matched by boundary (leaf-to-leaf)", srcRootFqn, destRootFqn, len(leafSrc))
	}

	for i := range leafSrc {
		gplog.Debug("leaf pair by boundary: %q.%q -> %q.%q (reltuples=%v)",
			leafSrc[i].Schema, leafSrc[i].Name, leafDst[i].Schema, leafDst[i].Name, leafSrc[i].RelTuples)
	}

	return leafSrc, leafDst, unmatched, false
}

// excludeTablePair excludes table pairs based on source and destination tables
func (qw *QueryWrapper) excludeTablePair(srcConn, destConn *dbconn.DBConn,
	srcTables, destTables, exclTables []option.Table,
	userTables map[string]option.TableStatistics, dbname string) ([]option.Table, []option.Table) {

	if len(srcTables) != len(destTables) {
		gplog.Fatal(errors.Errorf("The number of include table should be equal to dest table"), "")
	}

	excludedSrcTabs := make([]option.Table, 0)
	excludedDstTabs := make([]option.Table, 0)
	tabMap := make(map[string]string)

	// Build source table info map for partition root lookup
	srcTabInfo := make(map[string]option.Table)
	for _, t := range srcTables {
		k := t.Schema + "." + t.Name
		srcTabInfo[k] = t
	}

	// Build table mapping
	for i, t := range srcTables {
		src := t.Schema + "." + t.Name
		dst := destTables[i].Schema + "." + destTables[i].Name
		tabMap[src] = dst
	}

	// Remove excluded tables
	for _, e := range exclTables {
		k := e.Schema + "." + e.Name
		delete(tabMap, k)
	}

	// Process remaining tables
	for k, v := range tabMap {
		u, exists := userTables[k]
		if !exists {
			// Check if it's a partition root table
			if srcTab, ok := srcTabInfo[k]; ok && srcTab.Partition == 1 {
				sls := strings.Split(k, ".")
				sld := strings.Split(v, ".")

				allowReshape := utils.MustGetFlagBool(option.ALLOW_PARTITION_RESHAPE)
				matchedSrc, matchedDst, unmatchedLeaves, rootFallback := qw.pairPartitionLeaves(srcConn, destConn, srcTab, k, v, allowReshape)

				if rootFallback {
					// Full root->root (plain dest, zero matches, or read error).
					gplog.Warn("mapping partition root %q to %q as root->root ON SEGMENT; per-leaf parallelism is lost", k, v)
					excludedSrcTabs = append(excludedSrcTabs, option.Table{
						Schema:         sls[0],
						Name:           sls[1],
						Partition:      1,
						RelTuples:      srcTab.RelTuples,
						ForceOnSegment: true,
					})
					excludedDstTabs = append(excludedDstTabs, option.Table{
						Schema:    sld[0],
						Name:      sld[1],
						Partition: 0,
						RelTuples: srcTab.RelTuples,
					})
					continue
				}

				// Add matched leaf pairs (leaf-to-leaf).
				excludedSrcTabs = append(excludedSrcTabs, matchedSrc...)
				excludedDstTabs = append(excludedDstTabs, matchedDst...)
				gplog.Info("mapping partition root %q to %q: %d leaf->leaf pair(s) by boundary", k, v, len(matchedSrc))

				// Add unmatched src leaves as individual copies to dst root.
				for _, ul := range unmatchedLeaves {
					uls := strings.Split(ul.LeafName, ".")
					if len(uls) != 2 {
						gplog.Warn("unexpected leaf name format %q; skipping unmatched leaf", ul.LeafName)
						continue
					}
					gplog.Warn("unmatched src leaf %q will be copied individually to dst root %q", ul.LeafName, v)
					excludedSrcTabs = append(excludedSrcTabs, option.Table{
						Schema:         uls[0],
						Name:           uls[1],
						Partition:      0,
						RelTuples:      ul.RelTuples,
						ForceOnSegment: true,
					})
					excludedDstTabs = append(excludedDstTabs, option.Table{
						Schema:    sld[0],
						Name:      sld[1],
						Partition: 0,
						RelTuples: ul.RelTuples,
					})
				}
				continue
			}

			gplog.Warn("Relation \"%v\" does not exists on source database \"%v\"", k, dbname)
			continue
		}

		sls := strings.Split(k, ".")
		sld := strings.Split(v, ".")

		excludedSrcTabs = append(excludedSrcTabs, option.Table{
			Schema:       sls[0],
			Name:         sls[1],
			Partition:    u.Partition,
			RelTuples:    u.RelTuples,
			IsReplicated: u.IsReplicated,
		})
		excludedDstTabs = append(excludedDstTabs, option.Table{
			Schema:       sld[0],
			Name:         sld[1],
			Partition:    u.Partition,
			RelTuples:    u.RelTuples,
			IsReplicated: u.IsReplicated,
		})

		gplog.Debug("mapping table from \"%v\" to \"%v\"", k, v)
	}

	return excludedSrcTabs, excludedDstTabs
}

func (qw *QueryWrapper) GetNonPhysicalRelations(conn *dbconn.DBConn, pendingCheckRels []option.Table) []option.Table {
	nonPhysicalRels, err := qw.queryManager.GetNonPhysicalRelations(conn)
	gplog.FatalOnError(err)

	results := make([]option.Table, 0)
	for _, t := range pendingCheckRels {
		k := t.Schema + "." + t.Name
		_, exists := nonPhysicalRels[k]
		if exists {
			results = append(results, t)
		} else {
			gplog.Debug("Relation \"%v\" does not exists in database \"%v\"", k, conn.DBName)
		}
	}

	return results
}

// loadDestInventory queries the destination cluster for its current user
// tables and root partition tables, then registers them with config via
// MarkDestTables so that IsDestTableExisting can answer queries later
// without another round-trip. Returns the destination user-table map so
// callers that also need to validate against it (processDestinationTables)
// avoid an extra query.
//
// This is the only writer of the destination-side inventory consumed by
// FilterTablesByDestExisting (DDL pipeline) and filterTablePairsByDestExisting
// (data channel). Any return path in GetUserTables that means to honour
// --skip-existing must call this helper; otherwise IsDestTableExisting will
// always return false on that path and the skip filters will silently pass
// every table through.
func (qw *QueryWrapper) loadDestInventory(destConn *dbconn.DBConn) map[string]option.TableStatistics {
	gplog.Info("Retrieving user tables on destination database \"%v\"...", destConn.DBName)
	destTables, err := qw.queryManager.GetUserTables(destConn)
	gplog.FatalOnError(err)
	gplog.Info("Finished retrieving user tables")

	gplog.Info("Retrieving partition table on destination database \"%v\"...", destConn.DBName)
	destDbPartTables, err := qw.GetRootPartTables(destConn, true)
	gplog.FatalOnError(err)
	gplog.Info("Finished retrieving partition table")

	config.MarkDestTables(destConn.DBName, destTables, destDbPartTables)
	return destTables
}

func (qw *QueryWrapper) processDestinationTables(srcConn, destConn *dbconn.DBConn, srcTables map[string]option.TableStatistics) ([]option.Table, []option.Table, []option.Table, map[string][]string) {
	destTables := qw.loadDestInventory(destConn)

	// Validate tables
	config.ValidateIncludeTables(srcTables, srcConn.DBName)
	config.ValidateExcludeTables(srcTables, srcConn.DBName)
	config.ValidateDestTables(destTables, destConn.DBName)

	excludedSrcTabs, excludedDstTabs := qw.excludeTablePair(
		srcConn, destConn,
		config.GetIncludeTablesByDb(srcConn.DBName),
		config.GetDestTablesByDb(destConn.DBName),
		config.GetExclTablesByDb(srcConn.DBName),
		srcTables,
		srcConn.DBName)

	return excludedSrcTabs, excludedDstTabs, nil, qw.getPartitionTableMapping(srcConn, destConn, false)
}
