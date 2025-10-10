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

// excludeTablePair excludes table pairs based on source and destination tables
func (qw *QueryWrapper) excludeTablePair(srcTables, destTables, exclTables []option.Table,
	userTables map[string]option.TableStatistics, dbname string) ([]option.Table, []option.Table) {

	if len(srcTables) != len(destTables) {
		gplog.Fatal(errors.Errorf("The number of include table should be equal to dest table"), "")
	}

	excludedSrcTabs := make([]option.Table, 0)
	excludedDstTabs := make([]option.Table, 0)
	tabMap := make(map[string]string)

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

func (qw *QueryWrapper) processDestinationTables(srcConn, destConn *dbconn.DBConn, srcTables map[string]option.TableStatistics) ([]option.Table, []option.Table, []option.Table, map[string][]string) {
	// Get destination tables
	gplog.Info("Retrieving user tables on destination database \"%v\"...", destConn.DBName)
	destTables, err := qw.queryManager.GetUserTables(destConn)
	gplog.FatalOnError(err)
	gplog.Info("Finished retrieving user tables")

	// Get destination partition tables
	gplog.Info("Retrieving partition table on destination database \"%v\"...", destConn.DBName)
	destDbPartTables, err := qw.GetRootPartTables(destConn, true)
	gplog.FatalOnError(err)
	gplog.Info("Finished retrieving partition table")

	config.MarkDestTables(destConn.DBName, destTables, destDbPartTables)

	// Validate tables
	config.ValidateIncludeTables(srcTables, srcConn.DBName)
	config.ValidateExcludeTables(srcTables, srcConn.DBName)
	config.ValidateDestTables(destTables, destConn.DBName)

	excludedSrcTabs, excludedDstTabs := qw.excludeTablePair(
		config.GetIncludeTablesByDb(srcConn.DBName),
		config.GetDestTablesByDb(destConn.DBName),
		config.GetExclTablesByDb(srcConn.DBName),
		srcTables,
		srcConn.DBName)

	return excludedSrcTabs, excludedDstTabs, nil, qw.getPartitionTableMapping(srcConn, destConn, false)
}
