package copy

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
)

/*
 * This file contains wrapper functions that group together functions relating
 * to querying and printing metadata, so that the logic for each object type
 * can all be in one place and copy.go can serve as a high-level look at the
 * overall copy flow.
 */

/*
 * Setup and validation wrapper functions
 */

func SetLoggerVerbosity() {
	if utils.MustGetFlagBool(option.QUIET) {
		gplog.SetVerbosity(gplog.LOGERROR)
	} else if utils.MustGetFlagBool(option.DEBUG) {
		gplog.SetVerbosity(gplog.LOGDEBUG)
	} else if utils.MustGetFlagBool(option.VERBOSE) {
		gplog.SetVerbosity(gplog.LOGVERBOSE)
	}
}

func GetVersion() string {
	return utils.Version
}

func initializeConnectionPool(dbname, username, host string, port, numConns int) *dbconn.DBConn {
	dbConn := dbconn.NewDBConn(dbname, username, host, port)
	dbConn.MustConnect(numConns)
	utils.ValidateGPDBVersionCompatibility(dbConn)
	for connNum := 0; connNum < dbConn.NumConns; connNum++ {
		SetSessionGUCs(dbConn, connNum)
	}

	return dbConn
}

func SetSessionGUCs(dbConn *dbconn.DBConn, connNum int) {
	// These GUCs ensure the dumps portability accross systems
	dbConn.MustExec(GetSetupQuery(dbConn), connNum)
}

func GetSetupQuery(dbConn *dbconn.DBConn) string {
	setupQuery := fmt.Sprintf(`
SET application_name TO '%v';
SET search_path TO pg_catalog;
SET statement_timeout = 0;
SET DATESTYLE = ISO;
SET standard_conforming_strings = 1;
SET enable_mergejoin TO off;
SET gp_autostats_mode = NONE;
SELECT set_config('extra_float_digits', (SELECT max_val FROM pg_settings WHERE name = 'extra_float_digits'), false);
`, applicationName)

	if dbConn.Version.AtLeast("5") {
		setupQuery += "SET synchronize_seqscans TO off;\n"
	}
	if dbConn.Version.AtLeast("6") {
		setupQuery += "SET INTERVALSTYLE = POSTGRES;\n"

		if dbConn.HdwVersion.AtLeast("3") {
			setupQuery += "SET hdw_disable_copy_transcoding to on;\n"
		}
	}

	return setupQuery
}

func GetUserDatabases(conn *dbconn.DBConn) []string {
	dbnames := GetAllDatabases(conn)

	results := make([]string, 0)
	for _, db := range dbnames {
		if !utils.Exists(excludedDb, db) {
			results = append(results, db)
		}
	}

	return results
}

func GetAllDatabases(conn *dbconn.DBConn) []string {
	results := make([]string, 0)

	query := fmt.Sprintf(`
	SELECT d.datname
	FROM pg_database d`)

	gplog.Debug("GetAllDatabases, query is %v", query)
	err := conn.Select(&results, query)
	gplog.FatalOnError(err, fmt.Sprintf("Query was: %s", query))

	return results
}

func getUserTables(conn *dbconn.DBConn) (map[string]option.TableStatistics, error) {
	var query string
	// todo: cast(c.reltuples as bigint) AS relTuples ==> not cast, change struct to use RelTuples float64 as gpbackup
	/*
	 * pg_partition_rule and pg_partition table has been removed from GP7
	 */
	if conn.Version.AtLeast("7") {
		query = `
		SELECT
			n.nspname AS schema, c.relname as name, 0 as partition, cast(c.reltuples as bigint) AS relTuples
		FROM
			pg_class c
			JOIN pg_namespace n ON (c.relnamespace=n.oid)
			JOIN pg_catalog.gp_distribution_policy p ON (c.oid = p.localoid)
		WHERE
		    c.oid NOT IN (select partrelid from pg_partitioned_table)
			AND n.nspname NOT IN ('gpexpand', 'pg_bitmapindex', 'information_schema', 'gp_toolkit')
			AND n.nspname NOT LIKE 'pg_temp_%'
			AND c.relkind <> 'm'
		ORDER BY c.relpages DESC
		`
	} else {
		query = `
		SELECT t.* FROM (
		SELECT
			n.nspname AS schema, c.relname as name, 0 as partition, cast(c.reltuples as bigint) AS relTuples
		FROM
			pg_class c
			JOIN pg_namespace n ON (c.relnamespace=n.oid)
			JOIN pg_catalog.gp_distribution_policy p ON (c.oid = p.localoid)
		WHERE
			c.oid NOT IN ( SELECT parchildrelid as oid FROM pg_partition_rule )
			AND c.oid NOT IN ( SELECT parrelid as oid FROM pg_partition )
			AND n.nspname NOT IN ('gpexpand', 'pg_bitmapindex', 'information_schema', 'gp_toolkit')
			AND n.nspname NOT LIKE 'pg_temp_%' AND c.relstorage NOT IN ('v', 'x', 'f')
			AND c.relkind <> 'm'
		ORDER BY c.relpages DESC ) t
		UNION ALL
		SELECT
			n.nspname AS schema, cparent.relname AS name, 0 as partition, cast(cparent.reltuples as bigint) AS relTuples
		FROM pg_partition p
			JOIN pg_partition_rule r ON p.oid = r.paroid
			JOIN pg_class cparent ON cparent.oid = r.parchildrelid
			JOIN pg_namespace n ON cparent.relnamespace=n.oid
			JOIN (SELECT parrelid AS relid, max(parlevel) AS pl
				FROM pg_partition GROUP BY parrelid) AS levels ON p.parrelid = levels.relid
		WHERE
			r.parchildrelid != 0
			AND p.parlevel = levels.pl
			AND n.nspname NOT IN ('gpexpand', 'pg_bitmapindex', 'information_schema', 'gp_toolkit')
			AND n.nspname NOT LIKE 'pg_temp_%' AND cparent.relstorage NOT IN ('v', 'x', 'f')`
	}

	gplog.Debug("getUserTables, query is %v", query)
	tables := make([]option.Table, 0)
	err := conn.Select(&tables, query)
	if err != nil {
		return nil, err
	}

	results := make(map[string]option.TableStatistics)
	for _, t := range tables {
		k := t.Schema + "." + t.Name
		results[k] = option.TableStatistics{Partition: t.Partition, RelTuples: t.RelTuples}
	}

	return results, nil
}

func getRootPartTables(conn *dbconn.DBConn, isDest bool) map[string]bool {
	tables := getPartLeafTables(conn, isDest)
	nameMap := make(map[string]bool)
	for _, t := range tables {
		nameMap[t.RootName] = true
	}

	return nameMap
}

func excludeTables(includeTables, excludeTables map[string]option.TableStatistics) []option.Table {
	results := make([]option.Table, 0)

	for k, v := range includeTables {
		_, exists := excludeTables[k]

		if !exists {
			sl := strings.Split(k, ".")
			results = append(results, option.Table{Schema: sl[0], Name: sl[1],
				Partition: v.Partition, RelTuples: v.RelTuples})
		}
	}

	return results
}

func redirectSchemaTables(tables []option.Table) []option.Table {
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

		results = append(results, option.Table{Schema: ds, Name: v.Name,
			Partition: v.Partition, RelTuples: v.RelTuples})
	}

	return results
}

func redirectIncludeTables(tables []option.Table) []option.Table {
	if len(config.GetDestSchemas()) == 0 {
		return tables
	}

	results := make([]option.Table, 0)
	ds := config.GetDestSchemas()[0].Schema

	for _, v := range tables {
		results = append(results, option.Table{Schema: ds, Name: v.Name,
			Partition: v.Partition, RelTuples: v.RelTuples})
	}

	return results
}

func GetUserTables(srcConn, destConn *dbconn.DBConn) ([]option.Table, []option.Table, map[string][]string) {
	if utils.MustGetFlagBool(option.GLOBAL_METADATA_ONLY) {
		sl := strings.Split(CbcopyTestTable, ".")

		inclTabs := make([]option.Table, 0)
		inclTabs = append(inclTabs, option.Table{Schema: sl[0], Name: sl[1]})

		partNameMap := make(map[string][]string)
		return inclTabs, inclTabs, partNameMap
	}

	copyMode := config.GetCopyMode()

	gplog.Info("Retrieving user tables on \"%v\" database of srcConn...", srcConn.DBName)
	srcTables, err := getUserTables(srcConn)
	gplog.FatalOnError(err)
	gplog.Info("Finished retrieving user tables")

	gplog.Info("Retrieving partition table map on \"%v\" database of srcConn...", srcConn.DBName)
	srcDbPartTables := getRootPartTables(srcConn, false)
	gplog.Info("Finished retrieving partition table map")

	config.MarkExcludeTables(srcConn.DBName, srcTables, srcDbPartTables)

	exlTabs, _ := ExpandPartTables(srcConn, srcTables, config.GetExclTablesByDb(srcConn.DBName), false)
	if copyMode != option.CopyModeTable {
		srcTables = handleSchemaTable(srcConn, srcTables)
		results := excludeTables(srcTables, exlTabs)
		return results, redirectSchemaTables(results), GetPartTableMap(srcConn, destConn, false)
	}

	config.MarkIncludeTables(srcConn.DBName, srcTables, srcDbPartTables)
	if destConn == nil || len(config.GetDestTablesByDb(destConn.DBName)) == 0 {
		inclTabs, _ := ExpandPartTables(srcConn, srcTables, config.GetIncludeTablesByDb(srcConn.DBName), false)
		results := excludeTables(inclTabs, exlTabs)

		return results, redirectIncludeTables(results), GetPartTableMap(srcConn, destConn, false)
	}

	gplog.Info("Retrieving user tables on \"%v\" database of destConn...", destConn.DBName)
	destTables, err := getUserTables(destConn)
	gplog.FatalOnError(err)
	gplog.Info("Finished retrieving user tables")

	gplog.Info("Retrieving partition table map on \"%v\" database of destConn...", destConn.DBName)
	destDbPartTables := getRootPartTables(destConn, true)
	gplog.Info("Finished retrieving partition table map")

	config.MarkDestTables(destConn.DBName, destTables, destDbPartTables)

	config.ValidateIncludeTables(srcTables, srcConn.DBName)
	config.ValidateExcludeTables(srcTables, srcConn.DBName)
	config.ValidateDestTables(destTables, destConn.DBName)

	excludedSrcTabs, excludedDstTabs := excludeTablePair(config.GetIncludeTablesByDb(srcConn.DBName),
		config.GetDestTablesByDb(destConn.DBName),
		config.GetExclTablesByDb(srcConn.DBName),
		srcTables,
		srcConn.DBName)

	return excludedSrcTabs, excludedDstTabs, GetPartTableMap(srcConn, destConn, false)
}

func handleSchemaTable(srcConn *dbconn.DBConn, tables map[string]option.TableStatistics) map[string]option.TableStatistics {
	if config.GetCopyMode() != option.CopyModeSchema {
		return tables
	}

	schemaMap := make(map[string]bool)

	sourceSchemas := config.GetSourceSchemas()
	for _, schema := range sourceSchemas {
		if !SchemaExists(srcConn, schema.Schema) {
			gplog.Fatal(errors.Errorf("Schema \"%v\" does not exists on the source database \"%v\"", schema.Schema, srcConn.DBName), "")
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

func GetDbNameMap() map[string]string {
	dbMap := make(map[string]string)
	copyMode := config.GetCopyMode()

	sourceDbnames := make([]string, 0)
	destDbnames := make([]string, 0)

	if copyMode == option.CopyModeFull {
		sourceDbnames = GetUserDatabases(srcManageConn)
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

func CreateHelperPortTable(conn *dbconn.DBConn, timestamp string) {
	query := fmt.Sprintf(`
		CREATE EXTERNAL WEB TABLE public.cbcopy_ports_temp_onmaster_%v (cmdID text, segID int, port int) EXECUTE 'cat /tmp/cbcopy-*.txt 2>/dev/null || true' ON MASTER FORMAT 'TEXT'`,
		timestamp)

	gplog.Debug("CreateHelperPortTable, query is %v", query)
	_, err := conn.Exec(query)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		gplog.FatalOnError(err, fmt.Sprintf("Query was: %s", query))
	}

	query = fmt.Sprintf(`
		CREATE EXTERNAL WEB TABLE public.cbcopy_ports_temp_onall_%v (cmdID text, segID int, port int) EXECUTE 'cat /tmp/cbcopy-*.txt 2>/dev/null || true' ON ALL FORMAT 'TEXT'`,
		timestamp)

	gplog.Debug("CreateHelperPortTable, query is %v", query)
	_, err = conn.Exec(query)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		gplog.FatalOnError(err, fmt.Sprintf("Query was: %s", query))
	}
}

type HelperPortInfo struct {
	Content int
	Port    int
}

func getHelperPortList(conn *dbconn.DBConn, timestamp string, cmdId string, workerId int, isMasterCopy bool) ([]HelperPortInfo, error) {
	target := "onall"
	if isMasterCopy {
		target = "onmaster"
	}

	query := fmt.Sprintf(`
	SELECT segID AS content, port AS port
	FROM public.cbcopy_ports_temp_%v_%v
	WHERE cmdID = '%v'`, target, timestamp, cmdId)

	hpis := make([]HelperPortInfo, 0)

	gplog.Debug("[Worker %v] Retrieving ports of helper programs: %v", workerId, query)

	gplog.Debug("[Worker %v] Executing query: %v", workerId, query)
	err := conn.Select(&hpis, query, workerId)
	gplog.Debug("[Worker %v] Finished executing query", workerId)
	if err != nil {
		return nil, err
	}

	if len(hpis) == 0 {
		/* needs retry */
		return hpis, nil
	}

	/* remove duplicated items */
	segMap := make(map[int]int)
	for _, segPort := range hpis {
		_, exist := segMap[segPort.Content]
		if !exist {
			segMap[segPort.Content] = segPort.Port
		}
	}

	/* sort map */
	keys := make([]int, 0)
	for k := range segMap {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	hpis = make([]HelperPortInfo, 0)
	for _, k := range keys {
		port := segMap[k]
		hpis = append(hpis, HelperPortInfo{Content: k, Port: port})
	}

	return hpis, nil
}

func CreateDbIfNotExist(conn *dbconn.DBConn, dbname string) {
	dbnames := GetAllDatabases(conn)
	if utils.Exists(dbnames, dbname) {
		return
	}

	query := fmt.Sprintf(`CREATE DATABASE "%v"`, dbname)

	gplog.Info("Database \"%v\" does not exist. Creating.", dbname)
	_, err := conn.Exec(query)
	gplog.FatalOnError(err)
}

func SchemaExists(conn *dbconn.DBConn, schema string) bool {
	var counts int64

	query := fmt.Sprintf(`
		SELECT
               count(*) AS count
       FROM
               pg_namespace
       WHERE nspname='%v'`, schema)

	err := conn.Get(&counts, query)
	gplog.FatalOnError(err)

	return counts == 1
}

func formUserTableMap(srcTables, destTables []option.Table) map[string]string {
	result := make(map[string]string)

	for i, t := range srcTables {
		result[destTables[i].Schema+"."+destTables[i].Name] = t.Schema + "." + t.Name + "." + strconv.FormatInt(t.RelTuples, 10)
	}

	return result
}

func redirectPartitionTables(schemaMap map[string]string, schema string, tableName string, force bool) string {
	if force {
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

func getPartTableMap(conn *dbconn.DBConn, isDest bool, force bool) map[string][]string {
	var schemaMap map[string]string
	if config.GetCopyMode() == option.CopyModeSchema && len(config.GetDestSchemas()) > 0 {
		schemaMap = config.GetSchemaMap()
	}

	schema := ""
	if config.GetCopyMode() == option.CopyModeTable && len(config.GetDestSchemas()) > 0 {
		schema = config.GetDestSchemas()[0].Schema
	}

	leafTables := getPartLeafTables(conn, isDest)

	partMap := make(map[string][]string)
	for _, t := range leafTables {
		rootName := redirectPartitionTables(schemaMap, schema, t.RootName, force)

		children, exist := partMap[rootName]
		if !exist {
			children = make([]string, 0)
		}

		leafName := redirectPartitionTables(schemaMap, schema, t.LeafName, force)
		children = append(children, leafName)
		partMap[rootName] = children
	}

	return partMap
}

type PartLeafTable struct {
	RootName  string
	LeafName  string
	RelTuples int64
}

func getPartLeafTables(conn *dbconn.DBConn, isDest bool) []PartLeafTable {
	leafTables := srcPartLeafTable
	if isDest {
		leafTables = destPartLeafTable
	}

	if leafTables != nil {
		return leafTables
	}

	// todo: cast(reltuples as bigint) AS relTuples ==> not cast, change struct to use RelTuples float64 as gpbackup
	/*
	 * pg_partition_rule and pg_partition table has been removed from GP7
	 */
	var query string
	if conn.Version.AtLeast("7") {
		query = `
		SELECT n.nspname || '.' || relname AS rootname,   
 	 	n.nspname || '.' || 
 	 	(SELECT relname FROM pg_class WHERE oid = inhrelid ) 
 	 	AS 	leafname, cast(reltuples as bigint) AS relTuples
		FROM pg_class c
		JOIN pg_inherits p
		  ON c.oid = p.inhparent
		      AND 
		  (SELECT relispartition
		  FROM pg_class pc
		  WHERE oid = inhrelid )
		JOIN pg_namespace n ON c.relnamespace=n.oid 
		  AND n.nspname NOT IN ('gpexpand', 'pg_bitmapindex', 'information_schema', 'gp_toolkit')
		  AND n.nspname NOT LIKE 'pg_temp_%%'
		ORDER BY n.nspname, leafname;`
	} else {
		query = `
		SELECT na.nspname || '.' || cb.relname AS rootname, pc.schema ||'.' || pc.name AS leafname, pc.relTuples
		FROM pg_namespace na
		JOIN pg_class cb ON na.oid = cb.relnamespace
		JOIN (SELECT
		           p.parrelid AS parentid, n.nspname AS schema, cparent.relname AS name, cast(cparent.reltuples as bigint) AS relTuples
		          FROM pg_partition p
		              JOIN pg_partition_rule r ON p.oid = r.paroid
		              JOIN pg_class cparent ON cparent.oid = r.parchildrelid
		              JOIN pg_namespace n ON cparent.relnamespace=n.oid
		              JOIN (SELECT parrelid AS relid, max(parlevel) AS pl
		                  FROM pg_partition GROUP BY parrelid) AS levels ON p.parrelid = levels.relid
		          WHERE
		              r.parchildrelid != 0
		              AND p.parlevel = levels.pl
		              AND n.nspname NOT IN ('gpexpand', 'pg_bitmapindex', 'information_schema', 'gp_toolkit')
		              AND n.nspname NOT LIKE 'pg_temp_%%' AND cparent.relstorage NOT IN ('v', 'x', 'f')) AS pc ON cb.oid = pc.parentid
		ORDER BY pc.schema, pc.name`
	}

	gplog.Debug("getPartLeafTables, query is %v", query)
	results := make([]PartLeafTable, 0)
	err := conn.Select(&results, query)
	gplog.FatalOnError(err)

	if isDest {
		destPartLeafTable = results
	} else {
		srcPartLeafTable = results
	}

	return results
}

func GetPartTableMap(srcConn, destConn *dbconn.DBConn, force bool) map[string][]string {
	if config.GetCopyMode() == option.CopyModeTable && len(config.GetDestTables()) > 0 {
		return getPartTableMap(destConn, true, force)
	}

	return getPartTableMap(srcConn, false, force)
}

func ExpandPartTables(conn *dbconn.DBConn, userTables map[string]option.TableStatistics, tables []option.Table, isDest bool) (map[string]option.TableStatistics, []option.Table) {
	skippedRels := make([]option.Table, 0)

	expandMap := make(map[string]option.TableStatistics)

	tabMap := make(map[string][]option.Table)
	leafTables := getPartLeafTables(conn, isDest)
	for _, t := range leafTables {
		sl := strings.Split(t.LeafName, ".")
		children, exists := tabMap[t.RootName]
		if !exists {
			children = make([]option.Table, 0)
		}
		children = append(children, option.Table{Schema: sl[0], Name: sl[1], Partition: 0, RelTuples: t.RelTuples})
		tabMap[t.RootName] = children
	}

	for _, t := range tables {
		fqn := t.Schema + "." + t.Name
		if t.Partition == 1 {
			children, exists := tabMap[fqn]
			if exists {
				for _, m := range children {
					k := m.Schema + "." + m.Name
					expandMap[k] = option.TableStatistics{Partition: 0, RelTuples: m.RelTuples}
				}
				continue
			}

			skippedRels = append(skippedRels, t)
			continue
		}

		stat, exists := userTables[fqn]
		if exists {
			expandMap[fqn] = option.TableStatistics{Partition: 0, RelTuples: stat.RelTuples}
			continue
		}

		skippedRels = append(skippedRels, t)
	}

	return expandMap, skippedRels
}

func ResetCache() {
	srcPartLeafTable = nil
	destPartLeafTable = nil
}

type SegmentTupleCount struct {
	SegId int
	Count int64
}

func isEmptyTable(conn *dbconn.DBConn, schema, table string, workerId int) (bool, error) {
	query := fmt.Sprintf(`
		select 1 from %v.%v limit 1`, schema, table)
	gplog.Debug("isEmptyTable, query is %v", query)

	results := make([]int64, 0)
	err := conn.Select(&results, query, workerId)
	if err != nil {
		return false, err
	}

	return len(results) == 0, nil
}

func CreateTestTable(conn *dbconn.DBConn, tabName string) {
	query := fmt.Sprintf(`
		CREATE TABLE %v (a int) with (appendonly=true)`, tabName)

	gplog.Debug("CreateTestTable, query is %v", query)
	_, err := conn.Exec(query)
	if err == nil {
		return
	}

	if !strings.Contains(err.Error(), "already exists") {
		gplog.FatalOnError(err)
	}
}

func excludeTablePair(srcTables, destTables, exclTables []option.Table, userTables map[string]option.TableStatistics, dbname string) ([]option.Table, []option.Table) {
	if len(srcTables) != len(destTables) {
		gplog.Fatal(errors.Errorf("The number of include table should be equal to dest table"), "")
	}

	excludedSrcTabs := make([]option.Table, 0)
	excludedDstTabs := make([]option.Table, 0)

	tabMap := make(map[string]string)

	for i, t := range srcTables {
		src := t.Schema + "." + t.Name
		dst := destTables[i].Schema + "." + destTables[i].Name
		tabMap[src] = dst
	}

	for _, e := range exclTables {
		k := e.Schema + "." + e.Name

		_, exists := tabMap[k]
		if exists {
			delete(tabMap, k)
		}
	}

	for k, v := range tabMap {
		u, exists := userTables[k]
		if !exists {
			gplog.Fatal(errors.Errorf("Relation \"%v\" does not exists on \"%v\" database", k, dbname), "")
		}

		sls := strings.Split(k, ".")
		sld := strings.Split(v, ".")

		excludedSrcTabs = append(excludedSrcTabs, option.Table{Schema: sls[0], Name: sls[1],
			Partition: u.Partition, RelTuples: u.RelTuples})
		excludedDstTabs = append(excludedDstTabs, option.Table{Schema: sld[0], Name: sld[1],
			Partition: u.Partition, RelTuples: u.RelTuples})

		gplog.Debug("mapping table from \"%v\" to \"%v\"", k, v)
	}

	return excludedSrcTabs, excludedDstTabs
}
