package copy

import (
	"fmt"
	"strings"

	"github.com/cloudberry-contrib/cbcopy/internal/dbconn"
	"github.com/cloudberry-contrib/cbcopy/option"
	"github.com/cloudberry-contrib/cbcopy/utils"
	"github.com/apache/cloudberry-go-libs/gplog"
)

// TableManager handles all table-related operations
type QueryManager struct {
}

func NewQueryManager() *QueryManager {
	return &QueryManager{}
}

// GetSessionSetupQuery returns the SQL query for setting up database session parameters
func (qm *QueryManager) GetSessionSetupQuery(conn *dbconn.DBConn, applicationName string) string {
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

	if (conn.Version.IsGPDB() && conn.Version.AtLeast("5")) || conn.Version.IsCBDBFamily() {
		setupQuery += "SET synchronize_seqscans TO off;\n"
	}
	if (conn.Version.IsGPDB() && conn.Version.AtLeast("6")) || conn.Version.IsCBDBFamily() {
		setupQuery += "SET INTERVALSTYLE = POSTGRES;\n"
	}

	if conn.Version.IsCBEDB() {
		setupQuery += "set hashdata.gp_create_syntax_compatible to on;\n"
	}

	return setupQuery
}

// GetAllDatabases retrieves all database names from the cluster
func (qm *QueryManager) GetAllDatabases(conn *dbconn.DBConn) ([]string, error) {
	results := make([]string, 0)

	query := `SELECT d.datname FROM pg_database d`

	gplog.Debug("GetAllDatabases, query is %v", query)
	err := conn.Select(&results, query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %s, error: %v", query, err)
	}

	return results, nil
}

// GetUserTables retrieves user tables from the database
// example output:
// map[public.t1:{"Partition":0,"RelTuples":1000,"IsReplicated":false} public.t2:{"Partition":0,"RelTuples":2000,"IsReplicated":true}]
func (tm *QueryManager) GetUserTables(conn *dbconn.DBConn) (map[string]option.TableStatistics, error) {
	var query string

	if (conn.Version.IsGPDB() && conn.Version.AtLeast("7")) || conn.Version.IsCBDBFamily() {
		query = `
		SELECT
			quote_ident(n.nspname) AS schema,
			quote_ident(c.relname) as name,
			0 as partition,
			cast(c.reltuples as bigint) AS relTuples,
			CASE WHEN p.policytype = 'r' THEN true ELSE false END as isReplicated
		FROM
			pg_class c
			JOIN pg_namespace n ON (c.relnamespace=n.oid)
			JOIN pg_catalog.gp_distribution_policy p ON (c.oid = p.localoid)
		WHERE
		    c.oid NOT IN (select partrelid from pg_partitioned_table)
			AND n.nspname NOT IN ('gpexpand', 'pg_bitmapindex', 'information_schema', 'gp_toolkit', 'pg_ext_aux')
			AND n.nspname NOT LIKE 'pg_temp_%'
			AND c.relkind <> 'm'
		ORDER BY c.relpages DESC
		`
	} else if conn.Version.IsGPDB() && conn.Version.AtLeast("6") {
		query = `
		SELECT t.* FROM (
		SELECT
			quote_ident(n.nspname) AS schema,
			quote_ident(c.relname) as name,
			0 as partition,
			cast(c.reltuples as bigint) AS relTuples,
			CASE WHEN p.policytype = 'r' THEN true ELSE false END as isReplicated
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
			quote_ident(n.nspname) AS schema,
			quote_ident(cparent.relname) AS name,
			0 as partition,
			cast(cparent.reltuples as bigint) AS relTuples,
			false as isReplicated
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
	} else {
		query = `
		SELECT t.* FROM (
		SELECT
			quote_ident(n.nspname) AS schema,
			quote_ident(c.relname) as name,
			0 as partition,
			cast(c.reltuples as bigint) AS relTuples,
			false as isReplicated
		FROM
			pg_class c
			JOIN pg_namespace n ON (c.relnamespace=n.oid)
		WHERE
			c.oid NOT IN ( SELECT parchildrelid as oid FROM pg_partition_rule )
			AND c.oid NOT IN ( SELECT parrelid as oid FROM pg_partition )
			AND n.nspname NOT IN ('gpexpand', 'pg_bitmapindex', 'information_schema', 'gp_toolkit')
			AND n.nspname NOT LIKE 'pg_temp_%' AND c.relstorage NOT IN ('v', 'x', 'f')
			AND c.relkind <> 'm'
		ORDER BY c.relpages DESC ) t
		UNION ALL
		SELECT
			quote_ident(n.nspname) AS schema,
			quote_ident(cparent.relname) AS name,
			0 as partition,
			cast(cparent.reltuples as bigint) AS relTuples,
			false as isReplicated
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
		results[k] = option.TableStatistics{Partition: t.Partition, RelTuples: t.RelTuples, IsReplicated: t.IsReplicated}
	}

	return results, nil
}

// IsEmptyTable checks if a table is empty
func (tm *QueryManager) IsEmptyTable(conn *dbconn.DBConn, schema, table string, workerId int) (bool, error) {
	query := fmt.Sprintf(`select 1 from %v.%v limit 1`, schema, table)
	gplog.Debug("[Worker %v] isEmptyTable, query is %v", workerId, query)

	results := make([]int64, 0)
	err := conn.Select(&results, query, workerId)
	if err != nil {
		return false, err
	}

	return len(results) == 0, nil
}

// CreateDatabaseIfNotExists creates a database if it doesn't exist
func (qm *QueryManager) CreateDatabaseIfNotExists(conn *dbconn.DBConn, dbname string) error {
	dbnames, err := qm.GetAllDatabases(conn)
	if err != nil {
		return err
	}

	if utils.Exists(dbnames, dbname) {
		return nil
	}

	query := fmt.Sprintf(`CREATE DATABASE "%v"`, dbname)
	gplog.Info("Database \"%v\" does not exist. Creating...", dbname)

	_, err = conn.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create database %s: %v", dbname, err)
	}

	return nil
}

// SchemaExists checks if a schema exists in the database
func (qm *QueryManager) SchemaExists(conn *dbconn.DBConn, schema string) (bool, error) {
	var count int64

	query := fmt.Sprintf(`
	SELECT count(*) AS count
	FROM pg_namespace
	WHERE nspname='%v'`, schema)

	err := conn.Get(&count, query)
	if err != nil {
		return false, err
	}

	return count == 1, nil
}

// PartLeafTable represents a partition leaf table
type PartLeafTable struct {
	RootName  string
	LeafName  string
	RelTuples int64
}

// GetPartitionLeafTables retrieves partition leaf tables from the database
// example output:
// [{"RootName":"public.t1","LeafName":"public.t1","RelTuples":1000},{"RootName":"public.t2","LeafName":"public.t2","RelTuples":2000}]
func (qm *QueryManager) GetPartitionLeafTables(conn *dbconn.DBConn) ([]PartLeafTable, error) {
	var query string

	if (conn.Version.IsGPDB() && conn.Version.AtLeast("7")) || conn.Version.IsCBDBFamily() {
		query = `
		SELECT quote_ident(n.nspname) || '.' || quote_ident(relname) AS rootname,   
 	 	quote_ident(n.nspname) || '.' || 
 	 	(SELECT quote_ident(relname) FROM pg_class WHERE oid = inhrelid ) 
 	 	AS 	leafname, cast(reltuples as bigint) AS relTuples
		FROM pg_class c
		JOIN pg_inherits p
		  ON c.oid = p.inhparent
		      AND 
		  (SELECT relispartition
		  FROM pg_class pc
		  WHERE oid = inhrelid )
		JOIN pg_namespace n ON c.relnamespace=n.oid 
		  AND n.nspname NOT IN ('gpexpand', 'pg_bitmapindex', 'information_schema', 'gp_toolkit', 'pg_ext_aux')
		  AND n.nspname NOT LIKE 'pg_temp_%%'
		ORDER BY n.nspname, leafname;`
	} else {
		query = `
		SELECT quote_ident(na.nspname) || '.' || quote_ident(cb.relname) AS rootname, quote_ident(pc.schema) ||'.' || quote_ident(pc.name) AS leafname, pc.relTuples
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

	gplog.Debug("GetPartitionLeafTables, query is %v", query)
	results := make([]PartLeafTable, 0)
	err := conn.Select(&results, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition leaf tables: %v", err)
	}

	return results, nil
}

// CreateTestTable creates an test table with a single integer column
func (qm *QueryManager) CreateTestTable(conn *dbconn.DBConn, tableName string) error {
	query := fmt.Sprintf(`CREATE TABLE %v (a int)`, tableName)

	gplog.Debug("CreateTestTable, query is %v", query)
	_, err := conn.Exec(query)
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to create test table: %v", err)
		}
	}

	return nil
}

// getNonPhysicalRelations returns a map of database relations that don't have direct physical storage
// (except materialized views), including:
// - Views (v)
// - Sequences (S)
// - Foreign Tables (f)
// - Materialized Views (m)
// The returned map uses "schema.name" as key and true as value for all entries.
func (qm *QueryManager) GetNonPhysicalRelations(conn *dbconn.DBConn) (map[string]bool, error) {
	query := `
	SELECT quote_ident(n.nspname) AS schema, quote_ident(c.relname) as name
	FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE n.nspname NOT IN ('gpexpand', 'pg_bitmapindex', 'information_schema', 'gp_toolkit', 'pg_catalog', 'pg_aoseg', 'pg_ext_aux')
		AND n.nspname NOT LIKE 'pg_temp_%'
		AND c.relkind IN ('f','S','m','v')`
	relations := make([]option.Table, 0)
	err := conn.Select(&relations, query)
	if err != nil {
		return nil, err
	}

	results := make(map[string]bool)

	for _, v := range relations {
		k := v.Schema + "." + v.Name
		results[k] = true
	}

	return results, nil
}
