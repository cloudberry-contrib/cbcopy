package builtin

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_relations.go.
 */

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/meta/builtin/toc"
	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

func relationAndSchemaFilterClause(connectionPool *dbconn.DBConn) string {
	if filterRelationClause != "" {
		return filterRelationClause
	}
	filterRelationClause = SchemaFilterClause("n")
	if len(excludeRelations) > 0 {
		// https://github.com/greenplum-db/gpbackup/commit/1122088597ac9fb08f38587f1d87671264bc77d6
		quotedExcludeRelations, err := option.QuoteTableNames(connectionPool, excludeRelations)
		gplog.FatalOnError(err)

		excludeOids := getOidsFromRelationList(connectionPool, quotedExcludeRelations)
		if len(excludeOids) > 0 {
			filterRelationClause += fmt.Sprintf("\nAND c.oid NOT IN (%s)", strings.Join(excludeOids, ", "))
		}
	}
	if len(includeRelations) > 0 {
		gplog.Debug("terry debug, relationAndSchemaFilterClause, includeRelations: %v", includeRelations)
		quotedIncludeRelations, err := option.QuoteTableNames(connectionPool, includeRelations)
		gplog.FatalOnError(err)

		includeOids := getOidsFromRelationList(connectionPool, quotedIncludeRelations)
		filterRelationClause += fmt.Sprintf("\nAND c.oid IN (%s)", strings.Join(includeOids, ", "))
	}
	return filterRelationClause
}

/*

for reference, gpbackup code
note: it does look option setting, this provides convenient way to let test change setting on the fly. correspondingly, let's add some helper function, e.g. SetRelationFilter(...).

func relationAndSchemaFilterClause() string {
	if filterRelationClause != "" {
		return filterRelationClause
	}
	filterRelationClause = SchemaFilterClause("n")
	if len(MustGetFlagStringArray(option.EXCLUDE_RELATION)) > 0 {
		quotedExcludeRelations, err := option.QuoteTableNames(connectionPool, MustGetFlagStringArray(option.EXCLUDE_RELATION))
		gplog.FatalOnError(err)

		excludeOids := getOidsFromRelationList(connectionPool, quotedExcludeRelations)
		if len(excludeOids) > 0 {
			filterRelationClause += fmt.Sprintf("\nAND c.oid NOT IN (%s)", strings.Join(excludeOids, ", "))
		}
	}
	if len(MustGetFlagStringArray(option.INCLUDE_RELATION)) > 0 {
		quotedIncludeRelations, err := option.QuoteTableNames(connectionPool, MustGetFlagStringArray(option.INCLUDE_RELATION))
		gplog.FatalOnError(err)

		includeOids := getOidsFromRelationList(connectionPool, quotedIncludeRelations)
		filterRelationClause += fmt.Sprintf("\nAND c.oid IN (%s)", strings.Join(includeOids, ", "))
	}
	return filterRelationClause
}
*/

func getOidsFromRelationList(connectionPool *dbconn.DBConn, quotedIncludeRelations []string) []string {
	relList := utils.SliceToQuotedString(quotedIncludeRelations)
	query := fmt.Sprintf(`
	SELECT c.oid AS string
	FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)`, relList)
	return dbconn.MustSelectStringSlice(connectionPool, query)
}

func GetIncludedUserTableRelations(connectionPool *dbconn.DBConn, includedRelationsQuoted []string) []Relation {
	if len(includeRelations) > 0 {
		return getUserTableRelationsWithIncludeFiltering(connectionPool, includedRelationsQuoted)
	}
	return getUserTableRelations(connectionPool)
}

type Relation struct {
	SchemaOid uint32
	Oid       uint32
	Schema    string
	Name      string
}

func (r Relation) FQN() string {
	return utils.MakeFQN(r.Schema, r.Name)
}

func (r Relation) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_CLASS_OID, Oid: r.Oid}
}

/*
 * This function also handles exclude table filtering since the way we do
 * it is currently much simpler than the include case.
 */
func getUserTableRelations(connectionPool *dbconn.DBConn) []Relation {
	//Filter out non-external child partitions
	childPartitionFilter := ""

	/*
		 * note: gpbackup has below code, but we don't need it
		 *
		if !MustGetFlagBool(option.LEAF_PARTITION_DATA) && connectionPool.Version.Before("7") {
			// Filter out non-external child partitions in GPDB6 and earlier.
			// In GPDB7+ we do not want to exclude child partitions, they function as separate tables.
			childPartitionFilter = `
		AND c.oid NOT IN (
			SELECT p.parchildrelid
			FROM pg_partition_rule p
				LEFT JOIN pg_exttable e ON p.parchildrelid = e.reloid
			WHERE e.reloid IS NULL)`
		}
	*/

	// GP7 support change ref: https://github.com/greenplum-db/gpbackup/commit/14885d8242f785468496de3310d154e676f686e4
	// In GPDB 7+, root partitions are marked as relkind 'p'.
	relkindFilter := `'r'`
	if connectionPool.Version.AtLeast("7") {
		relkindFilter = `'r', 'p'`
	}

	query := fmt.Sprintf(`
	SELECT n.oid AS schemaoid,
		c.oid AS oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(c.relname) AS name
	FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE %s
		%s
		AND relkind IN (%s)
		AND %s
		ORDER BY c.oid`,
		relationAndSchemaFilterClause(connectionPool), childPartitionFilter, relkindFilter, ExtensionFilterClause("c"))

	gplog.Debug("getUserTableRelations， query is %v", query)
	results := make([]Relation, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	return results
}

// note gpbackup has two getUserTableRelationsWithIncludeFiltering (this one and another one in options/option.go)
// but we just have one. The one in options/option.go has gone some change for GP7 support (https://github.com/greenplum-db/gpbackup/commit/e00d2a1f6c027b2f634177d4b022fe3b70404619)
// not sure if there's any issue.

func getUserTableRelationsWithIncludeFiltering(connectionPool *dbconn.DBConn, includedRelationsQuoted []string) []Relation {
	// https://github.com/greenplum-db/gpbackup/commit/14885d8242f785468496de3310d154e676f686e4
	// In GPDB 7+, root partitions are marked as relkind 'p'.
	relkindFilter := `'r'`
	if connectionPool.Version.AtLeast("7") {
		relkindFilter = `'r', 'p'`
	}

	includeOids := getOidsFromRelationList(connectionPool, includedRelationsQuoted)
	oidStr := strings.Join(includeOids, ", ")
	query := fmt.Sprintf(`
	SELECT n.oid AS schemaoid,
		c.oid AS oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(c.relname) AS name
	FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE c.oid IN (%s)
		AND relkind IN (%s)
	ORDER BY c.oid`, oidStr, relkindFilter)

	gplog.Debug("getUserTableRelationsWithIncludeFiltering， query is %v", query)
	results := make([]Relation, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	return results
}

func GetForeignTableRelations(connectionPool *dbconn.DBConn) []Relation {
	query := fmt.Sprintf(`
	SELECT n.oid AS schemaoid,
		c.oid AS oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(c.relname) AS name
	FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE %s
		AND relkind = 'f'
		AND %s
	ORDER BY c.oid`,
		relationAndSchemaFilterClause(connectionPool), ExtensionFilterClause("c"))

	gplog.Debug("GetForeignTableRelations， query is %v", query)
	results := make([]Relation, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	return results
}

type Sequence struct {
	Relation
	// https://github.com/greenplum-db/gpbackup/commit/0c421c6238d51ce5b75f555f5fbf2413bb53f0c0
	OwningTableOid    string
	OwningTableSchema string
	OwningTable       string
	OwningColumn      string
	// https://github.com/greenplum-db/gpbackup/commit/5519469f02c3b785628e163c9096e3cbcbd10a0d
	UnqualifiedOwningColumn string
	// https://github.com/greenplum-db/gpbackup/commit/8768bebfec46a01c2862738de9c91ee7adbf47b0
	OwningColumnAttIdentity string
	// https://github.com/greenplum-db/gpbackup/commit/fd7e0115e13c2fe96bdd1df66ba9d91772c3a3fa
	IsIdentity bool
	Definition SequenceDefinition
}

func (s Sequence) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          s.Schema,
			Name:            s.Name,
			ObjectType:      "SEQUENCE",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
			//Oid:             0,
			//ClassOID:        0,
		}
}

type SequenceDefinition struct {
	LastVal   int64
	Type      string // https://github.com/greenplum-db/gpbackup/commit/eb6eaaa4e689b9d6a9554e98e7fe19c9dd820e78
	StartVal  int64
	Increment int64
	MaxVal    int64
	MinVal    int64
	CacheVal  int64
	// LogCnt      int64 // gpbackup not have it
	IsCycled    bool
	IsCalled    bool
	OwningTable string
}

// https://github.com/greenplum-db/gpbackup/commit/3e048ce1e4e4a22dedca5f5dd455704d7e82ecf3
func GetAllSequences(connectionPool *dbconn.DBConn) []Sequence {
	atLeast7Query := fmt.Sprintf(`
		SELECT n.oid AS schemaoid,
			c.oid AS oid,
			quote_ident(n.nspname) AS schema,
			quote_ident(c.relname) AS name,
			coalesce(d.refobjid::text, '') AS owningtableoid,
			coalesce(quote_ident(m.nspname), '') AS owningtableschema,
			coalesce(quote_ident(t.relname), '') AS owningtable,
			coalesce(quote_ident(a.attname), '') AS owningcolumn,
			coalesce(a.attidentity, '') AS owningcolumnattidentity,
			coalesce(quote_ident(a.attname), '') AS unqualifiedowningcolumn,
			CASE
				WHEN d.deptype IS NULL THEN false
				ELSE d.deptype = 'i'
			END AS isidentity
		FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			LEFT JOIN pg_depend d ON c.oid = d.objid AND d.deptype in ('a', 'i')
			LEFT JOIN pg_class t ON t.oid = d.refobjid
			LEFT JOIN pg_namespace m ON m.oid = t.relnamespace
			LEFT JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
		WHERE c.relkind = 'S'
			AND %s
			AND %s
		ORDER BY n.nspname, c.relname`,
		relationAndSchemaFilterClause(connectionPool), ExtensionFilterClause("c"))

	before7Query := fmt.Sprintf(`
		SELECT n.oid AS schemaoid,
			c.oid AS oid,
			quote_ident(n.nspname) AS schema,
			quote_ident(c.relname) AS name,
			coalesce(d.refobjid::text, '') AS owningtableoid,
			coalesce(quote_ident(m.nspname), '') AS owningtableschema,
			coalesce(quote_ident(t.relname), '') AS owningtable,
			coalesce(quote_ident(a.attname), '') AS owningcolumn
		FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			LEFT JOIN pg_depend d ON c.oid = d.objid AND d.deptype = 'a'
			LEFT JOIN pg_class t ON t.oid = d.refobjid
			LEFT JOIN pg_namespace m ON m.oid = t.relnamespace
			LEFT JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
		WHERE c.relkind = 'S'
			AND %s
			AND %s
		ORDER BY n.nspname, c.relname`,
		relationAndSchemaFilterClause(connectionPool), ExtensionFilterClause("c"))

	query := ""
	if connectionPool.Version.Before("7") {
		query = before7Query
	} else {
		query = atLeast7Query
	}

	gplog.Debug("GetAllSequences，query is %v", query)
	results := make([]Sequence, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	// Exclude owning table and owning column info for sequences
	// where owning table is excluded from backup
	excludeOids := make([]string, 0)
	if len(utils.MustGetFlagStringSlice(option.EXCLUDE_TABLE)) > 0 {
		excludeOids = getOidsFromRelationList(connectionPool,
			utils.MustGetFlagStringSlice(option.EXCLUDE_TABLE))
	}
	for i := range results {
		found := utils.Exists(excludeOids, results[i].OwningTableOid)
		if results[i].OwningTable != "" {
			results[i].OwningTable = fmt.Sprintf("%s.%s",
				results[i].OwningTableSchema, results[i].OwningTable)
		}
		if results[i].OwningColumn != "" {
			results[i].OwningColumn = fmt.Sprintf("%s.%s",
				results[i].OwningTable, results[i].OwningColumn)
		}
		if found {
			results[i].OwningTable = ""
			results[i].OwningColumn = ""
		}
		results[i].Definition = GetSequenceDefinition(connectionPool, results[i].FQN())
	}
	return results
}

// not used any more, https://github.com/greenplum-db/gpbackup/commit/0c421c6238d51ce5b75f555f5fbf2413bb53f0c0
func GetAllSequenceRelations(connectionPool *dbconn.DBConn) []Relation {
	query := fmt.Sprintf(`
	SELECT n.oid AS schemaoid,
		c.oid AS oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(c.relname) AS name
	FROM pg_class c
		LEFT JOIN pg_namespace n
		ON c.relnamespace = n.oid
	WHERE relkind = 'S'
		AND %s
		AND %s
	ORDER BY n.nspname, c.relname`,
		relationAndSchemaFilterClause(connectionPool), ExtensionFilterClause("c"))

	gplog.Debug("GetAllSequenceRelations，query is %v", query)
	results := make([]Relation, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	return results
}

func GetSequenceDefinition(connectionPool *dbconn.DBConn, seqName string) SequenceDefinition {
	startValQuery := ""
	if connectionPool.Version.AtLeast("6") {
		startValQuery = "start_value AS startval,"
	}

	before7Query := fmt.Sprintf(`
		SELECT last_value AS lastval,
			%s
			increment_by AS increment,
			max_value AS maxval,
			min_value AS minval,
			cache_value AS cacheval,
			is_cycled AS iscycled,
			is_called AS iscalled
		FROM %s`, startValQuery, seqName)

	atLeast7Query := fmt.Sprintf(`
		SELECT s.seqstart AS startval,
			r.last_value AS lastval,
			pg_catalog.format_type(s.seqtypid, NULL) AS type,
			s.seqincrement AS increment,
			s.seqmax AS maxval,
			s.seqmin AS minval,
			s.seqcache AS cacheval,
			s.seqcycle AS iscycled,
			r.is_called AS iscalled
		FROM %s r
		JOIN pg_sequence s ON s.seqrelid = '%s'::regclass::oid;`, seqName, seqName)

	query := ""
	if connectionPool.Version.Before("7") {
		query = before7Query
	} else {
		query = atLeast7Query
	}

	result := SequenceDefinition{}
	err := connectionPool.Get(&result, query)
	gplog.FatalOnError(err)
	return result
}

// not used any more, https://github.com/greenplum-db/gpbackup/commit/0c421c6238d51ce5b75f555f5fbf2413bb53f0c0
func GetSequenceColumnOwnerMap(connectionPool *dbconn.DBConn) (map[string]string, map[string]string) {
	query := fmt.Sprintf(`
	SELECT quote_ident(n.nspname) AS schema,
		quote_ident(s.relname) AS name,
		quote_ident(c.relname) AS tablename,
		quote_ident(a.attname) AS columnname
	FROM pg_depend d
		JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
		JOIN pg_class s ON s.oid = d.objid
		JOIN pg_class c ON c.oid = d.refobjid
		JOIN pg_namespace n ON n.oid = s.relnamespace
	WHERE s.relkind = 'S'
		AND %s`, relationAndSchemaFilterClause(connectionPool))

	gplog.Debug("GetSequenceColumnOwnerMap， query is %v", query)
	results := make([]struct {
		Schema     string
		Name       string
		TableName  string
		ColumnName string
	}, 0)
	sequenceOwnerTables := make(map[string]string)
	sequenceOwnerColumns := make(map[string]string)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err, fmt.Sprintf("Failed on query: %s", query))

	for _, seqOwner := range results {
		seqFQN := utils.MakeFQN(seqOwner.Schema, seqOwner.Name)
		tableFQN := fmt.Sprintf("%s.%s", seqOwner.Schema, seqOwner.TableName)
		columnFQN := fmt.Sprintf("%s.%s.%s", seqOwner.Schema, seqOwner.TableName, seqOwner.ColumnName)
		sequenceOwnerTables[seqFQN] = tableFQN
		sequenceOwnerColumns[seqFQN] = columnFQN
	}
	return sequenceOwnerTables, sequenceOwnerColumns
}

type View struct {
	Oid            uint32
	Schema         string
	Name           string
	Options        string
	Definition     sql.NullString
	Tablespace     string
	IsMaterialized bool
	// https://github.com/greenplum-db/gpbackup/commit/ea5977fc892735fbb9f41e1e0a4944e20e260077
	DistPolicy string
}

func (v View) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          v.Schema,
			Name:            v.Name,
			ObjectType:      v.ObjectType(),
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
			//Oid:             v.Oid,
			//ClassOID:        PG_CLASS_OID,
		}
}

func (v View) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_CLASS_OID, Oid: v.Oid}
}

func (v View) FQN() string {
	return utils.MakeFQN(v.Schema, v.Name)
}

func (v View) ObjectType() string {
	if v.IsMaterialized {
		return "MATERIALIZED VIEW"
	}
	return "VIEW"
}

// This function retrieves both regular views and materialized views.
// Materialized views were introduced in GPDB 7 and backported to GPDB 6.2.
func GetAllViews(connectionPool *dbconn.DBConn) []View {

	// When querying the view definition using pg_get_viewdef(), the pg function
	// obtains dependency locks that are not released until the transaction is
	// committed at the end of gpbackup session. This blocks other sessions
	// from commands that need AccessExclusiveLock (e.g. TRUNCATE).
	// NB: SAVEPOINT should be created only if there is transaction in progress
	if connectionPool.Tx[0] != nil {
		connectionPool.MustExec("SAVEPOINT gpbackup_get_views")
		defer connectionPool.MustExec("ROLLBACK TO SAVEPOINT gpbackup_get_views")
	}

	before6Query := fmt.Sprintf(`
	SELECT
		c.oid AS oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(c.relname) AS name,
		pg_get_viewdef(c.oid) AS definition
	FROM pg_class c
		LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace
	WHERE c.relkind IN ('m', 'v')
		AND %s
		AND %s`, relationAndSchemaFilterClause(connectionPool), ExtensionFilterClause("c"))

	// Materialized views were introduced in GPDB 7 and backported to GPDB 6.2.
	// Reloptions and tablespace added to pg_class in GPDB 6
	atLeast6Query := fmt.Sprintf(`
	SELECT
		c.oid AS oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(c.relname) AS name,
		pg_get_viewdef(c.oid) AS definition,
		coalesce(' WITH (' || array_to_string(c.reloptions, ', ') || ')', '') AS options,
		coalesce(quote_ident(t.spcname), '') AS tablespace,
		c.relkind='m' AS ismaterialized
	FROM pg_class c
		LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace
	WHERE c.relkind IN ('m', 'v')
		AND %s
		AND %s`, relationAndSchemaFilterClause(connectionPool), ExtensionFilterClause("c"))

	query := ""
	if connectionPool.Version.Before("6") {
		query = before6Query
	} else {
		query = atLeast6Query
	}

	gplog.Debug("GetAllViews, query is %v", query)
	results := make([]View, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	distPolicies := GetDistributionPolicies(connectionPool)

	// Remove all views that have NULL definitions. This can happen
	// if the query above is run and a concurrent view drop happens
	// just before the pg_get_viewdef function execute. Also, error
	// out if a view has anyarray typecasts in their view definition
	// as those will error out during restore. Anyarray typecasts can
	// show up if the view was created with array typecasting in an
	// OpExpr (Operator Expression) argument.
	verifiedResults := make([]View, 0)
	for _, result := range results {
		if result.Definition.Valid {
			if strings.Contains(result.Definition.String, "::anyarray") {
				gplog.Fatal(errors.Errorf("Detected anyarray type cast in view definition for View '%s'", result.FQN()),
					"Drop the view or recreate the view without explicit array type casts.")
			}
		} else {
			// do not append views with invalid definitions
			gplog.Warn("View '%s.%s' not backed up, most likely dropped after gpbackup had begun.", result.Schema, result.Name)
			continue
		}

		if result.IsMaterialized {
			result.DistPolicy = distPolicies[result.Oid]
		}
		verifiedResults = append(verifiedResults, result)

	}

	return verifiedResults
}

/*
type MaterializedView struct {
	Oid        uint32
	Schema     string
	Name       string
	Options    string
	Tablespace string
	Definition string
	Policy     string
}

func (v MaterializedView) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          v.Schema,
			Name:            v.Name,
			ObjectType:      "MATERIALIZED VIEW",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
			Oid:             v.Oid,
			ClassOID:        PG_CLASS_OID,
		}
}

func (v MaterializedView) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_CLASS_OID, Oid: v.Oid}
}

func (v MaterializedView) FQN() string {
	return utils.MakeFQN(v.Schema, v.Name)
}

*/

/*

func makeMaterializedView(view View) MaterializedView {
	return MaterializedView{
		Oid:        view.Oid,
		Schema:     view.Schema,
		Name:       view.Name,
		Options:    view.Options,
		Definition: view.Definition,
		Tablespace: view.Tablespace,
		Policy:     view.Policy,
	}
}

*/

// GenerateTableBatches batches tables to reduce network congestion and
// resource contention.  Returns an array of batches where a batch of tables is
// a single string with comma separated tables
func GenerateTableBatches(tables []Relation, batchSize int) []string {
	var tableNames []string
	for _, table := range tables {
		tableNames = append(tableNames, table.FQN())
	}

	var end int
	var batches []string
	i := 0
	for i < len(tables) {
		if i+batchSize < len(tables) {
			end = i + batchSize
		} else {
			end = len(tables)
		}

		batches = append(batches, strings.Join(tableNames[i:end], ", "))
		i = end
	}

	return batches
}
