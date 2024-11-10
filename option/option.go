package options

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

const (
	ANALYZE              = "analyze"
	APPEND               = "append"
	DBNAME               = "dbname"
	DEBUG                = "debug"
	DEST_DBNAME          = "dest-dbname"
	DEST_HOST            = "dest-host"
	DEST_PORT            = "dest-port"
	DEST_TABLE           = "dest-table"
	DEST_TABLE_FILE      = "dest-table-file"
	DEST_USER            = "dest-user"
	EXCLUDE_TABLE        = "exclude-table"
	EXCLUDE_TABLE_FILE   = "exclude-table-file"
	FULL                 = "full"
	INCLUDE_TABLE        = "include-table"
	INCLUDE_TABLE_FILE   = "include-table-file"
	COPY_JOBS            = "copy-jobs"
	METADATA_JOBS        = "metadata-jobs"
	METADATA_ONLY        = "metadata-only"
	GLOBAL_METADATA_ONLY = "global-metadata-only"
	DATA_ONLY            = "data-only"
	WITH_GLOBALMETA      = "with-globalmeta"
	COMPRESSION          = "compression"
	ON_SEGMENT_THRESHOLD = "on-segment-threshold"
	QUIET                = "quiet"
	SOURCE_HOST          = "source-host"
	SOURCE_PORT          = "source-port"
	SOURCE_USER          = "source-user"
	TRUNCATE             = "truncate"
	VALIDATE             = "validate"
	SCHEMA               = "schema"
	EXCLUDE_SCHEMA       = "exclude-schema" // test purpose, to reuse gpbackup integration test case
	DEST_SCHEMA          = "dest-schema"
	STATISTICS_ONLY      = "statistics-only"
	STATISTICS_JOBS      = "statistics-jobs"
	STATISTICS_FILE      = "statistics-file"
	SCHEMA_MAPPING_FILE  = "schema-mapping-file"
	OWNER_MAPPING_FILE   = "owner-mapping-file"
	TABLESPACE           = "tablespace"
	VERBOSE              = "verbose"
	DATA_PORT_RANGE      = "data-port-range"
)

const (
	CopyModeFull   = "full"
	CopyModeDb     = "db"
	CopyModeSchema = "schema"
	CopyModeTable  = "table"
)

const (
	TableModeTruncate = "truncate"
	TableModeAppend   = "append"
)

type DbTable struct {
	Database string
	Table
}

type Table struct {
	Schema    string
	Name      string
	Partition int
	RelTuples int64
}

type TablePair struct {
	SrcTable  Table
	DestTable Table
}

type DbSchema struct {
	Database string
	Schema   string
}

type TableStatistics struct {
	Partition int
	RelTuples int64
}

type Option struct {
	copyMode  string
	tableMode string

	sourceDbnames  []string
	destDbnames    []string
	excludedTables []*DbTable

	includedTables []*DbTable
	destTables     []*DbTable

	sourceSchemas []*DbSchema
	destSchemas   []*DbSchema

	ownerMap map[string]string

	statistics map[string]map[string]int64
}

func NewOption(initialFlags *pflag.FlagSet) (*Option, error) {
	copyMode := CopyModeFull
	tableMode := TableModeTruncate

	sourceDbnames, err := initialFlags.GetStringSlice(DBNAME)
	if err != nil {
		return nil, err
	}

	if len(sourceDbnames) > 0 {
		copyMode = CopyModeDb
	}

	destDbnames, err := initialFlags.GetStringSlice(DEST_DBNAME)
	if err != nil {
		return nil, err
	}

	tables := make([]string, 0)
	tables, err = initialFlags.GetStringSlice(EXCLUDE_TABLE)
	if err != nil {
		return nil, err
	}

	if len(tables) == 0 {
		if tables, err = readTableFileByFlag(initialFlags, EXCLUDE_TABLE_FILE); err != nil {
			return nil, err
		}
	}

	excludeTables, err := validateTables("exclude table", tables, false)
	if err != nil {
		return nil, err
	}

	schemas, err := initialFlags.GetStringSlice(SCHEMA)
	if err != nil {
		return nil, err
	}

	if len(schemas) > 0 {
		copyMode = CopyModeSchema
	}

	sourceSchemas, err := validateSchemas(schemas)
	if err != nil {
		return nil, err
	}

	schemas, err = initialFlags.GetStringSlice(DEST_SCHEMA)
	if err != nil {
		return nil, err
	}

	destSchemas, err := validateSchemas(schemas)
	if err != nil {
		return nil, err
	}

	if len(sourceSchemas) == 0 {
		schemaContent, err := readTableFileByFlag(initialFlags, SCHEMA_MAPPING_FILE)
		if err != nil {
			return nil, err
		}

		ss, ds := parseSchemaMappingFile(schemaContent)
		if len(ss) > 0 {
			copyMode = CopyModeSchema

			sourceSchemas, err = validateSchemas(ss)
			if err != nil {
				return nil, err
			}
			destSchemas, err = validateSchemas(ds)
			if err != nil {
				return nil, err
			}
		}
	}

	tables, err = initialFlags.GetStringSlice(INCLUDE_TABLE)
	if err != nil {
		return nil, err
	}

	if len(tables) == 0 {
		if tables, err = readTableFileByFlag(initialFlags, INCLUDE_TABLE_FILE); err != nil {
			return nil, err
		}
	}

	includeTables, err := validateTables("include table", tables, true)
	if err != nil {
		return nil, err
	}
	if len(includeTables) > 0 {
		copyMode = CopyModeTable
	}

	tables, err = initialFlags.GetStringSlice(DEST_TABLE)
	if err != nil {
		return nil, err
	}

	if len(tables) == 0 {
		if tables, err = readTableFileByFlag(initialFlags, DEST_TABLE_FILE); err != nil {
			return nil, err
		}
	}

	destTables, err := validateTables("dest table", tables, true)
	if err != nil {
		return nil, err
	}

	if append, _ := initialFlags.GetBool(APPEND); append {
		tableMode = TableModeAppend
	}

	lines, err := readTableFileByFlag(initialFlags, STATISTICS_FILE)
	if err != nil {
		return nil, err
	}

	statistics := parseStatisticsInfo(lines)

	lines, err = readTableFileByFlag(initialFlags, OWNER_MAPPING_FILE)
	if err != nil {
		return nil, err
	}

	ownerMap := parseOwnerMappingFile(lines)

	return &Option{
		copyMode:       copyMode,
		tableMode:      tableMode,
		sourceDbnames:  sourceDbnames,
		destDbnames:    destDbnames,
		excludedTables: excludeTables,
		includedTables: includeTables,
		destTables:     destTables,
		sourceSchemas:  sourceSchemas,
		destSchemas:    destSchemas,
		statistics:     statistics,
		ownerMap:       ownerMap,
	}, nil

}

func validateTables(title string, tableList []string, check bool) ([]*DbTable, error) {
	if len(tableList) == 0 {
		return nil, nil
	}

	result := make([]*DbTable, 0)
	dbs := make(map[string]bool)

	validFormat := regexp.MustCompile(`^(.+)\.(.+)\.(.+)$`)
	for _, fqn := range tableList {
		if !validFormat.Match([]byte(fqn)) {
			return nil, errors.Errorf(`Table %s is not correctly fully-qualified.  Please ensure that it is in the format database.schema.table.`, fqn)
		}

		sl := validFormat.FindStringSubmatch(fqn)
		result = append(result, &DbTable{Database: sl[1], Table: Table{Schema: sl[2], Name: sl[3]}})
		dbs[sl[1]] = true
	}

	if len(dbs) > 1 {
		return nil, errors.Errorf(`All %s should belong to the same database.`, title)
	}

	return result, nil
}

func validateSchemas(schemas []string) ([]*DbSchema, error) {
	if len(schemas) == 0 {
		return nil, nil
	}

	result := make([]*DbSchema, 0)
	dbs := make(map[string]bool)

	for _, schema := range schemas {
		sl := strings.Split(schema, ".")
		if len(sl) != 2 {
			return nil, errors.Errorf(`Schema %s is not correctly fully-qualified.  Please ensure that it is in the format database.schema.`, schema)
		}

		result = append(result, &DbSchema{Database: sl[0], Schema: sl[1]})
		dbs[sl[0]] = true
	}

	if len(dbs) > 1 {
		return nil, errors.Errorf(`All schemas should belong to the same database.`)
	}

	return result, nil
}

func (o Option) GetCopyMode() string {
	return o.copyMode
}

func (o Option) GetTableMode() string {
	return o.tableMode
}

func (o Option) GetSourceDbnames() []string {
	return o.sourceDbnames
}

func (o Option) GetDestDbnames() []string {
	return o.destDbnames
}

func (o Option) GetSourceSchemas() []*DbSchema {
	return o.sourceSchemas
}

func (o Option) GetDestSchemas() []*DbSchema {
	return o.destSchemas
}

func (o Option) GetSchemaMap() map[string]string {
	results := make(map[string]string)

	i := 0
	for _, v := range o.sourceSchemas {
		results[v.Schema] = o.destSchemas[i].Schema
		i++
	}

	return results
}

func (o Option) GetIncludeTablesByDb(dbname string) []Table {
	return o.getTablesByDb(dbname, o.includedTables)
}

func (o Option) GetDestTablesByDb(dbname string) []Table {
	return o.getTablesByDb(dbname, o.destTables)
}

func (o Option) GetExclTablesByDb(dbname string) []Table {
	return o.getTablesByDb(dbname, o.excludedTables)
}

func (o Option) GetIncludePartTablesByDb(dbname string) []Table {
	tables := o.getTablesByDb(dbname, o.includedTables)

	results := make([]Table, 0)
	for _, t := range tables {
		if t.Partition == 1 {
			results = append(results, Table{Schema: t.Schema, Name: t.Name, Partition: t.Partition})
		}
	}
	return results
}

func (o Option) GetTblSourceDbnames() []string {
	results := make([]string, 0)

	results = append(results, o.includedTables[0].Database)

	return results
}

func (o Option) GetTblDestDbnames() []string {
	results := make([]string, 0)

	dbMap := make(map[string]bool)

	for _, v := range o.destTables {
		dbMap[v.Database] = true
	}

	for k, _ := range dbMap {
		results = append(results, k)
	}

	return results
}

func (o Option) getTablesByDb(dbname string, tables []*DbTable) []Table {
	results := make([]Table, 0)

	for i := 0; i < len(tables); i++ {
		if dbname != tables[i].Database {
			continue
		}
		results = append(results, Table{Schema: tables[i].Schema, Name: tables[i].Name, Partition: tables[i].Partition})
	}

	return results
}

func (o Option) MarkIncludeTables(dbname string, userTables map[string]TableStatistics, partTables map[string]bool) {
	o.markTables(dbname, o.includedTables, userTables, partTables)
}

func (o Option) MarkDestTables(dbname string, userTables map[string]TableStatistics, partTables map[string]bool) {
	o.markTables(dbname, o.destTables, userTables, partTables)
}

func (o Option) MarkExcludeTables(dbname string, userTables map[string]TableStatistics, partTables map[string]bool) {
	o.markTables(dbname, o.excludedTables, userTables, partTables)
}

func (o Option) markTables(dbname string, tables []*DbTable, userTables map[string]TableStatistics, partTables map[string]bool) {
	for i := 0; i < len(tables); i++ {
		if dbname != tables[i].Database {
			continue
		}

		k := tables[i].Schema + "." + tables[i].Name

		_, exists := userTables[k]
		if exists {
			tables[i].Partition = 0
			continue
		}

		_, exists = partTables[k]
		if exists {
			tables[i].Partition = 1
		}
	}
}

func (o Option) GetDestTables() []*DbTable {
	return o.destTables
}

func (o Option) IsBaseTableMode() bool {
	return o.copyMode == CopyModeTable && len(o.GetDestTables()) == 0
}

func (o Option) ContainsMetadata(metadataOnly, dataOnly, statisticsOnly bool) bool {
	if metadataOnly ||
		(!metadataOnly && !dataOnly && !statisticsOnly) {
		return true
	}
	return false
}

func (o Option) GetTableStatistics(dbname, schema, table string) (bool, int64) {
	tableStat, exist := o.statistics[dbname]
	if !exist {
		return false, 0
	}

	tableName := schema + "." + table
	count, exist := tableStat[tableName]
	if !exist {
		return false, 0
	}
	return true, count
}

func (o Option) GetOwnerMap() map[string]string {
	return o.ownerMap
}

func (o Option) validatePartTables(title string, tables []*DbTable, userTables map[string]TableStatistics, dbname string) {
	for _, t := range tables {
		if t.Partition == 1 {
			gplog.Fatal(errors.Errorf("Found partition root table: %s.%s.%s in %s list", dbname, t.Schema, t.Name, title), "")
		}

		k := t.Schema + "." + t.Name

		_, exists := userTables[k]
		if !exists {
			gplog.Fatal(errors.Errorf("%v \"%v\" does not exists on \"%v\" database", title, k, dbname), "")
		}
	}
}

func (o Option) ValidateIncludeTables(userTables map[string]TableStatistics, dbname string) {
	o.validatePartTables("include table", o.includedTables, userTables, dbname)
}

func (o Option) ValidateExcludeTables(userTables map[string]TableStatistics, dbname string) {
	o.validatePartTables("exclude table", o.excludedTables, userTables, dbname)
}

func (o Option) ValidateDestTables(userTables map[string]TableStatistics, dbname string) {
	o.validatePartTables("dest table", o.destTables, userTables, dbname)
}

type FqnStruct struct {
	SchemaName string
	TableName  string
}

// https://github.com/greenplum-db/gpbackup/commit/6e9829cf5c12fb8e66cecd8143ac0a7379e44d01
func QuoteTableNames(conn *dbconn.DBConn, tableNames []string) ([]string, error) {
	if len(tableNames) == 0 {
		return []string{}, nil
	}

	// Properly escape single quote before running quote ident. Postgres
	// quote_ident escapes single quotes by doubling them
	escapedTables := make([]string, 0)
	for _, v := range tableNames {
		escapedTables = append(escapedTables, utils.EscapeSingleQuotes(v))
	}

	fqnSlice, err := SeparateSchemaAndTable(escapedTables)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)

	quoteIdentTableFQNQuery := `SELECT quote_ident('%s') AS schemaname, quote_ident('%s') AS tablename`
	for _, fqn := range fqnSlice {
		queryResultTable := make([]FqnStruct, 0)
		query := fmt.Sprintf(quoteIdentTableFQNQuery, fqn.SchemaName, fqn.TableName)
		gplog.Debug("QuoteTableNames, query is %v", query)
		err := conn.Select(&queryResultTable, query)
		if err != nil {
			return nil, err
		}
		quoted := queryResultTable[0].SchemaName + "." + queryResultTable[0].TableName
		result = append(result, quoted)
	}

	return result, nil
}

func SeparateSchemaAndTable(tableNames []string) ([]FqnStruct, error) {
	fqnSlice := make([]FqnStruct, 0)
	for _, fqn := range tableNames {
		parts := strings.Split(fqn, ".")
		if len(parts) > 2 {
			return nil, errors.Errorf("cannot process an Fully Qualified Name with embedded dots yet: %s", fqn)
		}
		if len(parts) < 2 {
			return nil, errors.Errorf("Fully Qualified Names require a minimum of one dot, specifying the schema and table. Cannot process: %s", fqn)
		}
		schema := parts[0]
		table := parts[1]
		if schema == "" || table == "" {
			return nil, errors.Errorf("Fully Qualified Names must specify the schema and table. Cannot process: %s", fqn)
		}

		currFqn := FqnStruct{
			SchemaName: schema,
			TableName:  table,
		}

		fqnSlice = append(fqnSlice, currFqn)
	}

	return fqnSlice, nil
}

func readTableFileByFlag(flagSet *pflag.FlagSet, flag string) ([]string, error) {
	filename, err := flagSet.GetString(flag)
	if err != nil {
		return nil, err
	}

	if len(filename) > 0 {
		tables, err := utils.ReadTableFile(filename)
		if err != nil {
			return nil, err
		}
		return tables, nil
	}

	return nil, nil
}

func CheckExclusiveFlags(flags *pflag.FlagSet, flagNames ...string) {
	numSet := 0
	for _, name := range flagNames {
		if flags.Changed(name) {
			numSet++
		}
	}
	if numSet > 1 {
		gplog.Fatal(errors.Errorf("The following flags may not be specified together: %s", strings.Join(flagNames, ", ")), "")
	}
}

func parseStatisticsInfo(lines []string) map[string]map[string]int64 {
	statistics := make(map[string]map[string]int64)

	for _, l := range lines {
		items := strings.Split(l, "\t")
		if len(items) != 2 {
			gplog.Fatal(errors.Errorf(`invalid statistics file content [%s]: every line
			should have two fields, seperated by tab. the first field is table name,
			the second field is the number of table records`, l), "")
		}

		sl := strings.Split(items[0], ".")
		if len(sl) != 3 {
			gplog.Fatal(errors.Errorf(`invalid statistics file content [%s]: please
			ensure table name is in the format "database.schema.table"`, l), "")
		}

		dbname := sl[0]
		table := sl[1] + "." + sl[2]

		tableStat, exist := statistics[dbname]
		if !exist {
			tableStat = make(map[string]int64)
		}

		count, err := strconv.ParseInt(items[1], 10, 64)
		if err != nil {
			gplog.Fatal(errors.Errorf(`invalid statistics file content [%s]: please
			ensure the number of table records is an integer`, l), "")
		}
		tableStat[table] = count

		statistics[dbname] = tableStat
	}

	return statistics
}

func parseOwnerMappingFile(lines []string) map[string]string {
	ownerMap := make(map[string]string)

	for _, l := range lines {
		items := strings.Split(l, ",")
		if len(items) != 2 {
			gplog.Fatal(errors.Errorf(`invalid owner mapping file content [%s]: every line
			should have two fields, seperated by comma. the first field is the source owner
			name, the second field is the target owner name`, l), "")
		}

		ownerMap[items[0]] = items[1]
	}

	return ownerMap
}

func parseSchemaMappingFile(lines []string) ([]string, []string) {
	source := make([]string, 0)
	dest := make([]string, 0)

	for _, l := range lines {
		items := strings.Split(l, ",")
		if len(items) != 2 {
			gplog.Fatal(errors.Errorf(`invalid schema mapping file content [%s]: every line
			should have two fields, seperated by comma. the first field is the source schema 
			name, the second field is the target schema name`, l), "")
		}

		source = append(source, items[0])
		dest = append(dest, items[1])
	}

	if lines != nil {
		if len(source) == 0 {
			gplog.Fatal(errors.Errorf(`schema mapping file should have at lease one record`), "")
		}

		return source, dest
	}

	return nil, nil
}

func MakeIncludeOptions(initialFlags *pflag.FlagSet, testTableName string) {
	initialFlags.Set(COPY_JOBS, "1")
	initialFlags.Set(METADATA_JOBS, "1")
	initialFlags.Set(METADATA_ONLY, "true")
	initialFlags.Set(WITH_GLOBALMETA, "true")
	initialFlags.Set(INCLUDE_TABLE, "postgres."+testTableName)
	initialFlags.Set(TRUNCATE, "true")
}

// note:
// options/options.go has getUserTableRelationsWithIncludeFiltering which was changed for GP7 support (https://github.com/greenplum-db/gpbackup/commit/e00d2a1f6c027b2f634177d4b022fe3b70404619)
// cbcopy don't have that function
