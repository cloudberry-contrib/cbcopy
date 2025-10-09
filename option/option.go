package option

import (
	"regexp"
	"strings"

	"github.com/cloudberry-contrib/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

const (
	APPEND                  = "append"
	DBNAME                  = "dbname"
	DEBUG                   = "debug"
	DEST_DBNAME             = "dest-dbname"
	DEST_HOST               = "dest-host"
	DEST_PORT               = "dest-port"
	DEST_TABLE              = "dest-table"
	DEST_TABLE_FILE         = "dest-table-file"
	DEST_USER               = "dest-user"
	EXCLUDE_TABLE           = "exclude-table"
	EXCLUDE_TABLE_FILE      = "exclude-table-file"
	FULL                    = "full"
	INCLUDE_TABLE           = "include-table"
	INCLUDE_TABLE_FILE      = "include-table-file"
	COPY_JOBS               = "copy-jobs"
	METADATA_JOBS           = "metadata-jobs"
	METADATA_ONLY           = "metadata-only"
	GLOBAL_METADATA_ONLY    = "global-metadata-only"
	DATA_ONLY               = "data-only"
	WITH_GLOBAL_METADATA    = "with-global-metadata"
	COMPRESSION             = "compression"
	ON_SEGMENT_THRESHOLD    = "on-segment-threshold"
	QUIET                   = "quiet"
	SOURCE_HOST             = "source-host"
	SOURCE_PORT             = "source-port"
	SOURCE_USER             = "source-user"
	TRUNCATE                = "truncate"
	VALIDATE                = "validate"
	SCHEMA                  = "schema"
	EXCLUDE_SCHEMA          = "exclude-schema" // test purpose, to reuse gpbackup integration test case
	DEST_SCHEMA             = "dest-schema"
	SCHEMA_MAPPING_FILE     = "schema-mapping-file"
	OWNER_MAPPING_FILE      = "owner-mapping-file"
	DEST_TABLESPACE         = "dest-tablespace"
	TABLESPACE_MAPPING_FILE = "tablespace-mapping-file"
	VERBOSE                 = "verbose"
	DATA_PORT_RANGE         = "data-port-range"
	CONNECTION_MODE         = "connection-mode"
)

const (
	CopyModeFull   = "full"
	CopyModeDb     = "db"
	CopyModeSchema = "schema"
	CopyModeTable  = "table"
)

const (
	ConnectionModePush = "push"
	ConnectionModePull = "pull"
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
	Schema       string
	Name         string
	Partition    int
	RelTuples    int64
	IsReplicated bool
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
	Partition    int
	RelTuples    int64
	IsReplicated bool
}

type Option struct {
	copyMode       string
	tableMode      string
	connectionMode string

	sourceDbnames  []string
	destDbnames    []string
	excludedTables []*DbTable

	includedTables []*DbTable
	destTables     []*DbTable

	sourceSchemas []*DbSchema
	destSchemas   []*DbSchema

	ownerMap      map[string]string
	tablespaceMap map[string]string
}

func NewOption(initialFlags *pflag.FlagSet) (*Option, error) {
	copyMode, tableMode := CopyModeFull, TableModeTruncate

	connectionMode, err := initialFlags.GetString(CONNECTION_MODE)
	if err != nil {
		return nil, err
	}

	sourceDbnames, err := getDbNames(initialFlags, DBNAME)
	if err != nil {
		return nil, err
	}
	if len(sourceDbnames) > 0 {
		copyMode = CopyModeDb
	}

	destDbnames, err := getDbNames(initialFlags, DEST_DBNAME)
	if err != nil {
		return nil, err
	}

	excludeTables, err := getTables(initialFlags, EXCLUDE_TABLE, EXCLUDE_TABLE_FILE, "exclude table")
	if err != nil {
		return nil, err
	}

	sourceSchemas, destSchemas, err := getSchemas(initialFlags, &copyMode)
	if err != nil {
		return nil, err
	}

	includeTables, err := getTables(initialFlags, INCLUDE_TABLE, INCLUDE_TABLE_FILE, "include table")
	if err != nil {
		return nil, err
	}
	if len(includeTables) > 0 {
		copyMode = CopyModeTable
	}

	destTables, err := getTables(initialFlags, DEST_TABLE, DEST_TABLE_FILE, "dest table")
	if err != nil {
		return nil, err
	}

	if append, _ := initialFlags.GetBool(APPEND); append {
		tableMode = TableModeAppend
	}

	ownerMap, err := getOwnerMap(initialFlags)
	if err != nil {
		return nil, err
	}

	tablespaceMap, err := getTablespaceMap(initialFlags)
	if err != nil {
		return nil, err
	}

	return &Option{
		copyMode:       copyMode,
		tableMode:      tableMode,
		connectionMode: connectionMode,
		sourceDbnames:  sourceDbnames,
		destDbnames:    destDbnames,
		excludedTables: excludeTables,
		includedTables: includeTables,
		destTables:     destTables,
		sourceSchemas:  sourceSchemas,
		destSchemas:    destSchemas,
		ownerMap:       ownerMap,
		tablespaceMap:  tablespaceMap,
	}, nil
}

func getDbNames(flags *pflag.FlagSet, flagName string) ([]string, error) {
	return flags.GetStringSlice(flagName)
}

func getTables(flags *pflag.FlagSet, tableFlag, fileFlag, title string) ([]*DbTable, error) {
	tables, err := flags.GetStringSlice(tableFlag)
	if err != nil {
		return nil, err
	}
	if len(tables) == 0 {
		tables, err = utils.ReadTableFileByFlag(flags, fileFlag)
		if err != nil {
			return nil, err
		}
	}
	return validateTables(title, tables)
}

func getSchemas(flags *pflag.FlagSet, copyMode *string) ([]*DbSchema, []*DbSchema, error) {
	schemas, err := flags.GetStringSlice(SCHEMA)
	if err != nil {
		return nil, nil, err
	}
	if len(schemas) > 0 {
		*copyMode = CopyModeSchema
	}
	sourceSchemas, err := validateSchemas(schemas)
	if err != nil {
		return nil, nil, err
	}

	schemas, err = flags.GetStringSlice(DEST_SCHEMA)
	if err != nil {
		return nil, nil, err
	}
	destSchemas, err := validateSchemas(schemas)
	if err != nil {
		return nil, nil, err
	}

	if len(sourceSchemas) == 0 {
		schemaContent, err := utils.ReadTableFileByFlag(flags, SCHEMA_MAPPING_FILE)
		if err != nil {
			return nil, nil, err
		}
		ss, ds := utils.ParseSchemaMappingFile(schemaContent)
		if len(ss) > 0 {
			*copyMode = CopyModeSchema
			sourceSchemas, err = validateSchemas(ss)
			if err != nil {
				return nil, nil, err
			}
			destSchemas, err = validateSchemas(ds)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	return sourceSchemas, destSchemas, nil
}

func getOwnerMap(flags *pflag.FlagSet) (map[string]string, error) {
	return getMapping(flags, OWNER_MAPPING_FILE)
}

func getTablespaceMap(flags *pflag.FlagSet) (map[string]string, error) {
	return getMapping(flags, TABLESPACE_MAPPING_FILE)
}

func getMapping(flags *pflag.FlagSet, fileFlag string) (map[string]string, error) {
	lines, err := utils.ReadTableFileByFlag(flags, fileFlag)
	if err != nil {
		return nil, err
	}
	return utils.ParseMappingFile(lines), nil
}

func validateTables(title string, tableList []string) ([]*DbTable, error) {
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

func (o Option) GetConnectionMode() string {
	return o.connectionMode
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

func (o Option) ContainsMetadata(metadataOnly, dataOnly bool) bool {
	if metadataOnly || (!metadataOnly && !dataOnly) {
		return true
	}

	return false
}

func (o Option) GetOwnerMap() map[string]string {
	return o.ownerMap
}

func (o Option) GetTablespaceMap() map[string]string {
	destTablespace := utils.MustGetFlagString(DEST_TABLESPACE)
	if len(destTablespace) > 0 {
		if len(o.tablespaceMap) != 0 {
			gplog.Fatal(errors.Errorf("The tablespace map must be empty. Current contents: %v", o.tablespaceMap), "")
		}

		o.tablespaceMap[destTablespace] = ""
		return o.tablespaceMap
	}

	return o.tablespaceMap
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

func MakeIncludeOptions(initialFlags *pflag.FlagSet, testTableName string) {
	initialFlags.Set(COPY_JOBS, "1")
	initialFlags.Set(METADATA_JOBS, "1")
	initialFlags.Set(METADATA_ONLY, "true")
	initialFlags.Set(WITH_GLOBAL_METADATA, "true")
	initialFlags.Set(INCLUDE_TABLE, "postgres."+testTableName)
	initialFlags.Set(TRUNCATE, "true")
}
