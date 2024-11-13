package copy

import (
	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/meta"
	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
)

// MetadataManager handles all metadata related operations during copy process
type MetadataManager struct {
	srcConn      *dbconn.DBConn
	destConn     *dbconn.DBConn
	donec        chan struct{}
	queryManager *QueryManager
	queryWrapper *QueryWrapper
	metaOps      meta.MetaOperator
}

// NewMetadataManager creates a new MetadataManager instance
func NewMetadataManager(srcConn, destConn *dbconn.DBConn,
	qm *QueryManager,
	qw *QueryWrapper,
	convert, withGlobal, metaOnly bool,
	timestamp string,
	partNameMap map[string][]string,
	tableMap map[string]string,
	ownerMap map[string]string) *MetadataManager {

	metaOps := meta.CreateMetaImpl(convert, withGlobal, metaOnly, timestamp, partNameMap, tableMap, ownerMap)

	return &MetadataManager{
		srcConn:      srcConn,
		destConn:     destConn,
		donec:        make(chan struct{}),
		queryManager: qm,
		queryWrapper: qw,
		metaOps:      metaOps,
	}
}

func (m *MetadataManager) Open() {
	m.metaOps.Open(m.srcConn, m.destConn)
}

func (m *MetadataManager) Close() {
	m.metaOps.Close()
}

// MigrateMetadata manages all pre-data operations
func (m *MetadataManager) MigrateMetadata(srcTables, destTables []option.Table) (chan option.TablePair, utils.ProgressBar) {
	var pgd utils.ProgressBar

	mode := config.GetCopyMode()
	tablec := make(chan option.TablePair, len(destTables))

	if !config.ContainsMetadata(utils.MustGetFlagBool(option.METADATA_ONLY), utils.MustGetFlagBool(option.DATA_ONLY)) {
		pgd = m.fillTablePairChan(srcTables, destTables, tablec)
		return tablec, pgd
	}

	switch mode {
	case option.CopyModeFull:
		fallthrough
	case option.CopyModeDb:
		pgd = m.metaOps.CopyDatabaseMetaData(tablec, m.donec)
	case option.CopyModeSchema:
		pgd = m.metaOps.CopySchemaMetaData(config.GetSourceSchemas(), config.GetDestSchemas(), tablec, m.donec)
	case option.CopyModeTable:
		if len(config.GetDestTables()) == 0 {
			m.validateSchemaExists(m.destConn, destTables)
			includeSchemas, includeTables := m.collectTablesAndSchemas(srcTables,
				m.queryWrapper.getPartitionTableMapping(m.srcConn, m.destConn, true))
			pgd = m.metaOps.CopyTableMetaData(config.GetDestSchemas(), includeSchemas, includeTables, tablec, m.donec)
		} else {
			pgd = m.fillTablePairChan(srcTables, destTables, tablec)
		}
	}

	return tablec, pgd
}

// HandlePostDataTask manages all post-data operations
func (m *MetadataManager) RestorePostMetadata(dbname, timestamp string) {
	if !config.ContainsMetadata(utils.MustGetFlagBool(option.METADATA_ONLY), utils.MustGetFlagBool(option.DATA_ONLY)) {
		return
	}

	if len(config.GetDestTables()) > 0 {
		return
	}

	m.metaOps.CopyPostData()
}

// Wait blocks until metadata migration is complete
func (m *MetadataManager) Wait() {
	<-m.donec
}

// fillTablePairChan fills the table pair channel with source and destination tables
func (m *MetadataManager) fillTablePairChan(srcTables, destTables []option.Table, tablec chan option.TablePair) utils.ProgressBar {
	if len(destTables) == 0 {
		close(m.donec)
		return nil
	}

	title := "Table copied: "
	pgd := utils.NewProgressBar(len(destTables), title, utils.PB_VERBOSE)

	for i, t := range srcTables {
		tablec <- option.TablePair{
			SrcTable: option.Table{
				Schema:    t.Schema,
				Name:      t.Name,
				RelTuples: t.RelTuples,
			},
			DestTable: option.Table{
				Schema: destTables[i].Schema,
				Name:   destTables[i].Name,
			},
		}
	}

	close(m.donec)
	return pgd
}

// CollectTablesAndSchemas collects unique tables and schemas for metadata processing.
// It handles partition tables by mapping child tables to their parent tables.
// Returns two slices:
// - A list of table names (including parent partition tables instead of child tables)
// - A list of unique schema names
func (m *MetadataManager) collectTablesAndSchemas(tables []option.Table, partNameMap map[string][]string) ([]string, []string) {
	// Build leaf table to parent table mapping
	leafTableMap := make(map[string]string)
	for parentTable, leafTables := range partNameMap {
		for _, leafTable := range leafTables {
			leafTableMap[leafTable] = parentTable
		}
	}

	// Collect unique tables and schemas
	schemaMap := make(map[string]bool)
	tableMap := make(map[string]bool)

	for _, t := range tables {
		child := t.Schema + "." + t.Name
		if parent, exists := leafTableMap[child]; exists {
			// Use parent table instead of child table
			tableMap[parent] = true
		} else {
			tableMap[child] = true
		}
		schemaMap[t.Schema] = true
	}

	// Convert maps to sorted slices
	includeTables := make([]string, 0, len(tableMap))
	includeSchemas := make([]string, 0, len(schemaMap))

	for tableName := range tableMap {
		includeTables = append(includeTables, tableName)
	}

	for schemaName := range schemaMap {
		includeSchemas = append(includeSchemas, schemaName)
	}

	return includeSchemas, includeTables
}

// ValidateSchemaExists checks if all required schemas exist in the destination database.
// It collects all unique schemas from the table list and verifies their existence.
// If any schema is missing, it will terminate the program with an error message.
func (m *MetadataManager) validateSchemaExists(destConn *dbconn.DBConn, tables []option.Table) {
	// Collect all unique schemas
	schemaMap := make(map[string]bool)
	for _, t := range tables {
		schemaMap[t.Schema] = true
	}

	// Verify each schema exists in the destination database
	for schema := range schemaMap {
		exists, err := m.queryManager.SchemaExists(destConn, schema)
		if err != nil {
			gplog.Fatal(errors.Errorf("failed to check schema existence: %v", err), "")
		}

		if !exists {
			gplog.Fatal(errors.Errorf("Please create the schema \"%v\" on the destination database \"%v\" first",
				schema, destConn.DBName), "")
		}
	}
}
