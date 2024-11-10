package copy

import (
	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

func doPreDataTask(srcConn, destConn *dbconn.DBConn, srcTables, destTables []option.Table) (chan option.TablePair, chan struct{}, utils.ProgressBar) {
	var pgd utils.ProgressBar

	m := config.GetCopyMode()

	donec := make(chan struct{})
	tablec := make(chan option.TablePair, len(destTables))

	gplog.Info("doPreDataTask, mode: \"%v\"", m)

	if !config.ContainsMetadata(utils.MustGetFlagBool(option.METADATA_ONLY), utils.MustGetFlagBool(option.DATA_ONLY)) {
		pgd = fillTablePairChan(srcTables, destTables, tablec, donec)

		gplog.Info("doPreDataTask, no metadata to copy")

		return tablec, donec, pgd
	}

	switch m {
	case option.CopyModeFull:
		fallthrough
	case option.CopyModeDb:
		pgd = metaOps.CopyDatabaseMetaData(tablec, donec)
	case option.CopyModeSchema:
		pgd = metaOps.CopySchemaMetaData(config.GetSourceSchemas(), config.GetDestSchemas(), tablec, donec)
	case option.CopyModeTable:
		if len(config.GetDestTables()) == 0 {
			ValidateSchemaExists(destConn, destTables)
			includeSchemas, includeTables := CollectTablesAndSchemas(srcTables, GetPartTableMap(srcConn, destConn, true))

			pgd = metaOps.CopyTableMetaData(config.GetDestSchemas(), includeSchemas, includeTables, tablec, donec)
		} else {
			pgd = fillTablePairChan(srcTables, destTables, tablec, donec)
		}
	}

	return tablec, donec, pgd
}

func fillTablePairChan(srcTables,
	destTables []option.Table,
	tablec chan option.TablePair,
	donec chan struct{}) utils.ProgressBar {
	if len(destTables) == 0 {
		return nil
	}

	title := "Table copied: "
	pgd := utils.NewProgressBar(len(destTables), title, utils.PB_VERBOSE)

	for i, t := range srcTables {
		tablec <- option.TablePair{SrcTable: option.Table{Schema: t.Schema,
			Name:      t.Name,
			RelTuples: t.RelTuples},
			DestTable: option.Table{Schema: destTables[i].Schema,
				Name: destTables[i].Name}}
	}
	close(donec)

	return pgd
}

func doPostDataTask(dbname, timestamp string) {
	if !config.ContainsMetadata(utils.MustGetFlagBool(option.METADATA_ONLY), utils.MustGetFlagBool(option.DATA_ONLY)) {
		return
	}

	if len(config.GetDestTables()) > 0 {
		return
	}

	metaOps.CopyPostData()
}

// CollectTablesAndSchemas collects unique tables and schemas for metadata processing.
// It handles partition tables by mapping child tables to their parent tables.
// Returns two slices:
// - A list of table names (including parent partition tables instead of child tables)
// - A list of unique schema names
func CollectTablesAndSchemas(tables []option.Table, partNameMap map[string][]string) ([]string, []string) {
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
