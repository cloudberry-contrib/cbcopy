package copy

import (
	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/options"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

func doPreDataTask(srcConn, destConn *dbconn.DBConn, srcTables, destTables []options.Table) (chan options.TablePair, chan struct{}, utils.ProgressBar) {
	var pgd utils.ProgressBar

	m := option.GetCopyMode()

	donec := make(chan struct{})
	tablec := make(chan options.TablePair, len(destTables))

	gplog.Info("doPreDataTask, mode: \"%v\"", m)

	if !option.ContainsMetadata(utils.MustGetFlagBool(options.METADATA_ONLY),
		utils.MustGetFlagBool(options.DATA_ONLY),
		utils.MustGetFlagBool(options.STATISTICS_ONLY)) {
		pgd = fillTablePairChan(srcTables, destTables, tablec, donec)

		gplog.Info("doPreDataTask, no metadata to copy")

		return tablec, donec, pgd
	}

	switch m {
	case options.CopyModeFull:
		fallthrough
	case options.CopyModeDb:
		pgd = metaOps.CopyDatabaseMetaData(tablec, donec)
	case options.CopyModeSchema:
		pgd = metaOps.CopySchemaMetaData(option.GetSourceSchemas(), option.GetDestSchemas(), tablec, donec)
	case options.CopyModeTable:
		if len(option.GetDestTables()) == 0 {
			ValidateSchemaExists(destConn, destTables)
			includeSchemas, includeTables := CollectTablesAndSchemas(srcTables, GetPartTableMap(srcConn, destConn, true))

			pgd = metaOps.CopyTableMetaData(option.GetDestSchemas(), includeSchemas, includeTables, tablec, donec)
		} else {
			pgd = fillTablePairChan(srcTables, destTables, tablec, donec)
		}
	}

	return tablec, donec, pgd
}

func fillTablePairChan(srcTables,
	destTables []options.Table,
	tablec chan options.TablePair,
	donec chan struct{}) utils.ProgressBar {
	if len(destTables) == 0 {
		return nil
	}

	title := "Table copied: "
	if utils.MustGetFlagBool(options.STATISTICS_ONLY) {
		title = "Table collected: "
	}

	pgd := utils.NewProgressBar(len(destTables), title, utils.PB_VERBOSE)

	for i, t := range srcTables {
		tablec <- options.TablePair{SrcTable: options.Table{Schema: t.Schema,
			Name:      t.Name,
			RelTuples: t.RelTuples},
			DestTable: options.Table{Schema: destTables[i].Schema,
				Name: destTables[i].Name}}
	}
	close(donec)

	return pgd
}

func doPostDataTask(dbname, timestamp string) {
	if !option.ContainsMetadata(utils.MustGetFlagBool(options.METADATA_ONLY),
		utils.MustGetFlagBool(options.DATA_ONLY),
		utils.MustGetFlagBool(options.STATISTICS_ONLY)) {
		return
	}

	if len(option.GetDestTables()) > 0 {
		return
	}

	metaOps.CopyPostData()
}

// CollectTablesAndSchemas collects unique tables and schemas for metadata processing.
// It handles partition tables by mapping child tables to their parent tables.
// Returns two slices:
// - A list of table names (including parent partition tables instead of child tables)
// - A list of unique schema names
func CollectTablesAndSchemas(tables []options.Table, partNameMap map[string][]string) ([]string, []string) {
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
