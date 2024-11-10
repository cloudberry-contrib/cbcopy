package copy

import (
	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/options"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
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
			schemaMap := make(map[string]bool)
			for _, t := range destTables {
				schemaMap[t.Schema] = true
			}

			for k, _ := range schemaMap {
				if !SchemaExists(destConn, k) {
					gplog.Fatal(errors.Errorf("Please create the schema \"%v\" on the dest database \"%v\" first", k, destConn.DBName), "")
				}
			}

			pgd = metaOps.CopyTableMetaData(option.GetDestSchemas(), srcTables, tablec, donec)
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
