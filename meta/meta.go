package meta

import (
	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/meta/builtin"
	"github.com/cloudberrydb/cbcopy/options"
	"github.com/cloudberrydb/cbcopy/utils"
)

type MetaOperator interface {
	Open(srcConn, destConn *dbconn.DBConn)
	CopyDatabaseMetaData(tablec chan options.TablePair, donec chan struct{}) utils.ProgressBar
	CopySchemaMetaData(sschemas, dschemas []*options.DbSchema, tablec chan options.TablePair, donec chan struct{}) utils.ProgressBar
	CopyTableMetaData(dschemas []*options.DbSchema,
		sschemas []string,
		tables []string,
		tablec chan options.TablePair,
		donec chan struct{}) utils.ProgressBar
	CopyPostData()
	GetErrorTableMetaData() map[string]builtin.Empty
	Close()
}

func CreateMetaImpl(convert, withGlobal, metaOnly bool,
	timestamp string,
	partNameMap map[string][]string,
	tableMap map[string]string,
	ownerMap map[string]string) MetaOperator {

	return builtin.NewBuiltinMeta(convert,
		withGlobal,
		metaOnly,
		timestamp,
		partNameMap,
		tableMap,
		ownerMap)
}
