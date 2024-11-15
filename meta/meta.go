package meta

import (
	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/meta/builtin"
	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
)

type MetaOperator interface {
	Open(srcConn, destConn *dbconn.DBConn)
	CopyDatabaseMetaData(tablec chan option.TablePair, donec chan struct{}) utils.ProgressBar
	CopySchemaMetaData(sschemas, dschemas []*option.DbSchema, tablec chan option.TablePair, donec chan struct{}) utils.ProgressBar
	CopyTableMetaData(dschemas []*option.DbSchema,
		sschemas []string,
		tables []string,
		tablec chan option.TablePair,
		donec chan struct{}) utils.ProgressBar
	CopyPostData()
	GetErrorTableMetaData() map[string]builtin.Empty
	Close()
}

func CreateMetaImpl(withGlobal, metaOnly bool,
	timestamp string,
	partNameMap map[string][]string,
	tableMap map[string]string,
	ownerMap map[string]string,
	tablespaceMap map[string]string) MetaOperator {

	return builtin.NewBuiltinMeta(withGlobal,
		metaOnly,
		timestamp,
		partNameMap,
		tableMap,
		ownerMap,
		tablespaceMap)
}
