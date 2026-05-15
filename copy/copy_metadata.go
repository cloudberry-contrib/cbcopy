package copy

import (
	"strings"

	"github.com/cloudberry-contrib/cbcopy/internal/dbconn"
	"github.com/cloudberry-contrib/cbcopy/meta"
	"github.com/cloudberry-contrib/cbcopy/meta/builtin"
	"github.com/cloudberry-contrib/cbcopy/option"
	"github.com/cloudberry-contrib/cbcopy/utils"
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
	withGlobal, metaOnly bool,
	timestamp string,

	partNameMap map[string][]string,
	tableMap map[string]string,
	ownerMap map[string]string,
	tablespaceMap map[string]string) *MetadataManager {

	metaOps := meta.CreateMetaImpl(withGlobal, metaOnly, timestamp, partNameMap, tableMap, ownerMap, tablespaceMap)

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

// MigrateMetadata manages all pre-data operations.
//
// Note: --skip-existing pair filtering happens in doCopy() before this
// function is called, so srcTables/destTables here are already post-filter.
// Filtering here would be too late -- TableMap (which RestoreCleanup
// iterates at the tail of restorePredata) is constructed in NewMetadataManager
// above us. See issue #34.
func (m *MetadataManager) MigrateMetadata(srcTables, destTables, nonPhysicalRels []option.Table) (chan option.TablePair, utils.ProgressBar) {
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
			includeSchemas, includeTables := m.collectTablesAndSchemas(srcTables, nonPhysicalRels,
				m.queryWrapper.getPartitionTableMapping(m.srcConn, m.destConn, true))
			pgd = m.metaOps.CopyTableMetaData(config.GetDestSchemas(), includeSchemas, includeTables, tablec, m.donec)
		} else {
			pgd = m.fillTablePairChan(srcTables, destTables, tablec)
		}
	}

	return tablec, pgd
}

// RestorePostMetadata manages all post-data operations
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
	if !config.ContainsMetadata(utils.MustGetFlagBool(option.METADATA_ONLY), utils.MustGetFlagBool(option.DATA_ONLY)) {
		return
	}

	<-m.donec
}

// filterTablePairsByDestExisting drops, from the parallel src/dest table
// arrays, any entry whose destination side is already present on the
// destination database. A destination side counts as "present" if either
//
//   * its own FQN is in the destination inventory, or
//   * it is a partition leaf whose root is in the destination inventory.
//
// The second case matches the DDL-filter semantics in
// meta/builtin.FilterTablesByDestExisting (gpcopy parity: existence is
// authoritative at the root, so a tree where the root exists on dest is
// fully skipped, including leaves that exist only on the source). Without
// this check, a leaf that is in source but absent on dest would be kept
// here, its parent's DDL would still be skipped by the DDL filter, and
// the subsequent COPY would fail with "relation does not exist".
//
// partNameMap is the post-redirect map from dest root FQN to dest leaf
// FQNs returned by QueryWrapper.getPartitionTableMapping. May be empty
// when no partitioned tables are in scope.
func filterTablePairsByDestExisting(srcDbName, destDbName string, src, dst []option.Table, partNameMap map[string][]string) ([]option.Table, []option.Table) {
	if len(src) == 0 {
		return src, dst
	}

	// Invert partNameMap so we can ask "given a dest leaf FQN, what is
	// the dest root FQN?" in O(1).
	leafToRoot := make(map[string]string, len(partNameMap)*2)
	for root, leaves := range partNameMap {
		for _, leaf := range leaves {
			leafToRoot[leaf] = root
		}
	}

	rootExistsOnDest := func(rootFQN string) bool {
		parts := strings.SplitN(rootFQN, ".", 2)
		if len(parts) != 2 {
			return false
		}
		return config.IsDestTableExisting(destDbName, parts[0], parts[1])
	}

	keptSrc := make([]option.Table, 0, len(src))
	keptDst := make([]option.Table, 0, len(dst))
	for i := range src {
		dstSchema := dst[i].Schema
		dstName := dst[i].Name

		skip := false
		if config.IsDestTableExisting(destDbName, dstSchema, dstName) {
			skip = true
		} else if rootFQN, ok := leafToRoot[dstSchema+"."+dstName]; ok && rootExistsOnDest(rootFQN) {
			skip = true
		}

		if skip {
			builtin.RecordPairSkip(
				srcDbName, src[i].Schema, src[i].Name,
				destDbName, dstSchema, dstName,
			)
			continue
		}
		keptSrc = append(keptSrc, src[i])
		keptDst = append(keptDst, dst[i])
	}
	return keptSrc, keptDst
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
				Schema:       t.Schema,
				Name:         t.Name,
				RelTuples:    t.RelTuples,
				IsReplicated: t.IsReplicated,
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
func (m *MetadataManager) collectTablesAndSchemas(tables, nonPhysicalRels []option.Table, partNameMap map[string][]string) ([]string, []string) {
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

	for _, t := range nonPhysicalRels {
		schemaMap[t.Schema] = true
	}

	// Convert maps to sorted slices
	includeTables := make([]string, 0, len(tableMap))
	includeSchemas := make([]string, 0, len(schemaMap))

	for tableName := range tableMap {
		includeTables = append(includeTables, tableName)
	}

	for _, t := range nonPhysicalRels {
		includeTables = append(includeTables, t.Schema+"."+t.Name)
	}

	for schemaName := range schemaMap {
		includeSchemas = append(includeSchemas, schemaName)
	}

	return includeSchemas, includeTables
}
