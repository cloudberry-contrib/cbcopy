package copy

import (
	"fmt"
	"sync"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

type TableCopier struct {
	srcConn      *dbconn.DBConn
	destConn     *dbconn.DBConn
	srcTable     option.Table
	destTable    option.Table
	workerID     int
	progressBar  utils.ProgressBar
	copiedMap    map[string]int
	queryManager *QueryManager
}

func NewTableCopier(src, dest *dbconn.DBConn, srcTable, destTable option.Table, workerID int, progressBar utils.ProgressBar, copiedMap map[string]int) *TableCopier {
	return &TableCopier{
		srcConn:      src,
		destConn:     dest,
		srcTable:     srcTable,
		destTable:    destTable,
		workerID:     workerID,
		progressBar:  progressBar,
		copiedMap:    copiedMap,
		queryManager: NewQueryManager(),
	}
}

func (tc *TableCopier) Copy() {
	var inTxn bool
	var err error
	var isSkipped bool

	defer func() {
		tc.cleanupAfterCopy(isSkipped, inTxn, err)
	}()

	isSkipped = tc.shouldSkipCopy()
	if isSkipped {
		return
	}

	inTxn, err = tc.prepareForCopy()
	if err != nil {
		return
	}

	if err = tc.copyData(); err != nil {
		return
	}

	gplog.Debug("[Worker %v] Committing transaction on destination database", tc.workerID)
	err = tc.destConn.Commit(tc.workerID)
}

func (tc *TableCopier) prepareForCopy() (bool, error) {
	gplog.Debug("[Worker %v] There are %v rows in the source table \"%v.%v\"",
		tc.workerID, tc.srcTable.RelTuples, tc.srcTable.Schema, tc.srcTable.Name)

	query := fmt.Sprintf("%v\nSET client_encoding = '%s';", tc.queryManager.GetSessionSetupQuery(tc.destConn),
		encodingGuc.ClientEncoding)
	gplog.Debug("[Worker %v] Executing setup query: %v", tc.workerID, query)

	if _, err := tc.destConn.Exec(query, tc.workerID); err != nil {
		return false, err
	}

	gplog.Debug("[Worker %v] Starting transaction on destination database", tc.workerID)
	if err := tc.destConn.Begin(tc.workerID); err != nil {
		return true, err
	}

	if config.GetTableMode() == option.TableModeTruncate {
		gplog.Debug("[Worker %v] Truncating table \"%v.%v\"", tc.workerID, tc.destTable.Schema, tc.destTable.Name)
		_, err := tc.destConn.Exec("TRUNCATE TABLE "+tc.destTable.Schema+"."+tc.destTable.Name, tc.workerID)
		if err != nil {
			return true, err
		}

		gplog.Debug("[Worker %v] Finished truncating table \"%v.%v\"", tc.workerID, tc.destTable.Schema, tc.destTable.Name)
	}

	return true, nil
}

func (tc *TableCopier) shouldSkipCopy() bool {
	gplog.Debug("[Worker %v] Executing isEmptyTable \"%v.%v\" on source database",
		tc.workerID, tc.srcTable.Schema, tc.srcTable.Name)

	isEmpty, err := tc.queryManager.IsEmptyTable(tc.srcConn, tc.srcTable.Schema, tc.srcTable.Name, tc.workerID)
	if err != nil {
		gplog.Error("[Worker %v] Failed to execute isEmptyTable(): %v", tc.workerID, err)
		return false
	}

	if !isEmpty {
		gplog.Debug("[Worker %v] Source table \"%v.%v\" is not empty",
			tc.workerID, tc.srcTable.Schema, tc.srcTable.Name)
		return false
	}

	gplog.Debug("[Worker %v] Source table \"%v.%v\" is empty",
		tc.workerID, tc.srcTable.Schema, tc.srcTable.Name)
	return true
}

func (tc *TableCopier) cleanupAfterCopy(isSkipped bool, inTxn bool, err error) {
	tc.progressBar.Increment()

	tablePath := fmt.Sprintf("\"%v.%v\"", tc.srcTable.Schema, tc.srcTable.Name)

	if isSkipped {
		tc.copiedMap[tablePath] = COPY_SKIPED
		utils.WriteDataFile(fSkipped, tc.srcConn.DBName+"."+tc.srcTable.Schema+"."+tc.srcTable.Name+"\n")
		gplog.Debug("[Worker %v] Skipped copying table %v: table is empty", tc.workerID, tablePath)
		return
	}

	if err != nil {
		tc.copiedMap[tablePath] = COPY_FAILED
		utils.WriteDataFile(fFailed, tc.srcConn.DBName+"."+tc.srcTable.Schema+"."+tc.srcTable.Name+"\n")
		gplog.Error("[Worker %v] Failed to copy table %v: %v", tc.workerID, tablePath, err)

		tc.rollback(inTxn)
		return
	}

	tc.copiedMap[tablePath] = COPY_SUCCED
	utils.WriteDataFile(fCopySucced, tc.srcConn.DBName+"."+tc.srcTable.Schema+"."+tc.srcTable.Name+"\n")
	gplog.Debug("[Worker %v] Successfully copied table %v", tc.workerID, tablePath)
}

func (tc *TableCopier) copyData() error {
	command := CreateCopyStrategy(tc.srcTable.RelTuples, tc.workerID, srcSegmentsHostInfo, destSegmentsIpInfo, tc.srcConn, tc.destConn)
	copyOp := NewCopyOperation(command, tc.srcConn, tc.destConn, tc.srcTable, tc.destTable, tc.workerID)
	return copyOp.Execute(timestamp)
}

func (tc *TableCopier) rollback(inTxn bool) {
	if inTxn {
		tc.destConn.Rollback(tc.workerID)
	}
}

type CopyManager struct {
	srcConn     *dbconn.DBConn
	destConn    *dbconn.DBConn
	progressBar utils.ProgressBar
	results     []map[string]int
}

func NewCopyManager(src, dest *dbconn.DBConn, progressBar utils.ProgressBar) *CopyManager {
	manager := &CopyManager{
		srcConn:     src,
		destConn:    dest,
		progressBar: progressBar,
		results:     make([]map[string]int, src.NumConns),
	}

	// Initialize maps for each worker
	for i := 0; i < src.NumConns; i++ {
		manager.results[i] = make(map[string]int)
	}

	return manager
}

func (m *CopyManager) Copy(tables chan option.TablePair) {
	var wg sync.WaitGroup

	gplog.Debug("Copying selected tables from database \"%v => %v\"",
		m.srcConn.DBName, m.destConn.DBName)

	for i := 0; i < m.srcConn.NumConns; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			m.worker(workerID, tables)
		}(i)
	}

	if !config.ContainsMetadata(utils.MustGetFlagBool(option.METADATA_ONLY), utils.MustGetFlagBool(option.DATA_ONLY)) ||
		len(config.GetDestTables()) > 0 {
		close(tables)
	}

	wg.Wait()

	m.generateReport()
}

func (m *CopyManager) worker(workerID int, tables chan option.TablePair) {
	for table := range tables {
		if utils.WasTerminated {
			return
		}

		copier := NewTableCopier(
			m.srcConn,
			m.destConn,
			table.SrcTable,
			table.DestTable,
			workerID,
			m.progressBar,
			m.results[workerID],
		)

		copier.Copy()
	}
}

func (m *CopyManager) generateReport() {
	succedTabs := 0
	failedTabs := 0
	skipedTabs := 0

	for _, m := range m.results {
		for _, v := range m {
			switch v {
			case COPY_SUCCED:
				succedTabs++
			case COPY_SKIPED:
				skipedTabs++
			case COPY_FAILED:
				failedTabs++
			}
		}
	}

	gplog.Info("Database %v: successfully copied %v tables, skipped %v tables, failed %v tables",
		m.srcConn.DBName, succedTabs, skipedTabs, failedTabs)
}
