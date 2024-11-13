package copy

import (
	"fmt"
	"os"
	"sync"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
)

type TableCopier struct {
	manager      *CopyManager
	srcTable     option.Table
	destTable    option.Table
	workerID     int
	copiedMap    map[string]int
	queryManager *QueryManager
}

func NewTableCopier(manager *CopyManager,
	srcTable, destTable option.Table,
	workerID int,
	copiedMap map[string]int) *TableCopier {
	return &TableCopier{
		manager:      manager,
		srcTable:     srcTable,
		destTable:    destTable,
		workerID:     workerID,
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
	err = tc.manager.destConn.Commit(tc.workerID)
}

func (tc *TableCopier) prepareForCopy() (bool, error) {
	gplog.Debug("[Worker %v] There are %v rows in the source table \"%v.%v\"",
		tc.workerID, tc.srcTable.RelTuples, tc.srcTable.Schema, tc.srcTable.Name)

	query := fmt.Sprintf("%v\nSET client_encoding = '%s';", tc.queryManager.GetSessionSetupQuery(
		tc.manager.destConn, tc.manager.appName),
		tc.manager.encodingGuc.ClientEncoding)
	gplog.Debug("[Worker %v] Executing setup query: %v", tc.workerID, query)

	if _, err := tc.manager.destConn.Exec(query, tc.workerID); err != nil {
		return false, err
	}

	gplog.Debug("[Worker %v] Starting transaction on destination database", tc.workerID)
	if err := tc.manager.destConn.Begin(tc.workerID); err != nil {
		return true, err
	}

	if config.GetTableMode() == option.TableModeTruncate {
		gplog.Debug("[Worker %v] Truncating table \"%v.%v\"", tc.workerID, tc.destTable.Schema, tc.destTable.Name)
		_, err := tc.manager.destConn.Exec("TRUNCATE TABLE "+tc.destTable.Schema+"."+tc.destTable.Name, tc.workerID)
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

	isEmpty, err := tc.queryManager.IsEmptyTable(tc.manager.srcConn, tc.srcTable.Schema, tc.srcTable.Name, tc.workerID)
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
	tc.manager.progressBar.Increment()

	tablePath := fmt.Sprintf("\"%v.%v\"", tc.srcTable.Schema, tc.srcTable.Name)

	if isSkipped {
		tc.copiedMap[tablePath] = COPY_SKIPED
		utils.WriteDataFile(tc.manager.fSkipped, tc.manager.srcConn.DBName+"."+tc.srcTable.Schema+"."+tc.srcTable.Name+"\n")
		gplog.Debug("[Worker %v] Skipped copying table %v: table is empty", tc.workerID, tablePath)
		return
	}

	if err != nil {
		tc.copiedMap[tablePath] = COPY_FAILED
		utils.WriteDataFile(tc.manager.fFailed, tc.manager.srcConn.DBName+"."+tc.srcTable.Schema+"."+tc.srcTable.Name+"\n")
		gplog.Error("[Worker %v] Failed to copy table %v: %v", tc.workerID, tablePath, err)

		tc.rollback(inTxn)
		return
	}

	tc.copiedMap[tablePath] = COPY_SUCCED
	utils.WriteDataFile(tc.manager.fSucced, tc.manager.srcConn.DBName+"."+tc.srcTable.Schema+"."+tc.srcTable.Name+"\n")
	gplog.Debug("[Worker %v] Successfully copied table %v", tc.workerID, tablePath)
}

func (tc *TableCopier) copyData() error {
	command := CreateCopyStrategy(tc.srcTable.RelTuples,
		tc.workerID,
		tc.manager.srcSegmentsHostInfo,
		tc.manager.destSegmentsIpInfo,
		tc.manager.srcConn,
		tc.manager.destConn)
	copyOp := NewCopyOperation(command,
		tc.manager.srcConn,
		tc.manager.destConn,
		tc.manager.destManageConn,
		tc.srcTable,
		tc.destTable,
		tc.workerID)

	return copyOp.Execute(tc.manager.timestamp)
}

func (tc *TableCopier) rollback(inTxn bool) {
	if inTxn {
		tc.manager.destConn.Rollback(tc.workerID)
	}
}

type CopyManager struct {
	srcConn             *dbconn.DBConn
	destConn            *dbconn.DBConn
	destManageConn      *dbconn.DBConn
	srcSegmentsHostInfo []utils.SegmentHostInfo
	destSegmentsIpInfo  []utils.SegmentIpInfo
	timestamp           string
	appName             string
	encodingGuc         *SessionGUCs
	progressBar         utils.ProgressBar
	results             []map[string]int
	fSucced             *os.File
	fFailed             *os.File
	fSkipped            *os.File
}

func NewCopyManager(src, dest, destManageConn *dbconn.DBConn,
	srcSegmentsHostInfo []utils.SegmentHostInfo,
	destSegmentsIpInfo []utils.SegmentIpInfo,
	timestamp string,
	appName string,
	encodingGuc *SessionGUCs,
	progressBar utils.ProgressBar) *CopyManager {

	currentUser, _ := operating.System.CurrentUser()

	manager := &CopyManager{
		srcConn:             src,
		destConn:            dest,
		destManageConn:      destManageConn,
		srcSegmentsHostInfo: srcSegmentsHostInfo,
		destSegmentsIpInfo:  destSegmentsIpInfo,
		timestamp:           timestamp,
		appName:             appName,
		encodingGuc:         encodingGuc,
		progressBar:         progressBar,
		results:             make([]map[string]int, src.NumConns),
		fSucced: utils.OpenDataFile(fmt.Sprintf("%s/gpAdminLogs/%v_%v",
			currentUser.HomeDir, CopySuccedFileName, timestamp)),
		fFailed: utils.OpenDataFile(fmt.Sprintf("%s/gpAdminLogs/%v_%v",
			currentUser.HomeDir, FailedFileName, timestamp)),
		fSkipped: utils.OpenDataFile(fmt.Sprintf("%s/gpAdminLogs/%v_%v",
			currentUser.HomeDir, SkippedFileName, timestamp)),
	}

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
			m,
			table.SrcTable,
			table.DestTable,
			workerID,
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

func (m *CopyManager) Close() {
	if m.srcConn != nil {
		m.srcConn.Close()
	}
	if m.destConn != nil {
		m.destConn.Close()
	}

	utils.CloseDataFile(m.fFailed)
	utils.CloseDataFile(m.fSucced)
	utils.CloseDataFile(m.fSkipped)
}
