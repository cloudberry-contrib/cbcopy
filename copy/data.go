package copy

import (
	"fmt"
	"sync"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/meta/builtin"
	"github.com/cloudberrydb/cbcopy/options"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

func doCopyTasks(srcConn, destConn *dbconn.DBConn, tasks chan options.TablePair, progressBar utils.ProgressBar) []map[string]int {
	gplog.Debug("Copying source database \"%v\"", srcConn.DBName)
	gplog.Debug("Copying selected tables from database \"%v\" => \"%v\"", srcConn.DBName, destConn.DBName)

	copiedMaps := make([]map[string]int, srcConn.NumConns)
	/*
	 * We break when an interrupt is received and rely on
	 * TerminateHangingCopySessions to kill any COPY statements
	 * in progress if they don't finish on their own.
	 */
	workerPool := sync.WaitGroup{}

	for i := 0; i < srcConn.NumConns; i++ {
		copiedMaps[i] = make(map[string]int)
		workerPool.Add(1)
		go func(whichConn int) {
			defer workerPool.Done()
			for table := range tasks {
				gplog.Debug("Copying selected table: database %v => %v, get one from chan, table %v ==> %v, whichConn: %v, copiedMaps[whichConn]: %v", srcConn.DBName, destConn.DBName, table.SrcTable, table.DestTable, whichConn, copiedMaps[whichConn])
				copyBetweenCluster(srcConn, destConn, table.SrcTable, table.DestTable, copiedMaps[whichConn], whichConn, progressBar)

				if utils.WasTerminated {
					return
				}
			}
		}(i)
	}

	if !option.ContainsMetadata(utils.MustGetFlagBool(options.METADATA_ONLY),
		utils.MustGetFlagBool(options.DATA_ONLY), false) ||
		len(option.GetDestTables()) > 0 {
		close(tasks)
	}

	workerPool.Wait()
	gplog.Debug("Finished copying database \"%v\"", srcConn.DBName)
	return copiedMaps
}

func shouldSkipTransferData(conn *dbconn.DBConn,
	srcTable options.Table,
	workerId int) bool {
	gplog.Debug("[Worker %v] Executing isEmptyTable \"%v\".\"%v\" on source database",
		workerId, srcTable.Schema, srcTable.Name)
	isEmpty, err := isEmptyTable(conn, srcTable.Schema, srcTable.Name, workerId)

	gplog.Debug("[Worker %v] Finished executing isEmptyTable", workerId)

	if err != nil {
		gplog.Error("[Worker %v] Failed to execute isEmptyTable(): %v", workerId, err)
		return false
	}

	if isEmpty {
		gplog.Debug("[Worker %v] Table \"%v\".\"%v\" is empty",
			workerId, srcTable.Schema, srcTable.Name)
		return true
	}

	return false
}

func copyBetweenCluster(srcConn, destConn *dbconn.DBConn,
	srcTable, destTable options.Table,
	copiedMap map[string]int,
	connNum int,
	progressBar utils.ProgressBar) {
	var command CopyCommand

	found, reltuples := option.GetTableStatistics(srcConn.DBName, srcTable.Schema, srcTable.Name)
	if found {
		gplog.Debug("[Worker %v] Found table \"%v\".\"%v\" in statistics file, overwrite reltuples %v->%v",
			connNum, srcTable.Schema, srcTable.Name, srcTable.RelTuples, reltuples)
		srcTable.RelTuples = reltuples
	}

	gplog.Debug("[Worker %v] There are %v rows in the source table \"%v\".\"%v\"",
		connNum, srcTable.RelTuples, srcTable.Schema, srcTable.Name)

	inTxn, err := copyPreData(srcConn, destConn, srcTable, destTable, connNum)
	if err != nil {
		goto failed
	}

	gplog.Debug("[Worker %v] Transferring table \"%v\".\"%v\" => \"%v\".\"%v\"",
		connNum, srcTable.Schema, srcTable.Name, destTable.Schema, destTable.Name)

	if !shouldSkipTransferData(srcConn, srcTable, connNum) {
		command = CreateCopyStrategy(srcTable.RelTuples, connNum, srcSegmentsHostInfo, destSegmentsIpInfo, srcConn, destConn)
		err = copyForTargetTable(command, srcConn, destConn, srcTable, destTable, connNum)

		if err != nil {
			goto failed
		}

		gplog.Debug("[Worker %v] Committing changes of table \"%v\".\"%v\" => \"%v\".\"%v\"",
			connNum, srcTable.Schema, srcTable.Name, destTable.Schema, destTable.Name)

		err = destConn.Commit(connNum)
		if err != nil {
			goto failed
		}
	} else {
		gplog.Debug("[Worker %v] Committing changes of table \"%v\".\"%v\" => \"%v\".\"%v\"",
			connNum, srcTable.Schema, srcTable.Name, destTable.Schema, destTable.Name)

		err = destConn.Commit(connNum)
		if err != nil {
			goto failed
		}
	}

	copyPostData(COPY_SUCCED, connNum, inTxn, srcConn, destConn, srcTable, destTable, copiedMap, progressBar, nil)
	return

failed:
	copyPostData(COPY_FAILED, connNum, inTxn, srcConn, destConn, srcTable, destTable, copiedMap, progressBar, err)
}

func copyForTargetTable(command CopyCommand,
	srcConn, destConn *dbconn.DBConn,
	srcTable, destTable options.Table,
	connNum int) error {

	copyOp := NewCopyOperation(command, srcConn, destConn, srcTable, destTable, connNum)
	return copyOp.Execute(timestamp)
}

func copyPreData(srcConn, destConn *dbconn.DBConn, srcTable, destTable options.Table, connNum int) (bool, error) {
	query := fmt.Sprintf("%v\nSET client_encoding = '%s';", GetSetupQuery(destConn), encodingGuc.ClientEncoding)
	gplog.Debug("[Worker %v] Executing setup query: %v", connNum, query)
	_, err := destConn.Exec(query, connNum)
	if err != nil {
		return false, err
	}

	err = destConn.Begin(connNum)
	if err != nil {
		return true, err
	}

	if option.GetTableMode() == options.TableModeTruncate {
		gplog.Debug("[Worker %v] Truncating table \"%v\".\"%v\"",
			connNum, destTable.Schema, destTable.Name)
		_, err = destConn.Exec("TRUNCATE TABLE "+destTable.Schema+"."+destTable.Name, connNum)
		if err != nil {
			return true, err
		}
		gplog.Debug("[Worker %v] Finished truncating table \"%v\".\"%v\"",
			connNum, destTable.Schema, destTable.Name)
	}
	return true, nil
}

func copyPostData(result, workerId int,
	inTxn bool,
	srcConn, destConn *dbconn.DBConn,
	srcTable, destTable options.Table,
	copiedMap map[string]int,
	progressBar utils.ProgressBar,
	err error) {

	if result == COPY_SUCCED {
		gplog.Debug("[Worker %v] Finished copying table \"%v\".\"%v\" => \"%v\".\"%v\"",
			workerId, srcTable.Schema, srcTable.Name, destTable.Schema, destTable.Name)
		utils.WriteDataFile(fCopySucced, srcConn.DBName+"."+srcTable.Schema+"."+srcTable.Name+"\n")
	} else {
		gplog.Error("[Worker %v] Failed to copy table \"%v\".\"%v\" => \"%v\".\"%v\" : %v",
			workerId, srcTable.Schema, srcTable.Name, destTable.Schema, destTable.Name, err)
		utils.WriteDataFile(fFailed, srcConn.DBName+"."+srcTable.Schema+"."+srcTable.Name+"\n")
	}

	copiedMap[srcTable.Schema+"."+srcTable.Name] = result
	progressBar.Increment()

	if result == COPY_FAILED && inTxn {
		destConn.Rollback(workerId)
	}
}

func printCopyReport(srcDbname string, copiedMaps []map[string]int) {
	totalTabs := 0
	succedTabs := 0
	failedTabs := 0
	skipedTabs := 0

	for _, m := range copiedMaps {
		for _, v := range m {
			totalTabs++
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
		srcDbname, succedTabs, skipedTabs, failedTabs)
}

func writeResultFile(dbname string, otherRels []options.Table, errorRels map[string]builtin.Empty) {
	for _, t := range otherRels {
		k := t.Schema + "." + t.Name
		_, exists := errorRels[k]
		if exists {
			utils.WriteDataFile(fFailed, dbname+"."+t.Schema+"."+t.Name+"\n")
			continue
		}

		utils.WriteDataFile(fCopySucced, dbname+"."+t.Schema+"."+t.Name+"\n")
	}
}
