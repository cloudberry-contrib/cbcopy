package builtin

/*
 * This file contains functions related to executing multiple SQL statements in parallel.
 */

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/meta/builtin/toc"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

var (
	mutex = &sync.Mutex{}
)

func executeStatementsForConn(conn *dbconn.DBConn, statements chan toc.StatementWithType, fatalErr *error, numErrors *int32, progressBar utils.ProgressBar, whichConn int, executeInParallel bool) {
	for statement := range statements {
		if *fatalErr != nil {
			return
		}
		gplog.Debug("executeStatementsForConn, statement is:\n\t %v", statement.Statement)
		_, err := conn.Exec(statement.Statement, whichConn)
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			gplog.Verbose("Error encountered when executing statement: %s Error was: %s", strings.TrimSpace(statement.Statement), err.Error())
			if executeInParallel {
				atomic.AddInt32(numErrors, 1)
				mutex.Lock()
				errorTablesMetadata[statement.Schema+"."+statement.Name] = Empty{}
				mutex.Unlock()
			} else {
				*numErrors = *numErrors + 1
				errorTablesMetadata[statement.Schema+"."+statement.Name] = Empty{}
			}
		}
		progressBar.Increment()
	}
}

/*
 * This function creates a worker pool of N goroutines to be able to execute up
 * to N statements in parallel.
 */
func ExecuteStatements(conn *dbconn.DBConn, statements []toc.StatementWithType, progressBar utils.ProgressBar, executeInParallel bool, whichConn ...int) {
	var workerPool sync.WaitGroup
	var fatalErr error
	var numErrors int32
	tasks := make(chan toc.StatementWithType, len(statements))
	for _, statement := range statements {
		tasks <- statement
	}
	close(tasks)

	gplog.Debug("ExecuteStatements")
	if !executeInParallel {
		connNum := conn.ValidateConnNum(whichConn...)
		executeStatementsForConn(conn, tasks, &fatalErr, &numErrors, progressBar, connNum, executeInParallel)
	} else {
		for i := 0; i < conn.NumConns; i++ {
			workerPool.Add(1)
			go func(connNum int) {
				defer workerPool.Done()
				connNum = conn.ValidateConnNum(connNum)
				executeStatementsForConn(conn, tasks, &fatalErr, &numErrors, progressBar, connNum, executeInParallel)
			}(i)
		}
		workerPool.Wait()
	}
	if fatalErr != nil {
		fmt.Println("")
		gplog.Fatal(fatalErr, "")
	} else if numErrors > 0 {
		fmt.Println("")
		gplog.Error("Encountered %d errors during metadata restore; see log file %s for a list of failed statements.", numErrors, gplog.GetLogFilePath())
	}
}

func ExecuteStatementsAndCreateProgressBar(conn *dbconn.DBConn, statements []toc.StatementWithType, objectsTitle string, showProgressBar int, executeInParallel bool, whichConn ...int) {
	progressBar := utils.NewProgressBar(len(statements), fmt.Sprintf("%s restored: ", objectsTitle), showProgressBar)
	progressBar.Start()
	ExecuteStatements(conn, statements, progressBar, executeInParallel, whichConn...)
	progressBar.Finish()
}

/*
 *   There is an existing bug in Greenplum where creating indexes in parallel
 *   on an AO table that didn't have any indexes previously can cause
 *   deadlock.
 *
 *   We work around this issue by restoring post data objects in
 *   two batches. The first batch takes one index from each table and
 *   restores them in parallel (which has no possibility of deadlock) and
 *   then the second restores all other postdata objects in parallel. After
 *   each table has at least one index, there is no more risk of deadlock.
 */
func BatchPostdataStatements(statements []toc.StatementWithType) ([]toc.StatementWithType, []toc.StatementWithType, []toc.StatementWithType) {
	indexMap := make(map[string]bool)
	firstBatch := make([]toc.StatementWithType, 0)
	secondBatch := make([]toc.StatementWithType, 0)
	thirdBatch := make([]toc.StatementWithType, 0)
	for _, statement := range statements {
		_, tableIndexPresent := indexMap[statement.ReferenceObject]
		if statement.ObjectType == "INDEX" && !tableIndexPresent {
			indexMap[statement.ReferenceObject] = true
			firstBatch = append(firstBatch, statement)
		} else if strings.Contains(statement.ObjectType, " METADATA") { // https://github.com/greenplum-db/gpbackup/commit/4f79e0e68156852ba903b3f875c9d4a882feb7ba
			thirdBatch = append(thirdBatch, statement)
		} else {
			secondBatch = append(secondBatch, statement)
		}
	}
	return firstBatch, secondBatch, thirdBatch
}
