package builtin

/*
 * This file contains functions related to executing multiple SQL statements in parallel.
 */

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/meta/builtin/toc"
	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

type RunningInfo struct {
	statement toc.StatementWithType
	taskid    int
}

var (
	errMutex     = &sync.Mutex{}
	taskMutex    = &sync.Mutex{}
	tabMapMutex  = &sync.Mutex{}
	runningTasks map[int]RunningInfo
)

func lookupSourceTables(schema, name string, partNameMap map[string][]string, tabMap map[string]string) []option.TablePair {
	// gplog.Debug("-- debug, lookupSourceTables, schema: %v, name: %v, partNameMap: %v, tabMap: %v", schema, name, partNameMap, tabMap)
	result := make([]option.TablePair, 0)
	destFqn := schema + "." + name
	leafTables, found := partNameMap[destFqn]

	/**
	(workaround) for GP7, partitioned table and leaf table are created separately, so for partitioned table, let's just create, no data copy,
	e.g.
		db1=# \d
										List of relations
		 Schema |       Name        |       Type        |  Owner  |       Storage
		--------+-------------------+-------------------+---------+----------------------
		 public | t3                | table             | gpadmin | heap
		 public | test2             | partitioned table | gpadmin |
		 public | test2_1_prt_l1def | table             | gpadmin | append only columnar
		 public | test2_1_prt_l1p1  | table             | gpadmin | append only columnar
		(4 rows)

		dropdb db1_copy; cbcopy  --source-host localhost  --dest-host localhost --dbname=db1 --dest-dbname=db1_copy --truncate --metadata-jobs 1 --copy-jobs 1;

		in meta file
			CREATE TABLE public.test2
			CREATE TABLE public.t3
			CREATE TABLE public.test2_1_prt_l1def
			CREATE TABLE public.test2_1_prt_l1p1

		the debug output is:
		--- debug, lookupSourceTables, schema: public, name: test2, partNameMap: map[public.test2:[public.test2_1_prt_l1def public.test2_1_prt_l1p1]], tabMap: map[public.t3:public.t3.-1 public.test2_1_prt_l1def:public.test2_1_prt_l1def.-1 public.test2_1_prt_l1p1:public.test2_1_prt_l1p1.-1]

	before this workaround code change ("if 7, then found = false"), we would start data copy on leaf table once root table is created, but at this time leaf table is not created yet, then error would happen.

	todo: add some more detail description on variables value (partNameMap, tabMap, etc)
	*/
	if gpdbVersion.AtLeast("7") {
		found = false
	}

	if !found {
		exist, tp := formTablePair(schema, name, tabMap)
		if exist {
			result = append(result, tp)
		}
		return result
	}

	for _, l := range leafTables {
		sl := strings.Split(l, ".")
		exist, tp := formTablePair(sl[0], sl[1], tabMap)
		if exist {
			result = append(result, tp)
		}
	}

	return result
}

func formTablePair(schema, name string, tabMap map[string]string) (bool, option.TablePair) {
	destFqn := schema + "." + name

	tabMapMutex.Lock()
	defer tabMapMutex.Unlock()

	if _, exist := tabMap[destFqn]; !exist {
		return false, option.TablePair{}
	}

	srcFqn := tabMap[destFqn]
	sl := strings.Split(srcFqn, ".")
	relTuples, _ := strconv.ParseInt(sl[2], 10, 64)

	delete(tabMap, destFqn)

	return true, option.TablePair{SrcTable: option.Table{Schema: sl[0], Name: sl[1], RelTuples: relTuples}, DestTable: option.Table{Schema: schema, Name: name}}
}

func executeStatementsForDependentObject(conn *dbconn.DBConn, runningInfos chan RunningInfo, numErrors *int32, progressBar utils.ProgressBar, whichConn int, partNameMap map[string][]string, tabMap map[string]string, tablec chan option.TablePair) {
	// gplog.Debug("-- debug, executeStatementsForDependentObject, partNameMap: %v, tabMap: %v", partNameMap, tabMap)
	for info := range runningInfos {
		gplog.Debug("executeStatementsForDependentObject, query is %v", info.statement.Statement)
		_, err := conn.Exec(info.statement.Statement, whichConn)
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			gplog.Error("[Worker %v][conn %v] Error encountered when executing statement: %s \nError was: %s", whichConn, conn, strings.TrimSpace(info.statement.Statement), err.Error())

			atomic.AddInt32(numErrors, 1)
			errMutex.Lock()
			errorTablesMetadata[info.statement.Schema+"."+info.statement.Name] = Empty{}
			errMutex.Unlock()
		}
		progressBar.Increment()

		if info.statement.ObjectType == "TABLE" {
			tps := lookupSourceTables(info.statement.Schema, info.statement.Name, partNameMap, tabMap)
			for _, tp := range tps {
				// the DDL and DML are done in parallel, once a table is created, it would add into channel, then DML (doCopyTasks) would start on it
				gplog.Debug("executeStatementsForDependentObject, add one to chan: %v", tp)
				tablec <- tp
			}
		}

		removeRunningTask(info.taskid)
	}
}

func isDependentStatementRunning(r RunningInfo, depMap map[UniqueID]bool) bool {
	uid := UniqueID{
		ClassID: r.statement.ClassOID,
		Oid:     r.statement.Oid,
	}

	if _, exist := depMap[uid]; exist {
		return true
	}

	return false
}

func shouldScheduleStatement(statement toc.StatementWithType, numTasks int) bool {
	uid := UniqueID{
		ClassID: statement.ClassOID,
		Oid:     statement.Oid,
	}
	depMap := relevantDeps[uid]

	rts := getRunningTasks()
	numRunningTask := len(rts)

	for _, t := range rts {
		if statement.ClassOID == t.statement.ClassOID && statement.Oid == t.statement.Oid {
			return false
		}

		if isDependentStatementRunning(t, depMap) {
			return false
		}
	}

	return numRunningTask < numTasks
}

func scheduleStatements(tasks chan RunningInfo, statements []toc.StatementWithType, numTasks int) {
	for i := 0; i < len(statements); i++ {
		statement := statements[i]

		// todo: 这里是不是可以优化，这里貌似是说如果这个statement所依赖的还在执行，那它一直循环在这里，即使有空闲的connection可以用于其他可以并行的。
		for {
			if shouldScheduleStatement(statement, numTasks) {
				addRunningTask(i, statement)
				tasks <- RunningInfo{statement, i}
				break
			}

			time.Sleep(50 * time.Millisecond)
		}
	}

	close(tasks)
}

func ExecuteDependentStatements(conn *dbconn.DBConn, statements []toc.StatementWithType, progressBar utils.ProgressBar, partNameMap map[string][]string, tabMap map[string]string, tablec chan option.TablePair) {
	var numErrors int32
	workerPool := sync.WaitGroup{}
	tasks := make(chan RunningInfo, len(statements))

	for i := 0; i < conn.NumConns; i++ {
		workerPool.Add(1)
		go func(connNum int) {
			defer workerPool.Done()
			connNum = conn.ValidateConnNum(connNum)
			executeStatementsForDependentObject(conn, tasks, &numErrors, progressBar, connNum, partNameMap, tabMap, tablec)
		}(i)
	}

	runningTasks = make(map[int]RunningInfo)

	scheduleStatements(tasks, statements, conn.NumConns)
	workerPool.Wait()

	if numErrors > 0 {
		gplog.Error("Encountered %d errors during metadata restore; see log file %s for a list of failed statements.", numErrors, gplog.GetLogFilePath())
	}
}

// 对于runningtasks，多个协程，并发访问

func addRunningTask(taskid int, statement toc.StatementWithType) {
	taskMutex.Lock()
	defer taskMutex.Unlock()

	runningTasks[taskid] = RunningInfo{statement, taskid}
}

func removeRunningTask(taskid int) {
	taskMutex.Lock()
	defer taskMutex.Unlock()

	delete(runningTasks, taskid)
}

func getRunningTasks() map[int]RunningInfo {
	result := make(map[int]RunningInfo)

	taskMutex.Lock()
	defer taskMutex.Unlock()
	for k, v := range runningTasks {
		result[k] = v
	}
	return result
}

func RestoreCleanup(tabMap map[string]string, tablec chan option.TablePair) {
	for k, v := range tabMap {
		ssl := strings.Split(v, ".")
		dsl := strings.Split(k, ".")
		relTuples, _ := strconv.ParseInt(ssl[2], 10, 64)

		tp := option.TablePair{SrcTable: option.Table{Schema: ssl[0],
			Name:      ssl[1],
			RelTuples: relTuples},
			DestTable: option.Table{Schema: dsl[0],
				Name: dsl[1]}}
		tablec <- tp
	}
}
