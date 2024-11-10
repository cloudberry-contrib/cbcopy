package copy

import (
	"fmt"
	"sync"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

func doStatisticsTasks(conn *dbconn.DBConn,
	tasks chan options.TablePair,
	progressBar utils.ProgressBar) {
	gplog.Debug("Collecting database \"%v\" statistics", conn.DBName)

	workerPool := sync.WaitGroup{}

	for i := 0; i < conn.NumConns; i++ {
		workerPool.Add(1)
		go func(whichConn int) {
			defer workerPool.Done()
			for table := range tasks {
				collect(conn, table.SrcTable, whichConn, progressBar)

				if utils.WasTerminated {
					return
				}
			}
		}(i)
	}

	close(tasks)
	workerPool.Wait()

	gplog.Debug("Finished collecting database \"%v\" statistics", conn.DBName)
}

func collect(conn *dbconn.DBConn,
	table options.Table,
	connNum int,
	progressBar utils.ProgressBar) {

	gplog.Debug("[Worker %v] collecting table \"%v\".\"%v\" statistics",
		connNum, table.Schema, table.Name)

	countPerSeg, err := getTupleCount(conn, table.Schema, table.Name)
	if err != nil {
		gplog.Error("[Worker %v] failed to collect \"%v\".\"%v\" statistics: %v",
			connNum, table.Schema, table.Name, err)
		utils.WriteDataFile(fFailed, conn.DBName+"."+table.Schema+"."+table.Name+"\n")
		progressBar.Increment()
		return
	}

	gplog.Debug("[Worker %v] Finished collecting table", connNum)

	totalTuples := int64(0)
	for _, v := range countPerSeg {
		totalTuples += v.Count
	}

	if totalTuples == 0 {
		utils.WriteDataFile(fStatEmpty, conn.DBName+"."+table.Schema+"."+table.Name+"\n")
		progressBar.Increment()
		return
	}

	utils.WriteDataFile(fStatCount,
		fmt.Sprintf("%v.%v.%v\t%v\n", conn.DBName, table.Schema, table.Name, totalTuples))

	minTuples := countPerSeg[0].Count
	maxTuples := countPerSeg[len(countPerSeg)-1].Count
	utils.WriteDataFile(fStatSkew,
		fmt.Sprintf("%v.%v.%v\t%v\t%v\n",
			conn.DBName, table.Schema, table.Name, minTuples, maxTuples))

	progressBar.Increment()
}
