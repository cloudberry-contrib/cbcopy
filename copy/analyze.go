package copy

import (
	"sync"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

func doAnalyzeTasks(conn *dbconn.DBConn, tables []options.Table) {
	gplog.Info("Analyzing dest database \"%v\"", conn.DBName)
	gplog.Info("Analyzing selected tables from database \"%v\"", conn.DBName)
	tasks := make(chan options.Table, len(tables))
	workerPool := sync.WaitGroup{}

	for i := 0; i < conn.NumConns; i++ {
		workerPool.Add(1)
		go func(whichConn int) {
			defer workerPool.Done()
			for table := range tasks {
				analyzeTable(conn, table, whichConn)

				if utils.WasTerminated {
					return
				}
			}
		}(i)
	}

	for _, table := range tables {
		tasks <- table
	}
	close(tasks)

	workerPool.Wait()
	gplog.Info("Finished analyzing database \"%v\"", conn.DBName)
}

func analyzeTable(conn *dbconn.DBConn, table options.Table, connNum int) {
	gplog.Info("[Worker %v] Analyzing table \"%v\".\"%v\"", connNum, table.Schema, table.Name)
	query := "ANALYZE " + table.Schema + "." + table.Name
	_, err := conn.Exec(query, connNum)
	if err != nil {
		gplog.Error("[Worker %v] Failed to analyze \"%v\".\"%v\" : %v", connNum, table.Schema, table.Name, err)
	}

	gplog.Info("[Worker %v] Finished copying table \"%v\".\"%v\"", connNum, table.Schema, table.Name)
}
