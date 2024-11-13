package copy

import (
	"context"
	"time"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// CopyOperation encapsulates the state for a copy operation
type CopyOperation struct {
	command        CopyCommand
	srcConn        *dbconn.DBConn
	destConn       *dbconn.DBConn
	destManageConn *dbconn.DBConn
	srcTable       option.Table
	destTable      option.Table
	connNum        int
	cmdID          string
	ctx            context.Context
	cancel         context.CancelFunc
}

// NewCopyOperation creates a new CopyOperation instance
func NewCopyOperation(command CopyCommand, srcConn, destConn, destManageConn *dbconn.DBConn,
	srcTable, destTable option.Table, connNum int) *CopyOperation {

	ctx, cancel := context.WithCancel(context.Background())
	return &CopyOperation{
		command:        command,
		srcConn:        srcConn,
		destConn:       destConn,
		destManageConn: destManageConn,
		srcTable:       srcTable,
		destTable:      destTable,
		connNum:        connNum,
		cmdID:          uuid.NewV4().String(),
		ctx:            ctx,
		cancel:         cancel,
	}
}

// executeCopyFrom handles the copy from operation
func (op *CopyOperation) executeCopyFrom(donec chan struct{}, fromRows *int64, copyErr *error) {
	defer close(donec)

	rows, err := op.command.CopyFrom(op.destConn, op.ctx, op.destTable, op.cmdID)
	if err != nil {
		*copyErr = err
		return
	}
	*fromRows = rows
}

// waitForHelperPorts waits for and retrieves helper port information
func (op *CopyOperation) waitForHelperPorts(timestamp string, donec chan struct{}, copyErr *error) ([]HelperPortInfo, error) {
	const maxRetries = 1000
	const retryInterval = 500 * time.Millisecond

	ph := NewPortHelper(op.destManageConn)

	for i := 0; i < maxRetries; i++ {
		time.Sleep(retryInterval)
		if *copyErr != nil {
			<-donec
			return nil, *copyErr
		}

		helperPorts, err := ph.GetHelperPortList(timestamp, op.cmdID, op.connNum, op.command.IsMasterCopy())
		if err != nil {
			gplog.Debug("[Worker %v] Failed to retrieve dest segments port info: %v",
				op.connNum, err)
			continue
		}

		if op.command.IsCopyFromStarted(int64(len(helperPorts))) {
			gplog.Debug("[Worker %v] Retried %v times to get segment helpers' ports, got %v items",
				op.connNum, i+1, len(helperPorts))
			return helperPorts, nil
		}
	}

	return nil, errors.New("max retries exceeded while waiting for helper ports")
}

// validateRowCounts validates that source and destination row counts match
func (op *CopyOperation) validateRowCounts(totalFromRows, totalToRows int64) error {
	if !utils.MustGetFlagBool(option.VALIDATE) {
		return nil
	}

	if totalFromRows != totalToRows {
		return errors.Errorf(
			"Copy to affected rows %v are not equal to copy from affected rows %v",
			totalToRows, totalFromRows)
	}
	return nil
}

// logCopyResults logs the results of the copy operation
func (op *CopyOperation) logCopyResults(totalToRows, totalFromRows int64) {
	gplog.Debug("[Worker %v] source \"%v\".\"%v\" affected rows %v, dest \"%v\".\"%v\" affected rows %v",
		op.connNum, op.srcTable.Schema, op.srcTable.Name, totalToRows,
		op.destTable.Schema, op.destTable.Name, totalFromRows)
}

// Execute performs the copy operation
func (op *CopyOperation) Execute(timestamp string) error {
	var fromRows int64
	var copyErr error
	donec := make(chan struct{})

	// Start copy from operation in a separate goroutine
	go op.executeCopyFrom(donec, &fromRows, &copyErr)

	// Wait for helper ports, checking for CopyFrom errors during wait
	helperPorts, err := op.waitForHelperPorts(timestamp, donec, &copyErr)
	if err != nil {
		return op.handleFailure(donec, err, copyErr)
	}

	// Execute copy to operation
	totalToRows, err := op.command.CopyTo(op.srcConn, op.srcTable, helperPorts, op.cmdID)
	if err != nil {
		return op.handleFailure(donec, err, copyErr)
	}

	<-donec
	if copyErr != nil {
		return copyErr
	}

	// Validate row counts
	if err := op.validateRowCounts(fromRows, totalToRows); err != nil {
		return err
	}

	op.logCopyResults(totalToRows, fromRows)
	return nil
}

// handleFailure handles any failures during the copy operation
func (op *CopyOperation) handleFailure(donec chan struct{}, toErr error, fromErr error) error {
	gplog.Debug("[Worker %v] Failed to copy %v.%v to %v.%v : %v",
		op.connNum, op.srcTable.Schema, op.srcTable.Name,
		op.destTable.Schema, op.destTable.Name, toErr)
	time.Sleep(2 * time.Second)
	if fromErr != nil {
		<-donec
		return fromErr
	}

	gplog.Debug("[Worker %v] Cancel copy from for \"%v\".\"%v\"",
		op.connNum, op.destTable.Schema, op.destTable.Name)
	op.cancel()
	return toErr
}
