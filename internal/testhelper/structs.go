package testhelper

/*
 * This file contains test structs for dependency injection and functions on those structs.
 */

import (
	// "github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/cloudberry-contrib/cbcopy/internal/cluster"

	"github.com/apache/cloudberry-go-libs/gplog"
	"github.com/jmoiron/sqlx"
)

type TestDriver struct {
	ErrToReturn  error
	ErrsToReturn []error
	DB           *sqlx.DB
	DBName       string
	User         string
	CallNumber   int
}

func (driver *TestDriver) Connect(driverName string, dataSourceName string) (*sqlx.DB, error) {
	if driver.ErrsToReturn != nil && driver.CallNumber < len(driver.ErrsToReturn) {
		// Return the errors in the order specified until we run out of specified errors, then return normally
		err := driver.ErrsToReturn[driver.CallNumber]
		driver.CallNumber++
		return nil, err
	} else if driver.ErrToReturn != nil {
		return nil, driver.ErrToReturn
	}
	return driver.DB, nil
}

type TestResult struct {
	Rows int64
}

func (result TestResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (result TestResult) RowsAffected() (int64, error) {
	return result.Rows, nil
}

/*
 * Each output or error type has both a plural form and a singular form.  If the plural form is set, it overrides the singular.
 * The singular form returns the same output and error on each call; the plural form returns one output per call (first element on the first call, etc.)
 * If more calls are made than there are outputs provided a Fatal error is raised, unless UseDefaultOutput or UseLastOutput is set.
 *
 * The LocalOutputs and LocalErrors arrays are "paired" in that the struct doesn't know how "normal" calls and "error" calls will be interleaved, so if
 * N calls are expected then at least N outputs and N errors must be provided; even if UseLastOutput or UseDefaultOutput is set in order to define its
 * behavior when more than N calls are made and it runs out of outputs and errors to return, the two array lengths must still be identical.
 */
type TestExecutor struct {
	LocalOutput   string
	LocalOutputs  []string
	LocalError    error
	LocalErrors   []error
	LocalCommands []string

	ClusterOutput   *cluster.RemoteOutput
	ClusterOutputs  []*cluster.RemoteOutput
	ClusterCommands [][]cluster.ShellCommand

	ErrorOnExecNum       int // Return LocalError after this many calls of ExecuteLocalCommand (0 means always return error); has no effect for ExecuteClusterCommand
	NumExecutions        int // Total of NumLocalExecutions and NumClusterExecutions, for convenience and backwards compatibility
	NumLocalExecutions   int
	NumClusterExecutions int
	UseLastOutput        bool // If we run out of LocalOutputs/LocalErrors or ClusterOutputs, default to the final items in those arrays
	UseDefaultOutput     bool // If we run out of LocalOutputs/LocalErrors or ClusterOutputs, default to LocalOutput/LocalError or ClusterOutput
}

func (executor *TestExecutor) ExecuteLocalCommand(commandStr string) (string, error) {
	executor.NumExecutions++
	executor.NumLocalExecutions++
	executor.LocalCommands = append(executor.LocalCommands, commandStr)
	if (executor.LocalOutputs == nil && executor.LocalErrors != nil) || (executor.LocalOutputs != nil && executor.LocalErrors == nil) {
		gplog.Fatal(nil, "If one of LocalOutputs or LocalErrors is set, both must be set")
	} else if executor.LocalOutputs != nil && executor.LocalErrors != nil && len(executor.LocalOutputs) != len(executor.LocalErrors) {
		gplog.Fatal(nil, "Found %d LocalOutputs and %d LocalErrors, but one output and one error must be set for each call", len(executor.LocalOutputs), len(executor.LocalErrors))
	}
	if executor.LocalOutputs != nil {
		if executor.NumLocalExecutions <= len(executor.LocalOutputs) {
			return executor.LocalOutputs[executor.NumLocalExecutions-1], executor.LocalErrors[executor.NumLocalExecutions-1]
		} else if executor.UseLastOutput {
			return executor.LocalOutputs[len(executor.LocalOutputs)-1], executor.LocalErrors[len(executor.LocalErrors)-1]
		} else if executor.UseDefaultOutput {
			return executor.LocalOutput, executor.LocalError
		}
		gplog.Fatal(nil, "ExecuteLocalCommand called %d times, but only %d outputs and errors provided", executor.NumLocalExecutions, len(executor.LocalOutputs))
	} else if executor.ErrorOnExecNum == 0 || executor.NumLocalExecutions == executor.ErrorOnExecNum {
		return executor.LocalOutput, executor.LocalError
	}
	return executor.LocalOutput, nil
}

func (executor *TestExecutor) ExecuteClusterCommand(scope cluster.Scope, commandList []cluster.ShellCommand) *cluster.RemoteOutput {
	executor.NumExecutions++
	executor.NumClusterExecutions++
	executor.ClusterCommands = append(executor.ClusterCommands, commandList)
	if executor.ClusterOutputs != nil {
		if executor.NumClusterExecutions <= len(executor.ClusterOutputs) {
			return executor.ClusterOutputs[executor.NumClusterExecutions-1]
		} else if executor.UseLastOutput {
			return executor.ClusterOutputs[len(executor.ClusterOutputs)-1]
		} else if executor.UseDefaultOutput {
			return executor.ClusterOutput
		}
		gplog.Fatal(nil, "ExecuteClusterCommand called %d times, but only %d ClusterOutputs provided", executor.NumClusterExecutions, len(executor.ClusterOutputs))
	}
	return executor.ClusterOutput
}
