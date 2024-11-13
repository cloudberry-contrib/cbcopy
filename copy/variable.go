package copy

import (
	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/option"
)

/*
 * This file contains global variables and setter functions for those variables
 * used in testing.
 */

const (
	COPY_SUCCED = iota
	COPY_SKIPED
	COPY_FAILED
)

const (
	CopySuccedFileName = "cbcopy_succeed"
	FailedFileName     = "cbcopy_failed"
	SkippedFileName    = "cbcopy_skipped"
)

const (
	CbcopyTestTable = "public.cbcopy_test"
)

/*
 * Non-flag variables
 */
var (
	objectCounts map[string]int

	config     *option.Option
	excludedDb = []string{"template0", "template1"}
)

func isSameVersion(srcVersion, destVersion dbconn.GPDBVersion) bool {
	if srcVersion.Is("4") && destVersion.Is("4") {
		return true
	} else if srcVersion.Is("5") && destVersion.Is("5") {
		return true
	} else if srcVersion.Is("6") && destVersion.Is("6") {
		return true
	} else if srcVersion.Is("2") && destVersion.Is("2") {
		return true
	} else if srcVersion.Is("3") && destVersion.Is("3") {
		return true
	} else {
		return false
	}
}
