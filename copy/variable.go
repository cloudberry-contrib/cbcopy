package copy

import (
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
	config     *option.Option
	excludedDb = []string{"template0", "template1"}
)
