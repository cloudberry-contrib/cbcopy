package copy

import (
	"os"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/meta"
	"github.com/cloudberrydb/cbcopy/options"
	"github.com/cloudberrydb/cbcopy/utils"
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
	StatCountFileName  = "cbcopy_statcount"
	StatEmptyFileName  = "cbcopy_statempty"
	StatSkewFileName   = "cbcopy_statskew"
)

const (
	CbcopyTestTable = "public.cbcopy_test"
)

/*
 * Non-flag variables
 */
var (
	srcManageConn  *dbconn.DBConn
	destManageConn *dbconn.DBConn

	objectCounts        map[string]int
	option              *options.Options
	destSegmentsIpInfo  []utils.SegmentIpInfo
	srcSegmentsHostInfo []utils.SegmentHostInfo
	timestamp           string
	convertDDL          bool
	excludedDestDb      = []string{"postgres", "template0", "template1"}
	excludedSourceDb    = []string{"template0", "template1"}
	fCopySucced         *os.File
	fFailed             *os.File
	fSkipped            *os.File
	fStatCount          *os.File
	fStatEmpty          *os.File
	fStatSkew           *os.File
	killed              bool
	metaOps             meta.MetaOperator
	encodingGuc         SessionGUCs
	srcPartLeafTable    []PartLeafTable
	destPartLeafTable   []PartLeafTable
	applicationName     string
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
