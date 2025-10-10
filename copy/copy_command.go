package copy

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/cloudberry-contrib/cbcopy/internal/dbconn"
	"github.com/cloudberry-contrib/cbcopy/option"
	"github.com/cloudberry-contrib/cbcopy/utils"
	"github.com/apache/cloudberry-go-libs/gplog"
	uuid "github.com/satori/go.uuid"
)

type CopyCommand interface {
	CopyTo(conn *dbconn.DBConn, table option.Table, ports []HelperPortInfo, cmdId string) (int64, error)
	CopyFrom(conn *dbconn.DBConn, ctx context.Context, table option.Table, ports []HelperPortInfo, cmdId string) (int64, error)
	IsCopyFromStarted(rows int64) bool
	IsMasterCopy() bool
}

type CopyBase struct {
	WorkerId             int
	SrcSegmentsHostInfo  []utils.SegmentHostInfo
	DestSegmentsHostInfo []utils.SegmentHostInfo
	ConnectionMode       string
	CompArg              string
}

func (cc *CopyBase) FormMasterHelperAddress(ports []HelperPortInfo) (string, string) {
	ip := utils.MustGetFlagString(option.DEST_HOST)
	port := strconv.Itoa(int(ports[0].Port))

	return port, ip
}

func (cc *CopyBase) FormAllSegsHelperAddress(ports []HelperPortInfo) (string, string) {
	ps := make([]string, 0)
	is := make([]string, 0)

	var connectTargetSegments []utils.SegmentHostInfo
	if cc.ConnectionMode == option.ConnectionModePush {
		connectTargetSegments = cc.DestSegmentsHostInfo
	} else { // ConnectionModePull
		connectTargetSegments = cc.SrcSegmentsHostInfo
	}

	for _, p := range ports {
		ps = append(ps, strconv.Itoa(int(p.Port)))
	}
	pl := strings.Join(ps, ",")

	for i := 0; i < len(ps); i++ {
		is = append(is, connectTargetSegments[i].Hostname)
	}
	il := strings.Join(is, ",")

	return pl, il
}

func (cc *CopyBase) FormDestSegsHelperAddress(ports []HelperPortInfo) (string, string) {
	ps := make([]string, 0)
	is := make([]string, 0)

	j := 0
	for i := 0; i < len(cc.SrcSegmentsHostInfo); i++ {
		ps = append(ps, strconv.Itoa(int(ports[j].Port)))
		is = append(is, cc.DestSegmentsHostInfo[j].Hostname)

		j++

		if j == len(cc.DestSegmentsHostInfo) {
			j = 0
		}
	}

	pl := strings.Join(ps, ",")
	il := strings.Join(is, ",")
	return pl, il
}

func (cc *CopyBase) FormSrcSegsHelperAddress(ports []HelperPortInfo) (string, string) {
	ps := make([]string, 0)
	is := make([]string, 0)

	for i := 0; i < len(cc.SrcSegmentsHostInfo); i++ {
		ps = append(ps, strconv.Itoa(int(ports[i].Port)))
		is = append(is, cc.SrcSegmentsHostInfo[i].Hostname)
	}

	pl := strings.Join(ps, ",")
	il := strings.Join(is, ",")
	return pl, il
}

func (cc *CopyBase) FormAllSegsIds() string {
	hs := make([]string, 0)

	for _, h := range cc.SrcSegmentsHostInfo {
		hs = append(hs, strconv.Itoa(int(h.Content)))
	}
	result := strings.Join(hs, " ")

	return result
}

func (cc *CopyBase) CommitBegin(conn *dbconn.DBConn) error {
	if err := conn.Commit(cc.WorkerId); err != nil {
		return err
	}

	if err := conn.Begin(cc.WorkerId); err != nil {
		return err
	}

	return nil
}

type CopyOnMaster struct {
	CopyBase
}

// CopyTo is part of the CopyCommand interface.
// It executes the COPY TO command to send data from the source database.
// The specific implementation varies based on the copy strategy.
func (com *CopyOnMaster) CopyTo(conn *dbconn.DBConn, table option.Table, ports []HelperPortInfo, cmdId string) (int64, error) {
	port, ip := com.FormMasterHelperAddress(ports)
	query := fmt.Sprintf(`COPY %v.%v TO PROGRAM 'cbcopy_helper %v --seg-id -1 --host %v --port %v --direction send' CSV IGNORE EXTERNAL PARTITIONS`,
		table.Schema, table.Name, com.CompArg, ip, port)

	gplog.Debug("[Worker %v] Execute on master, COPY command of sending data: %v", com.WorkerId, query)
	copied, err := conn.Exec(query, com.WorkerId)
	gplog.Debug("[Worker %v] Finished executing query", com.WorkerId)
	if err != nil {
		return 0, err
	}

	rows, _ := copied.RowsAffected()
	return rows, nil
}

// CopyFrom is part of the CopyCommand interface.
// It executes the COPY FROM command to receive data into the destination database.
// The specific implementation varies based on the copy strategy.
func (com *CopyOnMaster) CopyFrom(conn *dbconn.DBConn, ctx context.Context, table option.Table, ports []HelperPortInfo, cmdId string) (int64, error) {
	dataPortRange := utils.MustGetFlagString(option.DATA_PORT_RANGE)

	query := fmt.Sprintf(`COPY %v.%v FROM PROGRAM 'cbcopy_helper %v --listen --seg-id -1 --cmd-id %v --data-port-range %v --direction receive' CSV`,
		table.Schema, table.Name, com.CompArg, cmdId, dataPortRange)

	gplog.Debug("[Worker %v] Execute on master, COPY command of receiving data: %v", com.WorkerId, query)
	copied, err := conn.ExecContext(ctx, query, com.WorkerId)
	gplog.Debug("[Worker %v] Finished executing query", com.WorkerId)
	if err != nil {
		return 0, err
	}

	rows, _ := copied.RowsAffected()
	return rows, nil
}

func (com *CopyOnMaster) IsMasterCopy() bool {
	return true
}

func (com *CopyOnMaster) IsCopyFromStarted(rows int64) bool {
	return rows == 1
}

type CopyOnSegment struct {
	CopyBase
}

// CopyTo is the CopyOnSegment strategy's implementation of sending data.
// It uses ON SEGMENT clause to execute COPY on each segment.
func (cos *CopyOnSegment) CopyTo(conn *dbconn.DBConn, table option.Table, ports []HelperPortInfo, cmdId string) (int64, error) {
	var query string
	if cos.ConnectionMode == option.ConnectionModePull {
		dataPortRange := utils.MustGetFlagString(option.DATA_PORT_RANGE)
		query = fmt.Sprintf(`COPY %v.%v TO PROGRAM 'cbcopy_helper %v --listen --cmd-id %v --seg-id <SEGID> --data-port-range %v --direction send' ON SEGMENT CSV IGNORE EXTERNAL PARTITIONS`,
			table.Schema, table.Name, cos.CompArg, cmdId, dataPortRange)
	} else { // ConnectionModePush
		port, ip := cos.FormAllSegsHelperAddress(ports)
		query = fmt.Sprintf(`COPY %v.%v TO PROGRAM 'cbcopy_helper %v --seg-id <SEGID> --host %v --port %v --direction send' ON SEGMENT CSV IGNORE EXTERNAL PARTITIONS`,
			table.Schema, table.Name, cos.CompArg, ip, port)
	}

	gplog.Debug("[Worker %v] COPY command of sending data: %v", cos.WorkerId, query)
	copied, err := conn.Exec(query, cos.WorkerId)
	gplog.Debug("[Worker %v] Finished executing query", cos.WorkerId)
	if err != nil {
		return 0, err
	}

	rows, _ := copied.RowsAffected()
	return rows, nil
}

// CopyFrom is the CopyOnSegment strategy's implementation of receiving data.
// It uses ON SEGMENT clause to execute COPY on each segment.
func (cos *CopyOnSegment) CopyFrom(conn *dbconn.DBConn, ctx context.Context, table option.Table, ports []HelperPortInfo, cmdId string) (int64, error) {
	var query string
	if cos.ConnectionMode == option.ConnectionModePull {
		port, ip := cos.FormAllSegsHelperAddress(ports)
		query = fmt.Sprintf(`COPY %v.%v FROM PROGRAM 'cbcopy_helper %v --seg-id <SEGID> --host %s --port %s --direction receive' ON SEGMENT CSV`,
			table.Schema, table.Name, cos.CompArg, ip, port)
	} else { // ConnectionModePush
		dataPortRange := utils.MustGetFlagString(option.DATA_PORT_RANGE)
		query = fmt.Sprintf(`COPY %v.%v FROM PROGRAM 'cbcopy_helper %v --listen --seg-id <SEGID> --cmd-id %v --data-port-range %v --direction receive' ON SEGMENT CSV`,
			table.Schema, table.Name, cos.CompArg, cmdId, dataPortRange)
	}

	gplog.Debug("[Worker %v] COPY command of receiving data: %v", cos.WorkerId, query)
	copied, err := conn.ExecContext(ctx, query, cos.WorkerId)
	gplog.Debug("[Worker %v] Finished executing query", cos.WorkerId)
	if err != nil {
		return 0, err
	}

	rows, _ := copied.RowsAffected()
	return rows, nil
}

func (cos *CopyOnSegment) IsMasterCopy() bool {
	return false
}

func (cos *CopyOnSegment) IsCopyFromStarted(rows int64) bool {
	return rows == int64(len(cos.SrcSegmentsHostInfo))
}

type ExtDestGeCopy struct {
	CopyBase
}

// CopyTo is the ExtDestGeCopy strategy's implementation of sending data.
// Used when destination cluster has more segments than source.
func (edgc *ExtDestGeCopy) CopyTo(conn *dbconn.DBConn, table option.Table, ports []HelperPortInfo, cmdId string) (int64, error) {
	var query string
	if edgc.ConnectionMode == option.ConnectionModePull {
		dataPortRange := utils.MustGetFlagString(option.DATA_PORT_RANGE)
		query = fmt.Sprintf(`COPY %v.%v TO PROGRAM 'cbcopy_helper %v --listen --cmd-id %v --seg-id <SEGID> --data-port-range %v --direction send' ON SEGMENT CSV IGNORE EXTERNAL PARTITIONS`,
			table.Schema, table.Name, edgc.CompArg, cmdId, dataPortRange)
	} else { // ConnectionModePush
		port, ip := edgc.FormAllSegsHelperAddress(ports)
		query = fmt.Sprintf(`COPY %v.%v TO PROGRAM 'cbcopy_helper %v --seg-id <SEGID> --host %v --port %v --direction send' ON SEGMENT CSV IGNORE EXTERNAL PARTITIONS`,
			table.Schema, table.Name, edgc.CompArg, ip, port)
	}

	gplog.Debug("[Worker %v] COPY command of sending data: %v", edgc.WorkerId, query)
	copied, err := conn.Exec(query, edgc.WorkerId)
	gplog.Debug("[Worker %v] Finished executing query", edgc.WorkerId)
	if err != nil {
		return 0, err
	}

	rows, _ := copied.RowsAffected()
	return rows, nil
}

// CopyFrom is the ExtDestGeCopy strategy's implementation of receiving data.
// It creates an external web table and uses it to load data in parallel.
func (edgc *ExtDestGeCopy) CopyFrom(conn *dbconn.DBConn, ctx context.Context, table option.Table, ports []HelperPortInfo, cmdId string) (int64, error) {
	extTabName := "cbcopy_ext_" + strings.Replace(uuid.NewV4().String(), "-", "", -1)
	var query string
	ids := edgc.FormAllSegsIds()

	if edgc.ConnectionMode == option.ConnectionModePull {
		port, ip := edgc.FormAllSegsHelperAddress(ports)
		query = fmt.Sprintf(`CREATE EXTERNAL WEB TEMP TABLE %v (like %v.%v) EXECUTE 'MATCHED="0"; SRC_SEG_IDS_STR="%v"; for cur_id in $SRC_SEG_IDS_STR; do if [ "$cur_id" = "$GP_SEGMENT_ID" ]; then MATCHED="1"; break; fi; done; [ "$MATCHED" != "1" ] && exit 0 || cbcopy_helper %v --seg-id $GP_SEGMENT_ID --host %v --port %v --direction receive' FORMAT 'csv'`,
			extTabName, table.Schema, table.Name, ids, edgc.CompArg, ip, port)
	} else { // ConnectionModePush
		dataPortRange := utils.MustGetFlagString(option.DATA_PORT_RANGE)
		query = fmt.Sprintf(`CREATE EXTERNAL WEB TEMP TABLE %v (like %v.%v) EXECUTE 'MATCHED="0"; SRC_SEG_IDS_STR="%v"; for cur_id in $SRC_SEG_IDS_STR; do if [ "$cur_id" = "$GP_SEGMENT_ID" ]; then MATCHED="1"; break; fi; done; [ "$MATCHED" != "1" ] && exit 0 || cbcopy_helper %v --listen --seg-id $GP_SEGMENT_ID --cmd-id %s --data-port-range %v --direction receive' FORMAT 'csv'`,
			extTabName, table.Schema, table.Name, ids, edgc.CompArg, cmdId, dataPortRange)
	}

	if err := edgc.CommitBegin(conn); err != nil {
		return 0, err
	}

	gplog.Debug("[Worker %v] External web table command of receiving data: %v", edgc.WorkerId, query)
	_, err := conn.Exec(query, edgc.WorkerId)
	if err != nil {
		return 0, err
	}

	if err := edgc.CommitBegin(conn); err != nil {
		return 0, err
	}

	gplog.Debug("[Worker %v] Finished creating external web table %v", edgc.WorkerId, extTabName)

	query = fmt.Sprintf(`INSERT INTO %v.%v SELECT * FROM %v`, table.Schema, table.Name, extTabName)
	copied, err := conn.ExecContext(ctx, query, edgc.WorkerId)
	if err != nil {
		return 0, err
	}

	gplog.Debug("[Worker %v] Dropping external web table %v", edgc.WorkerId, extTabName)

	query = fmt.Sprintf(`DROP EXTERNAL TABLE %v`, extTabName)
	_, err = conn.Exec(query, edgc.WorkerId)
	if err != nil {
		return 0, err
	}
	gplog.Debug("[Worker %v] Finished droping external web table %v", edgc.WorkerId, extTabName)

	rows, _ := copied.RowsAffected()
	return rows, nil
}

func (edgc *ExtDestGeCopy) IsMasterCopy() bool {
	return false
}

func (edgc *ExtDestGeCopy) IsCopyFromStarted(rows int64) bool {
	return rows == int64(len(edgc.SrcSegmentsHostInfo))
}

type ExtDestLtCopy struct {
	CopyBase
}

func newExtDestLtCopy(workerId int, srcSegs []utils.SegmentHostInfo, destSegs []utils.SegmentHostInfo, connectionMode string, compArg string) *ExtDestLtCopy {
	edlc := &ExtDestLtCopy{}

	edlc.WorkerId = workerId
	edlc.SrcSegmentsHostInfo = srcSegs
	edlc.DestSegmentsHostInfo = destSegs
	edlc.ConnectionMode = connectionMode
	edlc.CompArg = compArg

	return edlc
}

func (edlc *ExtDestLtCopy) formClientNumbers() string {
	segMap := make(map[int]int)

	j := 0
	for i := 0; i < len(edlc.SrcSegmentsHostInfo); i++ {
		contentId := int(edlc.DestSegmentsHostInfo[j].Content)
		clientNumber, exist := segMap[contentId]
		if !exist {
			segMap[contentId] = 1
		} else {
			segMap[contentId] = clientNumber + 1
		}

		j++

		if j == len(edlc.DestSegmentsHostInfo) {
			j = 0
		}
	}

	keys := make([]int, 0)
	for k := range segMap {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	cs := make([]string, 0)
	for _, k := range keys {
		cs = append(cs, strconv.Itoa(segMap[k]))
	}

	return strings.Join(cs, ",")
}

// CopyTo is the ExtDestLtCopy strategy's implementation of sending data.
// Used when destination cluster has fewer segments than source.
func (edlc *ExtDestLtCopy) CopyTo(conn *dbconn.DBConn, table option.Table, ports []HelperPortInfo, cmdId string) (int64, error) {
	var query string
	if edlc.ConnectionMode == option.ConnectionModePull {
		dataPortRange := utils.MustGetFlagString(option.DATA_PORT_RANGE)
		query = fmt.Sprintf(`COPY %v.%v TO PROGRAM 'cbcopy_helper %v --listen --cmd-id %v --seg-id <SEGID> --data-port-range %v --direction send' ON SEGMENT CSV IGNORE EXTERNAL PARTITIONS`,
			table.Schema, table.Name, edlc.CompArg, cmdId, dataPortRange)
	} else { // ConnectionModePush
		port, ip := edlc.FormDestSegsHelperAddress(ports)
		query = fmt.Sprintf(`COPY %v.%v TO PROGRAM 'cbcopy_helper %v --seg-id <SEGID> --host %v --port %v --direction send' ON SEGMENT CSV IGNORE EXTERNAL PARTITIONS`,
			table.Schema, table.Name, edlc.CompArg, ip, port)
	}

	gplog.Debug("[Worker %v] COPY command of sending data: %v", edlc.WorkerId, query)
	copied, err := conn.Exec(query, edlc.WorkerId)
	gplog.Debug("[Worker %v] Finished executing query", edlc.WorkerId)
	if err != nil {
		return 0, err
	}

	rows, _ := copied.RowsAffected()
	return rows, nil
}

// CopyFrom is the ExtDestLtCopy strategy's implementation of receiving data.
// It creates an external web table and uses it to load data in parallel,
// specifying the number of source clients for each destination segment.
func (edlc *ExtDestLtCopy) CopyFrom(conn *dbconn.DBConn, ctx context.Context, table option.Table, ports []HelperPortInfo, cmdId string) (int64, error) {
	extTabName := "cbcopy_ext_" + strings.Replace(uuid.NewV4().String(), "-", "", -1)
	var query string

	if edlc.ConnectionMode == option.ConnectionModePull {
		port, ip := edlc.FormSrcSegsHelperAddress(ports)
		numDests := len(edlc.DestSegmentsHostInfo)

		query = fmt.Sprintf(`CREATE EXTERNAL WEB TEMP TABLE %v (like %v.%v) EXECUTE 'cbcopy_helper %v --seg-id $GP_SEGMENT_ID --host %s --port %s --num-dests %d --direction receive' FORMAT 'csv'`,
			extTabName, table.Schema, table.Name, edlc.CompArg, ip, port, numDests)
	} else { // ConnectionModePush
		dataPortRange := utils.MustGetFlagString(option.DATA_PORT_RANGE)
		clientNumbers := edlc.formClientNumbers()
		query = fmt.Sprintf(`CREATE EXTERNAL WEB TEMP TABLE %v (like %v.%v) EXECUTE 'cbcopy_helper %v --listen --seg-id $GP_SEGMENT_ID --cmd-id %v --client-numbers %v --data-port-range %v --direction receive' FORMAT 'csv'`,
			extTabName, table.Schema, table.Name, edlc.CompArg, cmdId, clientNumbers, dataPortRange)
	}

	if err := edlc.CommitBegin(conn); err != nil {
		return 0, err
	}

	gplog.Debug("[Worker %v] External web table command of receiving data: %v", edlc.WorkerId, query)
	_, err := conn.Exec(query, edlc.WorkerId)
	if err != nil {
		return 0, err
	}

	if err := edlc.CommitBegin(conn); err != nil {
		return 0, err
	}

	gplog.Debug("[Worker %v] Finished creating external web table %v", edlc.WorkerId, extTabName)

	query = fmt.Sprintf(`INSERT INTO %v.%v SELECT * FROM %v`, table.Schema, table.Name, extTabName)
	copied, err := conn.ExecContext(ctx, query, edlc.WorkerId)
	if err != nil {
		return 0, err
	}

	gplog.Debug("[Worker %v] Dropping external web table %v", edlc.WorkerId, extTabName)

	rows, _ := copied.RowsAffected()

	query = fmt.Sprintf(`DROP EXTERNAL TABLE %v`, extTabName)
	_, err = conn.Exec(query, edlc.WorkerId)
	if err != nil {
		return 0, err
	}

	gplog.Debug("[Worker %v] Finished droping external web table %v", edlc.WorkerId, extTabName)

	return rows, nil
}

func (edlc *ExtDestLtCopy) IsMasterCopy() bool {
	return false
}

func (edlc *ExtDestLtCopy) IsCopyFromStarted(rows int64) bool {
	if edlc.ConnectionMode == option.ConnectionModePull {
		return rows == int64(len(edlc.SrcSegmentsHostInfo))
	}
	return rows == int64(len(edlc.DestSegmentsHostInfo))
}

// createTestCopyStrategy creates a copy strategy for testing purposes based on the provided strategy name.
// It takes the following parameters:
//   - strategy: the name of the strategy to create (e.g., "CopyOnMaster", "CopyOnSegment", "ExtDestGeCopy")
//   - workerId: the identifier of the worker process
//   - srcSegs: information about the source segment hosts
//   - destSegs: information about the destination segment IPs
//
// It returns an instance of a struct that implements the CopyCommand interface.
func createTestCopyStrategy(strategy string, workerId int, srcSegs []utils.SegmentHostInfo, destSegs []utils.SegmentHostInfo, connectionMode string) CopyCommand {
	compArg := "--compress-type snappy"

	switch strategy {
	case "CopyOnMaster":
		return &CopyOnMaster{CopyBase: CopyBase{
			WorkerId:             workerId,
			SrcSegmentsHostInfo:  srcSegs,
			DestSegmentsHostInfo: destSegs,
			ConnectionMode:       connectionMode,
			CompArg:              compArg,
		}}
	case "CopyOnSegment":
		return &CopyOnSegment{CopyBase: CopyBase{
			WorkerId:             workerId,
			SrcSegmentsHostInfo:  srcSegs,
			DestSegmentsHostInfo: destSegs,
			ConnectionMode:       connectionMode,
			CompArg:              compArg,
		}}
	case "ExtDestGeCopy":
		return &ExtDestGeCopy{CopyBase: CopyBase{
			WorkerId:             workerId,
			SrcSegmentsHostInfo:  srcSegs,
			DestSegmentsHostInfo: destSegs,
			ConnectionMode:       connectionMode,
			CompArg:              compArg,
		}}
	default:
		return newExtDestLtCopy(workerId, srcSegs, destSegs, connectionMode, compArg)
	}
}

// CreateCopyStrategy creates the appropriate copy strategy based on various factors:
// - Number of tuples to copy (numTuples)
// - Number of segments in source and destination clusters (srcSegs, destSegs)
// - Database versions of source and destination (srcConn.Version, destConn.Version)
// It returns an instance of a struct that implements the CopyCommand interface.
func CreateCopyStrategy(isReplicated bool,
	numTuples int64,
	workerId int,
	srcSegs []utils.SegmentHostInfo,
	destSegs []utils.SegmentHostInfo,
	srcConn, destConn *dbconn.DBConn,
	connectionMode string) CopyCommand {
	compArg := "--compress-type snappy"

	if isReplicated {
		gplog.Debug("Using CopyOnMaster strategy for replicated table")
		return &CopyOnMaster{CopyBase: CopyBase{WorkerId: workerId, SrcSegmentsHostInfo: srcSegs, DestSegmentsHostInfo: destSegs, ConnectionMode: connectionMode, CompArg: compArg}}
	}

	if strategy := os.Getenv("TEST_COPY_STRATEGY"); strategy != "" {
		gplog.Debug("Using test copy strategy: %s", strategy)
		// Note: Test strategy helper needs to be updated if it exists
		// For now, let's assume createTestCopyStrategy is adapted or not used for this flow.
		return createTestCopyStrategy(strategy, workerId, srcSegs, destSegs, connectionMode)
	}

	if numTuples <= int64(utils.MustGetFlagInt(option.ON_SEGMENT_THRESHOLD)) {
		if !utils.MustGetFlagBool(option.COMPRESSION) {
			compArg = "--no-compression"
		}
		return &CopyOnMaster{CopyBase: CopyBase{WorkerId: workerId, SrcSegmentsHostInfo: srcSegs, DestSegmentsHostInfo: destSegs, ConnectionMode: connectionMode, CompArg: compArg}}
	}

	numSrcSegs := len(srcSegs)
	numDestSegs := len(destSegs)

	compArg = "--compress-type gzip"
	if !utils.MustGetFlagBool(option.COMPRESSION) {
		compArg = "--no-compression"
	}

	if srcConn.Version.Equals(destConn.Version) && numSrcSegs == numDestSegs {
		return &CopyOnSegment{CopyBase: CopyBase{WorkerId: workerId, SrcSegmentsHostInfo: srcSegs, DestSegmentsHostInfo: destSegs, ConnectionMode: connectionMode, CompArg: compArg}}
	}

	if numDestSegs >= numSrcSegs {
		return &ExtDestGeCopy{CopyBase: CopyBase{WorkerId: workerId, SrcSegmentsHostInfo: srcSegs, DestSegmentsHostInfo: destSegs, ConnectionMode: connectionMode, CompArg: compArg}}
	}

	return newExtDestLtCopy(workerId, srcSegs, destSegs, connectionMode, compArg)
}
