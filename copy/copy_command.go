package copy

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type CopyCommand interface {
	CopyTo(conn *dbconn.DBConn, table option.Table, ports []HelperPortInfo, cmdId string) (int64, error)
	CopyFrom(conn *dbconn.DBConn, ctx context.Context, table option.Table, cmdId string) (int64, error)
	IsCopyFromStarted(rows int64) bool
	IsMasterCopy() bool
}

type CopyCommon struct {
	WorkerId            int
	SrcSegmentsHostInfo []utils.SegmentHostInfo
	DestSegmentsIpInfo  []utils.SegmentIpInfo
	CompArg             string
}

func (cc *CopyCommon) FormMasterHelperAddress(ports []HelperPortInfo) (string, string) {
	ip := utils.MustGetFlagString(option.DEST_HOST)
	port := strconv.Itoa(int(ports[0].Port))

	return port, ip
}

func (cc *CopyCommon) FormAllSegsHelperAddress(ports []HelperPortInfo) (string, string) {
	ps := make([]string, 0)
	is := make([]string, 0)

	len := 0
	for _, p := range ports {
		len++
		ps = append(ps, strconv.Itoa(int(p.Port)))
	}
	pl := strings.Join(ps, ",")

	for i := 0; i < len; i++ {
		is = append(is, cc.DestSegmentsIpInfo[i].Ip)
	}
	il := strings.Join(is, ",")

	return pl, il
}

// --host 192.168.10.200,99999,192.168.10.200,99999 --port 37932,99999,46176,99999
func (cc *CopyCommon) FormSegsHelperAddress(ports []HelperPortInfo) (string, string) {
	ps := make([]string, 0)
	is := make([]string, 0)

	j := 0
	for i := 0; i < len(cc.SrcSegmentsHostInfo); i++ {
		ps = append(ps, strconv.Itoa(int(ports[j].Port)))
		is = append(is, cc.DestSegmentsIpInfo[j].Ip)

		j++

		if j == len(cc.DestSegmentsIpInfo) {
			j = 0
		}
	}

	pl := strings.Join(ps, ",")
	il := strings.Join(is, ",")
	return pl, il
}

func (cc *CopyCommon) FormAllSegsIds() string {
	hs := make([]string, 0)

	for _, h := range cc.SrcSegmentsHostInfo {
		hs = append(hs, strconv.Itoa(int(h.Content)))
	}
	result := strings.Join(hs, " ")

	return result
}

func (cc *CopyCommon) CommitBegin(conn *dbconn.DBConn) error {
	if err := conn.Commit(cc.WorkerId); err != nil {
		return err
	}

	if err := conn.Begin(cc.WorkerId); err != nil {
		return err
	}

	return nil
}

/* small table, copy on master directly */

type CopyOnMaster struct {
	CopyCommon
}

func (com *CopyOnMaster) CopyTo(conn *dbconn.DBConn, table option.Table, ports []HelperPortInfo, cmdId string) (int64, error) {
	port, ip := com.FormMasterHelperAddress(ports)
	query := fmt.Sprintf(`COPY %v.%v TO PROGRAM 'cbcopy_helper %v --seg-id -1 --host %v --port %v' CSV IGNORE EXTERNAL PARTITIONS`,
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

func (com *CopyOnMaster) CopyFrom(conn *dbconn.DBConn, ctx context.Context, table option.Table, cmdId string) (int64, error) {
	dataPortRange := utils.MustGetFlagString(option.DATA_PORT_RANGE)

	query := fmt.Sprintf(`COPY %v.%v FROM PROGRAM 'cbcopy_helper %v --listen --seg-id -1 --cmd-id %v --data-port-range %v' CSV`,
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

/* dest cluster segments == source cluster segments and use the same hash algorithm  */
type CopyOnSegment struct {
	CopyCommon
}

func (cos *CopyOnSegment) CopyTo(conn *dbconn.DBConn, table option.Table, ports []HelperPortInfo, cmdId string) (int64, error) {
	port, ip := cos.FormAllSegsHelperAddress(ports)
	query := fmt.Sprintf(`COPY %v.%v TO PROGRAM 'cbcopy_helper %v --seg-id <SEGID> --host %v --port %v' ON SEGMENT CSV IGNORE EXTERNAL PARTITIONS`,
		table.Schema, table.Name, cos.CompArg, ip, port)

	gplog.Debug("[Worker %v] COPY command of sending data: %v", cos.WorkerId, query)
	copied, err := conn.Exec(query, cos.WorkerId)
	gplog.Debug("[Worker %v] Finished executing query", cos.WorkerId)
	if err != nil {
		return 0, err
	}

	rows, _ := copied.RowsAffected()
	return rows, nil
}

func (cos *CopyOnSegment) CopyFrom(conn *dbconn.DBConn, ctx context.Context, table option.Table, cmdId string) (int64, error) {
	dataPortRange := utils.MustGetFlagString(option.DATA_PORT_RANGE)

	query := fmt.Sprintf(`COPY %v.%v FROM PROGRAM 'cbcopy_helper %v --listen --seg-id <SEGID> --cmd-id %v --data-port-range %v' ON SEGMENT CSV`,
		table.Schema, table.Name, cos.CompArg, cmdId, dataPortRange)

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

/* dest cluster segments >= source cluster segments */
type ExtDestGeCopy struct {
	CopyCommon
}

func (edgc *ExtDestGeCopy) CopyTo(conn *dbconn.DBConn, table option.Table, ports []HelperPortInfo, cmdId string) (int64, error) {
	port, ip := edgc.FormAllSegsHelperAddress(ports)
	query := fmt.Sprintf(`COPY %v.%v TO PROGRAM 'cbcopy_helper %v --seg-id <SEGID> --host %v --port %v' ON SEGMENT CSV IGNORE EXTERNAL PARTITIONS`,
		table.Schema, table.Name, edgc.CompArg, ip, port)

	gplog.Debug("[Worker %v] COPY command of sending data: %v", edgc.WorkerId, query)
	copied, err := conn.Exec(query, edgc.WorkerId)
	gplog.Debug("[Worker %v] Finished executing query", edgc.WorkerId)
	if err != nil {
		return 0, err
	}

	rows, _ := copied.RowsAffected()
	return rows, nil
}

func (edgc *ExtDestGeCopy) CopyFrom(conn *dbconn.DBConn, ctx context.Context, table option.Table, cmdId string) (int64, error) {
	dataPortRange := utils.MustGetFlagString(option.DATA_PORT_RANGE)

	extTabName := "cbcopy_ext_" + strings.Replace(uuid.NewV4().String(), "-", "", -1)
	ids := edgc.FormAllSegsIds()

	query := fmt.Sprintf(`CREATE EXTERNAL WEB TEMP TABLE %v (like %v.%v) EXECUTE 'MATCHED=0; SEGMENTS=(%v); for i in "${SEGMENTS[@]}"; do [ $i = "$GP_SEGMENT_ID" ] && MATCHED=1; done; [ $MATCHED != 1 ] && exit 0 || cbcopy_helper %v --listen --seg-id $GP_SEGMENT_ID --cmd-id %s --data-port-range %v' FORMAT 'csv'`,
		extTabName, table.Schema, table.Name, ids, edgc.CompArg, cmdId, dataPortRange)

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

/* dest cluster segments < source cluster segments */
type ExtDestLtCopy struct {
	CopyCommon
}

func newExtDestLtCopy(workerId int, srcSegs []utils.SegmentHostInfo, destSegs []utils.SegmentIpInfo, compArg string) *ExtDestLtCopy {
	edlc := &ExtDestLtCopy{}

	edlc.WorkerId = workerId
	edlc.SrcSegmentsHostInfo = srcSegs
	edlc.DestSegmentsIpInfo = destSegs
	edlc.CompArg = compArg

	return edlc
}

func (edlc *ExtDestLtCopy) formClientNumbers() string {
	segMap := make(map[int]int)

	j := 0
	for i := 0; i < len(edlc.SrcSegmentsHostInfo); i++ {
		contentId := int(edlc.DestSegmentsIpInfo[j].Content)
		clientNumber, exist := segMap[contentId]
		if !exist {
			segMap[contentId] = 1
		} else {
			segMap[contentId] = clientNumber + 1
		}

		j++

		if j == len(edlc.DestSegmentsIpInfo) {
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

func (edlc *ExtDestLtCopy) CopyTo(conn *dbconn.DBConn, table option.Table, ports []HelperPortInfo, cmdId string) (int64, error) {
	if len(ports) != len(edlc.DestSegmentsIpInfo) {
		return 0, errors.Errorf("The number of helper ports should be equal to the number of dest segments: [%v %v]", len(ports), len(edlc.DestSegmentsIpInfo))
	}

	port, ip := edlc.FormSegsHelperAddress(ports)
	query := fmt.Sprintf(`COPY %v.%v TO PROGRAM 'cbcopy_helper %v --seg-id <SEGID> --host %v --port %v' ON SEGMENT CSV IGNORE EXTERNAL PARTITIONS`,
		table.Schema, table.Name, edlc.CompArg, ip, port)

	gplog.Debug("[Worker %v] COPY command of sending data: %v", edlc.WorkerId, query)
	copied, err := conn.Exec(query, edlc.WorkerId)
	gplog.Debug("[Worker %v] Finished executing query", edlc.WorkerId)
	if err != nil {
		return 0, err
	}

	rows, _ := copied.RowsAffected()
	return rows, nil
}

func (edlc *ExtDestLtCopy) CopyFrom(conn *dbconn.DBConn, ctx context.Context, table option.Table, cmdId string) (int64, error) {
	dataPortRange := utils.MustGetFlagString(option.DATA_PORT_RANGE)

	extTabName := "cbcopy_ext_" + strings.Replace(uuid.NewV4().String(), "-", "", -1)
	clientNumbers := edlc.formClientNumbers()

	dpmCmd := ""
	dpm := os.Getenv("disable_parallel_mode")
	if len(dpm) > 0 {
		dpmCmd = "--disable-parallel-mode"
	}

	query := fmt.Sprintf(`CREATE EXTERNAL WEB TEMP TABLE %v (like %v.%v) EXECUTE 'cbcopy_helper %v --listen --seg-id $GP_SEGMENT_ID --cmd-id %v --client-numbers %v %v  --data-port-range %v' FORMAT 'csv'`,
		extTabName, table.Schema, table.Name, edlc.CompArg, cmdId, clientNumbers, dpmCmd, dataPortRange)

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
	return rows == int64(len(edlc.DestSegmentsIpInfo))
}

func createTestCopyStrategy(strategy string, workerId int, srcSegs []utils.SegmentHostInfo, destSegs []utils.SegmentIpInfo) CopyCommand {
	compArg := "--compress-type snappy"

	switch strategy {
	case "CopyOnMaster":
		return &CopyOnMaster{CopyCommon: CopyCommon{
			WorkerId:            workerId,
			SrcSegmentsHostInfo: srcSegs,
			DestSegmentsIpInfo:  destSegs,
			CompArg:             compArg,
		}}
	case "CopyOnSegment":
		return &CopyOnSegment{CopyCommon: CopyCommon{
			WorkerId:            workerId,
			SrcSegmentsHostInfo: srcSegs,
			DestSegmentsIpInfo:  destSegs,
			CompArg:             compArg,
		}}
	case "ExtDestGeCopy":
		return &ExtDestGeCopy{CopyCommon: CopyCommon{
			WorkerId:            workerId,
			SrcSegmentsHostInfo: srcSegs,
			DestSegmentsIpInfo:  destSegs,
			CompArg:             compArg,
		}}
	default:
		return newExtDestLtCopy(workerId, srcSegs, destSegs, compArg)
	}
}

func CreateCopyStrategy(numTuples int64, workerId int, srcSegs []utils.SegmentHostInfo, destSegs []utils.SegmentIpInfo, srcConn, destConn *dbconn.DBConn) CopyCommand {
	if strategy := os.Getenv("TEST_COPY_STRATEGY"); strategy != "" {
		gplog.Debug("Using test copy strategy: %s", strategy)
		return createTestCopyStrategy(strategy, workerId, srcSegs, destSegs)
	}

	compArg := "--compress-type snappy"
	if numTuples <= int64(utils.MustGetFlagInt(option.ON_SEGMENT_THRESHOLD)) {
		if !utils.MustGetFlagBool(option.COMPRESSION) {
			compArg = "--no-compression"
		}
		return &CopyOnMaster{CopyCommon: CopyCommon{WorkerId: workerId, SrcSegmentsHostInfo: srcSegs, DestSegmentsIpInfo: destSegs, CompArg: compArg}}
	}

	numSrcSegs := len(srcSegs)
	numDestSegs := len(destSegs)

	srcVersion := srcConn.Version
	if srcConn.HdwVersion.AtLeast("2") {
		srcVersion = srcConn.HdwVersion
	}

	destVersion := destConn.Version
	if destConn.HdwVersion.AtLeast("2") {
		destVersion = destConn.HdwVersion
	}

	compArg = "--compress-type gzip"
	if !utils.MustGetFlagBool(option.COMPRESSION) {
		compArg = "--no-compression"
	}

	if isSameVersion(srcVersion, destVersion) && numSrcSegs == numDestSegs {
		return &CopyOnSegment{CopyCommon: CopyCommon{WorkerId: workerId, SrcSegmentsHostInfo: srcSegs, DestSegmentsIpInfo: destSegs, CompArg: compArg}}
	}

	if numDestSegs >= numSrcSegs {
		return &ExtDestGeCopy{CopyCommon: CopyCommon{WorkerId: workerId, SrcSegmentsHostInfo: srcSegs, DestSegmentsIpInfo: destSegs, CompArg: compArg}}
	}

	return newExtDestLtCopy(workerId, srcSegs, destSegs, compArg)
}
