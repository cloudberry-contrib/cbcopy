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

/*
Package copy provides various strategies for copying data between source and
destination clusters in a distributed database environment. The strategies are
designed to handle different configurations and sizes of source and destination
clusters, optimizing for performance and resource utilization.

The main components of this package include:

1. CopyCommand Interface:
   - Defines the methods required for any copy strategy, including CopyTo and
     CopyFrom for sending and receiving data, and methods to check the status of
     the copy operation.

2. CopyBase Struct:
   - Provides common fields and methods used by all copy strategies, such as
     worker ID, segment information, and helper address formation.

3. Copy Strategies:
   - CopyOnMaster: Suitable for small tables, performs the copy operation on the
     master node without involving segment nodes.
   - CopyOnSegment: Used when the source and destination clusters have the same
     number of segments and distribution properties, allowing for parallel data
     transfer on each segment.
   - ExtDestGeCopy: Applied when the destination cluster has more or equal
     segments compared to the source, facilitating parallel data transfer with
     external web tables.
   - ExtDestLtCopy: Utilized when the destination cluster has fewer segments
     than the source, aggregating data from multiple source segments to each
     destination segment using external web tables.

4. Strategy Selection:
   - The CreateCopyStrategy function determines the appropriate copy strategy
     based on the number of tuples, segment configurations, and database
     versions, ensuring optimal performance for the given scenario.

This package is essential for efficiently managing data transfer in distributed
database systems, providing flexibility to adapt to various cluster
configurations and data sizes.
*/

type CopyCommand interface {
	CopyTo(conn *dbconn.DBConn, table option.Table, ports []HelperPortInfo, cmdId string) (int64, error)
	CopyFrom(conn *dbconn.DBConn, ctx context.Context, table option.Table, cmdId string) (int64, error)
	IsCopyFromStarted(rows int64) bool
	IsMasterCopy() bool
}

type CopyBase struct {
	WorkerId            int
	SrcSegmentsHostInfo []utils.SegmentHostInfo
	DestSegmentsIpInfo  []utils.SegmentIpInfo
	CompArg             string
}

func (cc *CopyBase) FormMasterHelperAddress(ports []HelperPortInfo) (string, string) {
	ip := utils.MustGetFlagString(option.DEST_HOST)
	port := strconv.Itoa(int(ports[0].Port))

	return port, ip
}

func (cc *CopyBase) FormAllSegsHelperAddress(ports []HelperPortInfo) (string, string) {
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

func (cc *CopyBase) FormSegsHelperAddress(ports []HelperPortInfo) (string, string) {
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

/*
CopyOnMaster is a copy strategy that performs the entire copy operation on the
master node, without involving the segment nodes.

This strategy is suitable for small tables, where the overhead of distributing
the data to segments and collecting results back would outweigh the benefits of
parallel processing.

When using CopyOnMaster:
 1. The COPY TO command is executed on the master node, which sends all data
    directly to the cbcopy_helper program.
 2. The cbcopy_helper program on the master streams the data to the destination
    database's master node.
 3. The destination master node receives the data and executes a COPY FROM command
    to load the data into the table.

Because all data flows through the master nodes, this strategy is limited by the
master's resources and network bandwidth. It is not suitable for large tables
where the master could become a bottleneck.

However, for small tables, CopyOnMaster can be more efficient than strategies
that involve the segments, as it avoids the overhead of coordinating and
aggregating results from multiple nodes.
*/
type CopyOnMaster struct {
	CopyBase
}

// CopyTo is part of the CopyCommand interface.
// It executes the COPY TO command to send data from the source database.
// The specific implementation varies based on the copy strategy.
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

// CopyFrom is part of the CopyCommand interface.
// It executes the COPY FROM command to receive data into the destination database.
// The specific implementation varies based on the copy strategy.
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

/*
CopyOnSegment is a copy strategy that performs the copy operation in parallel on
each segment node.

This strategy is applicable when the following conditions are met:
 1. The number of segments in the destination cluster is equal to the number of
    segments in the source cluster.
 2. The distribution key and hash algorithm used in the destination table are the
    same as those used in the source table.

When these conditions are satisfied, each source segment can copy its portion of
the data directly to the corresponding destination segment, without the need for
data redistribution.

The copy process for each segment is as follows:
 1. The source segment executes a COPY TO command with the ON SEGMENT clause,
    which sends the segment's data to the cbcopy_helper program.
 2. The cbcopy_helper program on the source segment streams the data to the
    cbcopy_helper program on the corresponding destination segment.
 3. The destination segment's cbcopy_helper program receives the data and executes
    a COPY FROM command with the ON SEGMENT clause to load the data into the table.

This strategy allows for efficient parallel data transfer, as each segment pair
can copy its data independently. It minimizes data movement across the network,
since each segment's data is sent directly to its destination.

However, CopyOnSegment is only applicable in specific scenarios where the source
and destination clusters have the same segment configuration and data distribution
properties.
*/
type CopyOnSegment struct {
	CopyBase
}

// CopyTo is the CopyOnSegment strategy's implementation of sending data.
// It uses ON SEGMENT clause to execute COPY on each segment.
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

// CopyFrom is the CopyOnSegment strategy's implementation of receiving data.
// It uses ON SEGMENT clause to execute COPY on each segment.
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

/*
ExtDestGeCopy is a copy strategy used when the number of segments in the
destination cluster is greater than or equal to the number of segments in the
source cluster.

In this scenario, each source segment sends its data to a single destination
segment. If there are more destination segments than source segments, some
destination segments will not receive any data.

The copy process is as follows:
 1. The source cluster executes a COPY TO command with the ON SEGMENT clause,
    which sends each segment's data to a corresponding cbcopy_helper program.
 2. Each cbcopy_helper program on the source side streams the data to a single
    cbcopy_helper program on the destination side.
 3. The destination cluster creates an external web table that uses the
    cbcopy_helper programs as the data source.
 4. The destination cluster executes an INSERT INTO command to load the data from
    the external web table into the target table.

The external web table is created with a custom EXECUTE clause that runs
cbcopy_helper on each destination segment. The EXECUTE clause includes a
condition to ensure that each destination segment only receives data if it
corresponds to a source segment.

This strategy allows for parallel data transfer, as each source segment sends
data to a destination segment independently. It is more efficient than
CopyOnMaster when the data size is large, as it distributes the workload across
all source segments.

However, it requires the number of destination segments to be greater than or
equal to the number of source segments. If the number of destination segments is
less than the number of source segments, the ExtDestLtCopy strategy should be
used instead, as it can handle this scenario correctly.
*/
type ExtDestGeCopy struct {
	CopyBase
}

// CopyTo is the ExtDestGeCopy strategy's implementation of sending data.
// Used when destination cluster has more segments than source.
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

// CopyFrom is the ExtDestGeCopy strategy's implementation of receiving data.
// It creates an external web table and uses it to load data in parallel.
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

/*
ExtDestLtCopy is a copy strategy used when the number of segments in the
destination cluster is less than the number of segments in the source cluster.

In this scenario, each destination segment receives data from multiple source
segments. The strategy uses an external web table on the destination side to
facilitate parallel data transfer.

The copy process is as follows:
 1. The source cluster executes a COPY TO command with the ON SEGMENT clause,
    which sends each segment's data to the corresponding cbcopy_helper program.
 2. Each cbcopy_helper program on the source side streams its data to a
    cbcopy_helper program on the destination side.
 3. The destination cluster creates an external web table that uses the
    cbcopy_helper programs as the data source.
 4. The destination cluster executes an INSERT INTO command to load the data from
    the external web table into the target table.

The external web table is created with a custom EXECUTE clause that runs
cbcopy_helper on each destination segment. The EXECUTE clause includes parameters
to specify the number of source clients for each destination segment, ensuring
that data from multiple source segments is correctly aggregated.

This strategy allows for efficient data transfer even when the destination
cluster has fewer segments than the source cluster. It balances the load by
distributing data from multiple source segments to each destination segment.

ExtDestLtCopy is particularly useful in scenarios where the destination cluster
is smaller, as it ensures that all data is transferred and loaded correctly
despite the difference in segment numbers.
*/
type ExtDestLtCopy struct {
	CopyBase
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

// CopyTo is the ExtDestLtCopy strategy's implementation of sending data.
// Used when destination cluster has fewer segments than source.
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

// CopyFrom is the ExtDestLtCopy strategy's implementation of receiving data.
// It creates an external web table and uses it to load data in parallel,
// specifying the number of source clients for each destination segment.
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

// createTestCopyStrategy creates a copy strategy for testing purposes based on the provided strategy name.
// It takes the following parameters:
//   - strategy: the name of the strategy to create (e.g., "CopyOnMaster", "CopyOnSegment", "ExtDestGeCopy")
//   - workerId: the identifier of the worker process
//   - srcSegs: information about the source segment hosts
//   - destSegs: information about the destination segment IPs
//
// It returns an instance of a struct that implements the CopyCommand interface.
func createTestCopyStrategy(strategy string, workerId int, srcSegs []utils.SegmentHostInfo, destSegs []utils.SegmentIpInfo) CopyCommand {
	compArg := "--compress-type snappy"

	switch strategy {
	case "CopyOnMaster":
		return &CopyOnMaster{CopyBase: CopyBase{
			WorkerId:            workerId,
			SrcSegmentsHostInfo: srcSegs,
			DestSegmentsIpInfo:  destSegs,
			CompArg:             compArg,
		}}
	case "CopyOnSegment":
		return &CopyOnSegment{CopyBase: CopyBase{
			WorkerId:            workerId,
			SrcSegmentsHostInfo: srcSegs,
			DestSegmentsIpInfo:  destSegs,
			CompArg:             compArg,
		}}
	case "ExtDestGeCopy":
		return &ExtDestGeCopy{CopyBase: CopyBase{
			WorkerId:            workerId,
			SrcSegmentsHostInfo: srcSegs,
			DestSegmentsIpInfo:  destSegs,
			CompArg:             compArg,
		}}
	default:
		return newExtDestLtCopy(workerId, srcSegs, destSegs, compArg)
	}
}

// CreateCopyStrategy creates the appropriate copy strategy based on various factors:
// - Number of tuples to copy (numTuples)
// - Number of segments in source and destination clusters (srcSegs, destSegs)
// - Database versions of source and destination (srcConn.Version, destConn.Version)
// It returns an instance of a struct that implements the CopyCommand interface.
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
		return &CopyOnMaster{CopyBase: CopyBase{WorkerId: workerId, SrcSegmentsHostInfo: srcSegs, DestSegmentsIpInfo: destSegs, CompArg: compArg}}
	}

	numSrcSegs := len(srcSegs)
	numDestSegs := len(destSegs)

	compArg = "--compress-type gzip"
	if !utils.MustGetFlagBool(option.COMPRESSION) {
		compArg = "--no-compression"
	}

	if srcConn.Version.Equals(destConn.Version) && numSrcSegs == numDestSegs {
		return &CopyOnSegment{CopyBase: CopyBase{WorkerId: workerId, SrcSegmentsHostInfo: srcSegs, DestSegmentsIpInfo: destSegs, CompArg: compArg}}
	}

	if numDestSegs >= numSrcSegs {
		return &ExtDestGeCopy{CopyBase: CopyBase{WorkerId: workerId, SrcSegmentsHostInfo: srcSegs, DestSegmentsIpInfo: destSegs, CompArg: compArg}}
	}

	return newExtDestLtCopy(workerId, srcSegs, destSegs, compArg)
}
