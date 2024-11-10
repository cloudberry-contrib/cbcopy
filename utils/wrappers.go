package utils

import (
	"fmt"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
)

/*
 * This file contains wrapper functions that group together functions relating
 * to querying and printing metadata, so that the logic for each object type
 * can all be in one place and copy.go can serve as a high-level look at the
 * overall copy flow.
 */

/*
 * Setup and validation wrapper functions
 */

type SegmentHostInfo struct {
	Content  int32
	Hostname string
}

type SegmentIpInfo struct {
	Content int32
	Ip      string
}

func GetSegmentsHost(conn *dbconn.DBConn) []SegmentHostInfo {
	query := fmt.Sprintf(`
	select content,hostname
	from gp_segment_configuration
	where role = 'p' and content != -1 order by content
	`)

	gplog.Debug("GetSegmentsHost, query is %v", query)
	hosts := make([]SegmentHostInfo, 0)
	err := conn.Select(&hosts, query)
	gplog.FatalOnError(err, fmt.Sprintf("Query was: %s", query))
	return hosts
}

func GetSegmentsIpAddress(conn *dbconn.DBConn, timestamp string) []SegmentIpInfo {
	if conn.HdwVersion.AtLeast("3") {
		return hdw3GetSegmentsIpAddress(conn, timestamp)
	}

	hosts := GetSegmentsHost(conn)
	results := make([]SegmentIpInfo, 0)

	for _, host := range hosts {
		gplog.Debug("Resolving IP address of dest segment \"%v\"", host.Hostname)
		segIp := getSegmentIpAddress(conn, timestamp, int(host.Content), host.Hostname)
		results = append(results, SegmentIpInfo{int32(host.Content), segIp})
	}

	for _, seg := range results {
		gplog.Info("dest segment content %v ip address %v", seg.Content, seg.Ip)
	}

	return results
}

func hdw3GetSegmentsIpAddress(conn *dbconn.DBConn, timestamp string) []SegmentIpInfo {
	results := make([]SegmentIpInfo, 0)
	query := fmt.Sprintf(`
	SELECT content, gp_toolkit.gethostip(address) as ip
	from gp_segment_configuration
	where role = 'p' and content != -1 order by content
	`)

	gplog.Debug("hdw3GetSegmentsIpAddress, query is %v", query)
	err := conn.Select(&results, query)
	gplog.FatalOnError(err, fmt.Sprintf("Query was: %s", query))

	return results
}

func getSegmentIpAddress(conn *dbconn.DBConn, timestamp string, segId int, segHost string) string {
	query := fmt.Sprintf(`
	CREATE EXTERNAL WEB TEMP TABLE cbcopy_hosts_temp_%v_%v(id int, content text)
	EXECUTE 'cbcopy_helper --seg-id %v --resolve %v' ON MASTER FORMAT 'TEXT'`,
		timestamp, segId, segId, segHost)

	gplog.Debug("getSegmentIpAddress, query is %v", query)
	_, err := conn.Exec(query)
	gplog.FatalOnError(err, fmt.Sprintf("Query was: %s", query))

	query = fmt.Sprintf(`
	SELECT id As content, content AS ip
	FROM cbcopy_hosts_temp_%v_%v`,
		timestamp, segId)

	gplog.Debug("getSegmentIpAddress, query is %v", query)
	results := make([]SegmentIpInfo, 0)
	err = conn.Select(&results, query)
	gplog.FatalOnError(err, fmt.Sprintf("Query was: %s", query))

	if len(results) != 1 {
		gplog.FatalOnError(errors.Errorf("Dest segment \"%v\" should return only one IP address", segHost),
			fmt.Sprintf("Query was: %s", query))
	}

	return results[0].Ip
}
