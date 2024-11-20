package utils

import (
	"fmt"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
)

type SegmentHostInfo struct {
	Content  int32
	Hostname string
}

type SegmentIpInfo struct {
	Content int32
	Ip      string
}

// GetSegmentsHost retrieves the content ID and hostname for each primary segment in the Greenplum database.
// It queries the gp_segment_configuration table to get the list of primary segments and their corresponding
// content IDs and hostnames. The function returns a slice of SegmentHostInfo structs containing the content ID
// and hostname for each primary segment.
func GetSegmentsHost(conn *dbconn.DBConn) []SegmentHostInfo {
	query := `
	select content,hostname
	from gp_segment_configuration
	where role = 'p' and content != -1 order by content
	`

	gplog.Debug("GetSegmentsHost, query is %v", query)
	hosts := make([]SegmentHostInfo, 0)
	err := conn.Select(&hosts, query)
	gplog.FatalOnError(err, fmt.Sprintf("Query was: %s", query))
	return hosts
}

// GetSegmentsIpAddress resolves the IP address for each primary segment in the Greenplum database.
// It first retrieves the list of primary segments and their hostnames using the GetSegmentsHost function.
// Then, for each segment, it calls the getSegmentIpAddress function to resolve the IP address of the segment.
// The function returns a slice of SegmentIpInfo structs containing the content ID and IP address for each primary segment.
func GetSegmentsIpAddress(conn *dbconn.DBConn, timestamp string) []SegmentIpInfo {
	hosts := GetSegmentsHost(conn)
	results := make([]SegmentIpInfo, 0)

	for _, host := range hosts {
		gplog.Debug("Resolving IP address of dest segment \"%v\"", host.Hostname)
		segIp := getSegmentIpAddress(conn, timestamp, int(host.Content), host.Hostname)
		gplog.Debug("dest segment content %v ip address %v", host.Content, segIp)

		results = append(results, SegmentIpInfo{int32(host.Content), segIp})
	}

	return results
}

// getSegmentIpAddress resolves the IP address for a single primary segment in the Greenplum database.
// It creates a temporary external web table using the provided timestamp and segment ID, and executes a
// helper function on the master segment to resolve the IP address of the specified segment host.
// The function then queries the temporary table to retrieve the resolved IP address and returns it as a string.
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
