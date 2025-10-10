package copy

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cloudberry-contrib/cbcopy/internal/dbconn"
	"github.com/apache/cloudberry-go-libs/gplog"
)

// PortHelper handles port-related operations
type PortHelper struct {
	conn *dbconn.DBConn
}

type HelperPortInfo struct {
	Content int
	Port    int
}

func NewPortHelper(conn *dbconn.DBConn) *PortHelper {
	return &PortHelper{conn: conn}
}

// CreateHelperPortTable creates temporary tables for storing helper program ports
func (ph *PortHelper) CreateHelperPortTable(timestamp string) error {
	// Create table for master
	query := fmt.Sprintf(`
		CREATE EXTERNAL WEB TABLE public.cbcopy_ports_temp_onmaster_%v 
		(cmdID text, segID int, port int) 
		EXECUTE 'cat /tmp/cbcopy-*.txt 2>/dev/null || true' 
		ON MASTER FORMAT 'TEXT'`, timestamp)

	gplog.Debug("CreateHelperPortTable, query is %v", query)
	_, err := ph.conn.Exec(query)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("failed to create master port table: %v", err)
	}

	// Create table for all segments
	query = fmt.Sprintf(`
		CREATE EXTERNAL WEB TABLE public.cbcopy_ports_temp_onall_%v 
		(cmdID text, segID int, port int) 
		EXECUTE 'cat /tmp/cbcopy-*.txt 2>/dev/null || true' 
		ON ALL FORMAT 'TEXT'`, timestamp)

	gplog.Debug("CreateHelperPortTable, query is %v", query)
	_, err = ph.conn.Exec(query)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("failed to create segment port table: %v", err)
	}

	return nil
}

// GetHelperPortList retrieves the list of helper program ports
func (ph *PortHelper) GetHelperPortList(timestamp string, cmdId string,
	workerId int, isMasterCopy bool) ([]HelperPortInfo, error) {

	target := "onall"
	if isMasterCopy {
		target = "onmaster"
	}

	query := fmt.Sprintf(`
	SELECT segID AS content, port AS port
	FROM public.cbcopy_ports_temp_%v_%v
	WHERE cmdID = '%v'`, target, timestamp, cmdId)

	hpis := make([]HelperPortInfo, 0)

	gplog.Debug("[Worker %v] Retrieving ports of helper programs: %v", workerId, query)

	err := ph.conn.Select(&hpis, query, workerId)
	if err != nil {
		return nil, err
	}

	if len(hpis) == 0 {
		return hpis, nil // needs retry
	}

	// Remove duplicates and sort
	return ph.processPortList(hpis), nil
}

// processPortList removes duplicates and sorts port information
func (ph *PortHelper) processPortList(hpis []HelperPortInfo) []HelperPortInfo {
	segMap := make(map[int]int)
	for _, segPort := range hpis {
		_, exist := segMap[segPort.Content]
		if !exist {
			segMap[segPort.Content] = segPort.Port
		}
	}

	// Sort by content ID
	keys := make([]int, 0)
	for k := range segMap {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	// Create sorted result
	result := make([]HelperPortInfo, 0)
	for _, k := range keys {
		result = append(result, HelperPortInfo{
			Content: k,
			Port:    segMap[k],
		})
	}

	return result
}
