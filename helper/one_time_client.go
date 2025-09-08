package helper

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

// OneTimeClient handles single connection scenarios for sending or receiving data.
type OneTimeClient struct {
	config *Config
	conn   net.Conn
}

func NewOneTimeClient(config *Config) Client {
	return &OneTimeClient{config: config}
}

func (c *OneTimeClient) open() error {
	config := c.config
	index := 0
	if config.SegID >= 0 {
		index = config.SegID
	}

	// In a one-to-one mapping, if a destination segment has no corresponding
	// source, it has no work to do.
	if index >= len(config.Hosts) {
		gplog.Debug("No source host for segment %d, exiting.", config.SegID)
		return nil
	}

	var hostAddress string
	var netConn net.Conn
	var err error

	times := 1
	for times < 5 {
		if strings.Contains(config.Hosts[index], ":") {
			hostAddress = "[" + config.Hosts[index] + "]"
		} else {
			hostAddress = config.Hosts[index]
		}

		netConn, err = net.DialTimeout("tcp", hostAddress+":"+config.Ports[index], 32*time.Second)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
		times++
	}
	if err != nil {
		return fmt.Errorf("failed to connect to server %v: %v", hostAddress+":"+config.Ports[index], err)
	}

	c.conn = netConn // Assign early in case compression fails
	gplog.Debug("Successfully established connection to %v:%v", hostAddress, config.Ports[index])

	if !config.NoCompression {
		c.conn, err = NewCompressConn(netConn, getCompressionType(config.TransCompType), config.Direction == DirectionModeReceive)
	}

	if err != nil {
		return fmt.Errorf("failed to create compress connection: %v", err)
	}

	return nil
}

func (c *OneTimeClient) close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

// Run executes the client operation.
func (c *OneTimeClient) Run() error {
	if err := c.open(); err != nil {
		return err
	}

	// If open() resulted in no connection (and no error), it means there was
	// no work to do for this segment.
	if c.conn == nil {
		return nil
	}
	defer c.close()

	if c.config.Direction == DirectionModeReceive {
		return utils.RedirectStream(c.conn, os.Stdout)
	}
	return utils.RedirectStream(os.Stdin, c.conn)
}
