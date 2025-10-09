package helper

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cloudberry-contrib/cbcopy/internal/reader"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

// ConcurrentClient handles receiving data from multiple sources concurrently.
type ConcurrentClient struct {
	config *Config
	writer io.WriteCloser
	wmu    sync.Mutex
	err    error
	errmu  sync.Mutex
}

func NewConcurrentClient(config *Config) Client {
	return &ConcurrentClient{config: config, writer: os.Stdout}
}

// Run executes the concurrent receive operation.
func (c *ConcurrentClient) Run() error {
	numSrc := len(c.config.Hosts)
	numDests := c.config.NumDests
	segID := c.config.SegID

	targetHosts := make([]string, 0)
	targetPorts := make([]string, 0)
	for i := segID; i < numSrc; i += numDests {
		targetHosts = append(targetHosts, c.config.Hosts[i])
		targetPorts = append(targetPorts, c.config.Ports[i])
	}

	var wg sync.WaitGroup
	for i, host := range targetHosts {
		wg.Add(1)
		go func(host, port string) {
			defer wg.Done()
			gplog.Debug("Starting client to receive from %s:%s", host, port)

			conn, err := c.connect(host, port)
			if err != nil {
				c.setError(err)
				return
			}
			defer conn.Close()

			csvReader := reader.NewCsvReader(conn, defaultBufSize, '"', '"', reader.EolNl)
			for {
				record, er := csvReader.Read()
				if len(record) > 0 {
					ew := c.write(record)
					if ew != nil {
						err = ew
						break
					}
				}

				if er == io.EOF {
					break
				}

				if er != nil {
					err = er
					break
				}
			}

			if err != nil {
				c.setError(err)
			}
		}(host, targetPorts[i])
	}

	wg.Wait()

	return c.err
}

func (c *ConcurrentClient) connect(host, port string) (net.Conn, error) {
	var hostAddress string
	var netConn net.Conn
	var err error

	times := 1
	for times < 5 {
		if strings.Contains(host, ":") {
			hostAddress = "[" + host + "]"
		} else {
			hostAddress = host
		}

		netConn, err = net.DialTimeout("tcp", hostAddress+":"+port, 32*time.Second)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
		times++
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server %v: %v", hostAddress+":"+port, err)
	}
	gplog.Debug("Successfully established connection to %v:%v", hostAddress, port)

	var finalConn net.Conn
	if !c.config.NoCompression {
		// For concurrent client, the direction is always receive.
		finalConn, err = NewCompressConn(netConn, getCompressionType(c.config.TransCompType), true)
	} else {
		finalConn = netConn
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create compress connection: %v", err)
	}

	return finalConn, nil
}

func (c *ConcurrentClient) write(record interface{}) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()

	_, err := c.writer.Write(record.([]byte))
	return err
}

func (c *ConcurrentClient) setError(err error) {
	c.errmu.Lock()
	defer c.errmu.Unlock()
	c.err = err
}
