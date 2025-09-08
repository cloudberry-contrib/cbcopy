package helper

import (
	"io"
	"net"
	"os"
	"sync"

	"github.com/greenplum-db/gp-common-go-libs/gplog"

	"github.com/cloudberrydb/cbcopy/internal/reader"
)

type ConcurrentServer struct {
	ServerBase
	writer io.WriteCloser
	wmu    sync.Mutex
}

func (c *ConcurrentServer) Stop() {
	close(c.quit)
	c.listener.Close()
	c.wg.Wait()
}

func (c *ConcurrentServer) write(record interface{}) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()

	_, err := c.writer.Write(record.([]byte))
	return err
}

func (c *ConcurrentServer) handleConnection(conn net.Conn) {
	go c.processCsvRecord(conn)
}

func (c *ConcurrentServer) processCsvRecord(conn net.Conn) {
	defer func() {
		conn.Close()
		c.increase()
	}()

	gplog.Debug("ConcurrentServer accept a connection from %v", conn.RemoteAddr())

	r, err := c.getReader(conn)
	if err != nil {
		c.setError(err)
		return
	}

	cr := r.(*reader.CsvReader)

	for {
		record, er := cr.Read()
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
}

func (c *ConcurrentServer) getReader(conn net.Conn) (interface{}, error) {
	if !c.isCompress {
		return reader.NewCsvReader(conn, defaultBufSize, '"', '"', reader.EolNl), nil
	}

	r, err := NewCompressReader(conn, c.compressType)
	if err != nil {
		return nil, err
	}

	return reader.NewCsvReader(r, defaultBufSize, '"', '"', reader.EolNl), nil
}

func NewConcurrentServer(numConn int32, config *Config) Server {
	gplog.Debug("Creating ConcurrentServer...")

	c := &ConcurrentServer{ServerBase: ServerBase{
		config:       config,
		quit:         make(chan interface{}),
		numConn:      numConn,
		numFinished:  0,
		isCompress:   !config.NoCompression,
		compressType: getCompressionType(config.TransCompType),
	}, writer: os.Stdout}

	c.handler = c
	c.wg.Add(1)
	return c
}
