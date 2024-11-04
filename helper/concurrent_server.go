package helper

import (
	"compress/gzip"
	"io"
	"net"
	"sync"

	"github.com/golang/snappy"
	"github.com/greenplum-db/gp-common-go-libs/gplog"

	"github.com/cloudberrydb/cbcopy/internal/reader"
)

type ConcurrentServer struct {
	ServerBase
	writer io.Writer
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

func (c *ConcurrentServer) createReader(conn net.Conn) (interface{}, error) {
	if !c.isCompress {
		return reader.NewCsvReader(conn, defaultBufSize, '"', '"', reader.EolNl), nil
	}

	if c.compressType == CompressSnappy {
		return reader.NewCsvReader(snappy.NewReader(conn), defaultBufSize, '"', '"', reader.EolNl), nil
	}

	r, err := gzip.NewReader(conn)
	if err != nil {
		return nil, err
	}

	return reader.NewCsvReader(r, defaultBufSize, '"', '"', reader.EolNl), nil
}

func (c *ConcurrentServer) sessionMain(session *Session) {
	go session.processRequest()
}

func NewConcurrentServer(numConn int32, writer io.Writer, config *Config) Server {
	gplog.Debug("Creating ConcurrentServer...")

	c := &ConcurrentServer{ServerBase: ServerBase{
		config:       config,
		quit:         make(chan interface{}),
		numConn:      numConn,
		numFinished:  0,
		isCompress:   !config.NoCompression,
		compressType: getCompressionType(config.TransCompType),
	}, writer: writer}

	c.server = c
	c.wg.Add(1)
	return c
}
