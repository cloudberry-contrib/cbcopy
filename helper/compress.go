package helper

import (
	"compress/gzip"
	"io"
	"net"

	"github.com/pkg/errors"

	"github.com/apache/cloudberry-go-libs/gplog"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

// CompressType is the compression format
type CompressType int

const (
	// CompressSnappy is the Snappy compression format
	CompressSnappy = iota
	CompressGzip
	CompressZstd
)

// baseCompressConn contains common fields and methods for compressed connections
type baseCompressConn struct {
	net.Conn
	plainCloser io.Closer
}

func (conn *baseCompressConn) Close() error {
	return conn.plainCloser.Close()
}

type ReadOnlyCompressConn struct {
	baseCompressConn
	reader io.Reader
}

func (conn *ReadOnlyCompressConn) Read(bytes []byte) (int, error) {
	return conn.reader.Read(bytes)
}

func (conn *ReadOnlyCompressConn) Write(bytes []byte) (int, error) {
	panic("Write() called on read-only compress connection")
}

func (conn *ReadOnlyCompressConn) Close() error {
	return conn.plainCloser.Close()
}

type WriteOnlyCompressConn struct {
	baseCompressConn
	writeCloser io.WriteCloser
}

func (conn *WriteOnlyCompressConn) Read(bytes []byte) (int, error) {
	panic("Read() called on write-only compress connection")
}

func (conn *WriteOnlyCompressConn) Write(bytes []byte) (int, error) {
	return conn.writeCloser.Write(bytes)
}

// Close properly closes both the compressor and underlying connection
func (conn *WriteOnlyCompressConn) Close() error {
	var err1 error
	var err2 error

	// call compression writer's own Close()
	err1 = conn.writeCloser.Close()
	// close net.Conn
	err2 = conn.plainCloser.Close()

	// return the first level error
	// if snappy and net both have errors, return snappy's
	if err1 != nil {
		return err1
	}
	return err2
}

// NewCompressReader creates a read-only compressed connection
func NewCompressReader(conn net.Conn, compressType CompressType) (net.Conn, error) {
	var reader io.Reader
	var err error

	switch compressType {
	case CompressSnappy:
		reader = snappy.NewReader(conn)
	case CompressGzip:
		reader, err = gzip.NewReader(conn)
		if err != nil {
			return nil, err
		}
	case CompressZstd:
		reader, err = zstd.NewReader(conn)
		if err != nil {
			return nil, err
		}
	default:
		gplog.Fatal(errors.Errorf("No recognized compression type %v", compressType), "")
	}

	return &ReadOnlyCompressConn{
		baseCompressConn: baseCompressConn{
			Conn:        conn,
			plainCloser: conn,
		},
		reader: reader,
	}, nil
}

// NewCompressWriter creates a write-only compressed connection
func NewCompressWriter(conn net.Conn, compressType CompressType) (net.Conn, error) {
	var writeCloser io.WriteCloser
	var err error

	switch compressType {
	case CompressSnappy:
		writeCloser = snappy.NewBufferedWriter(conn)
	case CompressGzip:
		writeCloser, err = gzip.NewWriterLevel(conn, gzip.DefaultCompression)
		if err != nil {
			return nil, err
		}
	case CompressZstd:
		writeCloser, err = zstd.NewWriter(conn, zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err != nil {
			return nil, err
		}
	default:
		gplog.Fatal(errors.Errorf("No recognized compression type %v", compressType), "")
	}

	return &WriteOnlyCompressConn{
		baseCompressConn: baseCompressConn{
			Conn:        conn,
			plainCloser: conn,
		},
		writeCloser: writeCloser,
	}, nil
}

// NewCompressConn creates a unidirectional compressed connection for backward compatibility
// isReader=true: creates a read-only connection for receiving compressed data
// isReader=false: creates a write-only connection for sending compressed data
func NewCompressConn(conn net.Conn, compressType CompressType, isReader bool) (net.Conn, error) {
	if isReader {
		return NewCompressReader(conn, compressType)
	} else {
		return NewCompressWriter(conn, compressType)
	}
}

func getCompressionType(compType string) CompressType {
	switch compType {
	case "snappy":
		return CompressSnappy
	case "zstd":
		return CompressZstd
	case "gzip":
		return CompressGzip
	default:
		return CompressZstd
	}
}
