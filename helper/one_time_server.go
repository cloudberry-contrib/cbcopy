package helper

import (
	"fmt"
	"net"
	"os"

	"github.com/cloudberry-contrib/cbcopy/utils"
	"github.com/apache/cloudberry-go-libs/gplog"
)

type OneTimeServer struct {
	ServerBase
}

func (o *OneTimeServer) Stop() {
}

func (o *OneTimeServer) handleConnection(conn net.Conn) {
	gplog.Error("OneTimeServer.handleConnection should not be called")
}

func (o *OneTimeServer) WaitForFinished() {}

func (o *OneTimeServer) Serve() {
	var conn net.Conn

	netConn, err := o.listener.Accept()
	if err != nil {
		o.setError(fmt.Errorf("failed to accept: %v\n", err))
	}

	/* close listener if we accepted on a connection from client */
	err = o.listener.Close()
	if err != nil {
		o.setError(fmt.Errorf("failed to close listener: %v\n", err))
	}

	gplog.Debug("Server accept a connection from %v", netConn.RemoteAddr())

	if !o.config.NoCompression {
		conn, err = NewCompressConn(netConn, o.compressType, o.config.Direction == DirectionModeReceive)
	} else {
		conn = netConn
	}

	if err != nil {
		gplog.Debug("Connection from %v closed without sending any data %v", netConn.RemoteAddr(), err)
		return
	}

	if o.config.Direction == DirectionModeReceive {
		gplog.Debug("passive server mode")
		err = utils.RedirectStream(conn, os.Stdout)
	} else {
		gplog.Debug("active server mode")
		err = utils.RedirectStream(os.Stdin, conn)
	}

	if err != nil {
		o.setError(err)
	}
}

func NewOneTimeServer(config *Config) Server {
	gplog.Debug("Creating OneTimeServer...")
	o := &OneTimeServer{ServerBase: ServerBase{
		config:       config,
		quit:         make(chan interface{}),
		numConn:      1,
		numFinished:  0,
		isCompress:   !config.NoCompression,
		compressType: getCompressionType(config.TransCompType)},
	}
	o.handler = o

	return o
}
