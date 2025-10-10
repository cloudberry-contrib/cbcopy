package helper

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/cloudberry-go-libs/gplog"
)

const (
	defaultBufSize = 1048576
)

type Server interface {
	Start() int
	Serve()
	Stop()
	WaitForFinished()
	Err() error
}

type Handler interface {
	handleConnection(conn net.Conn)
}

type ServerBase struct {
	handler      Handler
	config       *Config
	listener     net.Listener
	quit         chan interface{}
	wg           sync.WaitGroup
	numConn      int32
	numFinished  int32
	isCompress   bool
	compressType CompressType
	err          error
	emu          sync.Mutex
}

func (t *ServerBase) Start() int {
	var err error

	index := 0

	if t.config.SegID >= 0 {
		index = t.config.SegID
	}

	if len(t.config.DataPortRange) == 2 {
		for i := t.config.DataPortRange[0]; i <= t.config.DataPortRange[1]; i++ {
			t.listener, err = net.Listen("tcp", ":"+strconv.Itoa(i))
			if err == nil {
				break
			}
		}
		if err != nil {
			t.setError(fmt.Errorf("failed to listen on a free port[%v-%v]:%v\n",
				t.config.DataPortRange[0], t.config.DataPortRange[1], err))
			return 0
		}
	} else if len(t.config.Ports) == 0 || len(t.config.Ports) < (index+1) {
		t.listener, err = net.Listen("tcp", ":0")
		if err != nil {
			t.setError(fmt.Errorf("failed to listen on a free port:%v\n", err))
			return 0
		}
	} else {
		t.listener, err = net.Listen("tcp", ":"+t.config.Ports[index])
		if err != nil {
			t.setError(fmt.Errorf("failed to listen on port %v:%v\n", t.config.Ports[index], err))
			return 0
		}
	}

	port := t.listener.Addr().(*net.TCPAddr).Port

	tListener := t.listener.(*net.TCPListener)
	err = tListener.SetDeadline(time.Now().Add(time.Second * 600))
	if err != nil {
		t.setError(fmt.Errorf("failed to set deadline:%v\n", err))
		return 0
	}

	gplog.Debug("Listening on addr %v, seg-id %v cmd-id %v", t.listener.Addr(), t.config.SegID, t.config.CmdID)

	return port
}

func (t *ServerBase) isDone() bool {
	return t.numFinished == t.numConn
}

func (t *ServerBase) setError(err error) {
	t.emu.Lock()
	defer t.emu.Unlock()
	t.err = err
}

func (t *ServerBase) WaitForFinished() {
	for {
		time.Sleep(50 * time.Millisecond)

		t.emu.Lock()
		err := t.err
		t.emu.Unlock()

		if err != nil {
			break
		}

		done := t.isDone()
		if done {
			break
		}
	}
}

func (t *ServerBase) increase() {
	atomic.AddInt32(&t.numFinished, 1)
}

func (t *ServerBase) Serve() {
	go t.serve()
}

func (t *ServerBase) serve() {
	defer t.wg.Done()

	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.quit:
				return
			default:
				gplog.Error("Listener.Accept() failed: %v", err)
				t.setError(err)
			}
			continue
		}

		t.handler.handleConnection(conn)
	}
}

func (t *ServerBase) Err() error {
	return t.err
}

func createServerImpl(config *Config) Server {
	index := 0
	if config.SegID >= 0 {
		index = config.SegID
	}

	if len(config.NumClients) > 0 && len(config.NumClients) > index {
		nc, _ := strconv.Atoi(config.NumClients[index])
		if nc > 1 {
			return NewConcurrentServer(int32(nc), config)
		}
	}

	return NewOneTimeServer(config)
}
