package helper

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"net"
	"os"
	"runtime/debug"

	"github.com/cloudberry-contrib/cbcopy/utils"
	"github.com/apache/cloudberry-go-libs/gplog"
)

type Helper struct {
	config        *Config
	metaFilePath  string
	wasTerminated bool
	errCode       int
}

func NewHelper(cfg *Config) *Helper {
	return &Helper{
		config: cfg,
	}
}

func (h *Helper) handleResolve() error {
	addr, err := net.LookupIP(h.config.ToResolve)
	if err != nil {
		return fmt.Errorf("failed to resolve %s: %v", h.config.ToResolve, err)
	}

	if h.config.SegID >= 0 {
		fmt.Printf("%d\t%s\n", h.config.SegID, addr[0].String())
	} else {
		fmt.Println(addr[0].String())
	}

	return nil
}

func (h *Helper) handleMD5XOR() error {
	hash := md5.New()
	scanner := bufio.NewScanner(os.Stdin)
	xor := make([]byte, 16) // 16 Bytes, 128 Bits

	for scanner.Scan() {
		hash.Write(scanner.Bytes())
		sum := hash.Sum(nil)
		hash.Reset()

		for id, val := range sum {
			xor[id] = val ^ xor[id]
		}
	}

	_, err := fmt.Fprintf(os.Stdout, "%x\n", xor)

	return err
}

func (h *Helper) handleListen() error {
	server := createServerImpl(h.config)

	port := server.Start()
	if server.Err() != nil {
		return server.Err()
	}

	err := h.writeFile(h.config.CmdID, h.config.SegID, port)
	if err != nil {
		return err
	}

	server.Serve()

	server.WaitForFinished()

	return server.Err()
}

func (h *Helper) handleClient() error {
	client := createClientImpl(h.config)
	return client.Run()
}

func (h *Helper) writeFile(cmdID string, segID int, port int) error {
	h.metaFilePath = fmt.Sprintf("/tmp/cbcopy-%s-%d.txt", h.config.CmdID, h.config.SegID)

	metaFile, err := os.Create(h.metaFilePath)
	if err != nil {
		return err
	}

	defer func(metaFile *os.File) {
		_ = metaFile.Close()
	}(metaFile)

	_, err = metaFile.WriteString(fmt.Sprintf("%s\t%d\t%d\n", cmdID, segID, port))

	return err
}

func (h *Helper) Start() {
	gplog.InitializeLogging("cbcopy_helper", "")

	gplog.SetVerbosity(gplog.LOGERROR)

	utils.InitializeSignalHandler(func(isTerminated bool) {
		h.Cleanup(isTerminated)
	}, "cbcopy_helper process", &h.wasTerminated)

	gplog.Debug("Helper started with args %v", os.Args)

}

func (h *Helper) Stop() {
	if err := recover(); err != nil {
		gplog.Error("Panic recovered: %v", err)
		gplog.Error("Stack trace: %s", debug.Stack())
	}

	gplog.Debug("Helper stopping")
	h.errCode = gplog.GetErrorCode()

	h.Cleanup(false)
}

func (h *Helper) Cleanup(isTerminated bool) {
	if _, err := os.Stat(h.metaFilePath); err == nil {
		_ = os.Remove(h.metaFilePath)
	}
}

func (h *Helper) Run() error {
	h.Start()
	defer h.Stop()

	shouldContinue, err := h.config.Parse()
	if err != nil {
		return err
	}

	if !shouldContinue {
		return nil
	}

	switch {
	case h.config.ToResolve != "":
		return h.handleResolve()
	case h.config.MD5XORMode:
		return h.handleMD5XOR()
	case h.config.ListenMode:
		return h.handleListen()
	default:
		return h.handleClient()
	}
}

func (h *Helper) GetErrCode() int {
	return h.errCode
}
