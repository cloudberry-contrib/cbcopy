package helper

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/pkg/errors"
	flag "github.com/spf13/pflag"
)

const (
	DirectionModeSend    = "send"
	DirectionModeReceive = "receive"
)

type Config struct {
	Hosts         CommaStringFlags
	Ports         CommaIntFlags
	DataPortRange DashIntFlags
	ListenMode    bool
	NoCompression bool
	ToResolve     string
	CmdID         string
	SegID         int
	PrintVersion  bool
	MD5XORMode    bool
	TransCompType string
	NumClients    CommaIntFlags
	Direction     string
	NumDests      int
}

func NewConfig() *Config {
	return &Config{
		SegID:         -2,
		TransCompType: "gzip",
		Direction:     DirectionModeReceive,
	}
}

func (c *Config) InitFlags() {
	flag.StringVar(&c.ToResolve, "resolve", "", "The hostname to get address of")
	flag.BoolVar(&c.MD5XORMode, "md5xor", false, "Calculate the input's md5xor value")
	flag.StringVar(&c.CmdID, "cmd-id", "", "The copy command ID")
	flag.Var(&c.Hosts, "host", "The host to send data to")
	flag.Var(&c.Ports, "port", "The port to send data to")
	flag.IntVar(&c.SegID, "seg-id", -2, "The segment ID that running the helper")
	flag.BoolVar(&c.ListenMode, "listen", false, "Listen, rather than initiate a connection to a remote host")
	flag.Var(&c.DataPortRange, "data-port-range", "The range of listening port number to choose for receiving data on dest cluster")
	flag.BoolVar(&c.NoCompression, "no-compression", false, "Transfer the plain data, instead of compressing as Snappy format")
	flag.BoolVar(&c.PrintVersion, "version", false, "Print version")
	flag.StringVar(&c.TransCompType, "compress-type", "gzip", "Data compression algorithm, \"gzip\" or \"snappy\"")
	flag.Var(&c.NumClients, "client-numbers", "Number of clients")
	flag.StringVar(&c.Direction, "direction", DirectionModeSend, "Data flow direction, 'send' (stdin to net) or 'receive' (net to stdout)")
	flag.IntVar(&c.NumDests, "num-dests", 0, "Total number of destination segments, used for round-robin calculation")
}

func (c *Config) Parse() (bool, error) {
	c.InitFlags()

	if len(os.Args) == 1 {
		flag.PrintDefaults()
		return false, nil
	}

	flag.Parse()

	if c.PrintVersion {
		fmt.Printf("cbcopy_helper version %s\n", utils.Version)
		return false, nil
	}

	return true, c.validate()
}

func (c *Config) getChangedFlagNum(options []string) int {
	numSet := 0
	for _, o := range options {
		if f := flag.Lookup(o); f != nil && f.Changed {
			numSet++
		}
	}

	return numSet
}

func (c *Config) checkExclusiveFlags(options ...string) error {
	numSet := c.getChangedFlagNum(options)

	if numSet > 1 {
		return fmt.Errorf("the following flags may not be specified together: %s\n", strings.Join(options, ", "))
	}

	return nil
}

func (c *Config) checkMandatoryFlags(options ...string) error {
	numSet := c.getChangedFlagNum(options)

	if numSet != len(options) {
		return fmt.Errorf("the following flags must be specified: %s\n", strings.Join(options, ", "))
	}

	return nil
}

func (c *Config) validateCompressType(cType string) error {
	if len(cType) == 0 {
		return nil
	}

	if cType != "gzip" && cType != "snappy" {
		return fmt.Errorf("--compress-type must be \"gzip\" or \"snappy\"\n")
	}

	return nil
}

func (c *Config) parseDataPortRange(portPair DashIntFlags) error {
	if len(portPair) == 0 {
		return nil
	}

	bPort := portPair[0]
	ePort := portPair[1]

	if bPort < 1024 || bPort > 65535 {
		return fmt.Errorf("\"--data-port-range\" is invalid: the valid value should be 1024-65535\n")
	}

	if ePort < 1024 || ePort > 65535 {
		return fmt.Errorf("\"--data-port-range\" is invalid: the valid value should be 1024-65535\n")
	}

	if bPort >= ePort {
		return fmt.Errorf("\"--data-port-range\" is invalid: the first port number should be less than the second port number\n")
	}

	return nil
}

func (c *Config) validate() error {
	if flag.NArg() > 0 {
		return fmt.Errorf("there is an argument remaining after flags have been processed: \"%s\"\n", flag.Arg(0))
	}

	if err := c.checkExclusiveFlags("resolve", "listen", "md5xor"); err != nil {
		return err
	}
	if err := c.checkExclusiveFlags("resolve", "no-compression", "md5xor"); err != nil {
		return err
	}
	if err := c.checkExclusiveFlags("resolve", "port", "data-port-range", "md5xor"); err != nil {
		return err
	}
	if err := c.checkExclusiveFlags("resolve", "client-numbers"); err != nil {
		return err
	}
	if err := c.checkExclusiveFlags("resolve", "direction"); err != nil {
		return err
	}

	if err := c.checkExclusiveFlags("host", "md5xor"); err != nil {
		return err
	}
	if err := c.checkExclusiveFlags("host", "listen"); err != nil {
		return err
	}
	if err := c.checkExclusiveFlags("host", "client-numbers"); err != nil {
		return err
	}
	if err := c.checkExclusiveFlags("host", "disable-parallel-mode"); err != nil {
		return err
	}

	if err := c.checkExclusiveFlags("no-compression", "compress-type"); err != nil {
		return err
	}

	if c.ToResolve == "" && c.ListenMode {
		if err := c.checkMandatoryFlags("cmd-id", "seg-id"); err != nil {
			return err
		}
	}
	if c.ToResolve == "" && !c.ListenMode && !c.MD5XORMode {
		if err := c.checkMandatoryFlags("host", "port"); err != nil {
			return err
		}
	}

	if err := c.parseDataPortRange(c.DataPortRange); err != nil {
		return err
	}

	return c.validateCompressType(c.TransCompType)
}

type CommaStringFlags []string

func (c *CommaStringFlags) String() string {
	return fmt.Sprintf("%v", *c)
}

func (c *CommaStringFlags) Set(value string) error {
	for _, e := range strings.Split(value, ",") {
		*c = append(*c, e)
	}
	return nil
}

func (c *CommaStringFlags) Type() string {
	return "CommaStringFlags"
}

type CommaIntFlags []string

func (c *CommaIntFlags) String() string {
	return fmt.Sprintf("%v", *c)
}

func (c *CommaIntFlags) Set(value string) error {
	for _, e := range strings.Split(value, ",") {
		_, err := strconv.Atoi(e)
		if err != nil {
			return errors.New("invalid integer format")
		}

		*c = append(*c, e)
	}
	return nil
}

func (c *CommaIntFlags) Type() string {
	return "CommaIntFlags"
}

type DashIntFlags []int

func (d *DashIntFlags) String() string {
	return fmt.Sprintf("%v", *d)
}

func (d *DashIntFlags) Set(value string) error {
	sl := strings.Split(value, "-")
	if len(sl) != 2 {
		return errors.New("invalid dash format")
	}

	first, err := strconv.Atoi(sl[0])
	if err != nil {
		return errors.New("invalid integer format")
	}

	*d = append(*d, first)

	second, err := strconv.Atoi(sl[1])
	if err != nil {
		return errors.New("invalid integer format")
	}

	*d = append(*d, second)

	return nil
}

func (d *DashIntFlags) Type() string {
	return "DashIntFlags"
}
