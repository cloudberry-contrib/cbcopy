package utils

/*
 * This file contains miscellaneous functions that are generally useful and
 * don't fit into any other file.
 */

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"

	"github.com/cloudberry-contrib/cbcopy/internal/dbconn"
	"github.com/apache/cloudberry-go-libs/gplog"
	"github.com/apache/cloudberry-go-libs/operating"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

const MINIMUM_GPDB4_VERSION = "4.3.17"
const MINIMUM_GPDB5_VERSION = "5.1.0"

/*
 * General helper functions
 */

func OpenFileForWrite(filename string) (*os.File, error) {
	return os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
}

func WriteToFileAndMakeReadOnly(filename string, contents []byte) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	_, err = file.Write(contents)
	if err != nil {
		return err
	}

	err = file.Sync()
	if err != nil {
		return err
	}

	err = file.Chmod(0444)
	if err != nil {
		return err
	}

	return file.Close()
}

// Dollar-quoting logic is based on appendStringLiteralDQ() in pg_dump.
func DollarQuoteString(literal string) string {
	delimStr := "_XXXXXXX"
	quoteStr := ""
	for i := range delimStr {
		testStr := "$" + delimStr[0:i]
		if !strings.Contains(literal, testStr) {
			quoteStr = testStr + "$"
			break
		}
	}
	return quoteStr + literal + quoteStr
}

// This function assumes that all identifiers are already appropriately quoted
func MakeFQN(schema string, object string) string {
	return fmt.Sprintf("%s.%s", schema, object)
}

func ValidateFQNs(tableList []string) error {
	validFormat := regexp.MustCompile(`^[^.]+\.[^.]+$`)
	for _, fqn := range tableList {
		if !validFormat.Match([]byte(fqn)) {
			return errors.Errorf(`Table "%s" is not correctly fully-qualified.  Please ensure table is in the format "schema.table" and both the schema and table does not contain a dot (.).`, fqn)
		}
	}

	return nil
}

func ValidateFullPath(path string) error {
	if len(path) > 0 && !(strings.HasPrefix(path, "/") || strings.HasPrefix(path, "~")) {
		return errors.Errorf("%s is not an absolute path.", path)
	}
	return nil
}

// A description of compression levels for some compression type
type CompressionLevelsDescription struct {
	Min int
	Max int
}

func ValidateCompressionTypeAndLevel(compressionType string, compressionLevel int) error {
	compressionLevelsForType := map[string]CompressionLevelsDescription{
		"gzip": {Min: 1, Max: 9},
		"zstd": {Min: 1, Max: 19},
	}

	if levelsDescription, ok := compressionLevelsForType[compressionType]; ok {
		if compressionLevel < levelsDescription.Min || compressionLevel > levelsDescription.Max {
			return fmt.Errorf("compression type '%s' only allows compression levels between %d and %d, but the provided level is %d", compressionType, levelsDescription.Min, levelsDescription.Max, compressionLevel)
		}
	} else {
		return fmt.Errorf("unknown compression type '%s'", compressionType)
	}

	return nil
}

func InitializeSignalHandler(cleanupFunc func(bool), procDesc string, termFlag *bool) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for range signalChan {
			fmt.Println() // Add newline after "^C" is printed
			gplog.Warn("Received a termination signal, aborting %s", procDesc)
			*termFlag = true
			cleanupFunc(true)
			os.Exit(2)
		}
	}()
}

func TerminateHangingCopySessions(conn *dbconn.DBConn, appName string) {
	cname := "procpid"
	switch conn.Version.Type {
	case dbconn.GPDB:
		if conn.Version.AtLeast("6") {
			cname = "pid"
		}
	case dbconn.HDW:
		if conn.Version.AtLeast("3") {
			cname = "pid"
		}
	case dbconn.CBDB, dbconn.PGSQL:
		cname = "pid"
	default:
		gplog.Error("Unsupported database type: %s", conn.Version.Type)
		return
	}

	query := fmt.Sprintf(`SELECT
	pg_terminate_backend(%v)
FROM pg_stat_activity
WHERE application_name = '%s'
AND %v <> pg_backend_pid()`, cname, appName, cname)
	// We don't check the error as the connection may have finished or been previously terminated

	gplog.Debug("TerminateHangingCopySessions, query is: %v", query)
	_, _ = conn.Exec(query)
}

func ArrayIsDuplicated(elems []string) bool {
	elemsMap := make(map[string]bool)

	for _, v := range elems {
		if _, exist := elemsMap[v]; !exist {
			elemsMap[v] = true
		} else {
			return true
		}
	}

	return false
}

func ValidateGPDBVersionCompatibility(connectionPool *dbconn.DBConn) {
	if connectionPool.Version.IsGPDB() && connectionPool.Version.Before(MINIMUM_GPDB4_VERSION) {
		gplog.Fatal(errors.Errorf(`GPDB version %s is not supported. Please upgrade to GPDB %s.0 or later.`, connectionPool.Version.VersionString, MINIMUM_GPDB4_VERSION), "")
	} else if connectionPool.Version.IsGPDB() && connectionPool.Version.Is("5") && connectionPool.Version.Before(MINIMUM_GPDB5_VERSION) {
		gplog.Fatal(errors.Errorf(`GPDB version %s is not supported. Please upgrade to GPDB %s or later.`, connectionPool.Version.VersionString, MINIMUM_GPDB5_VERSION), "")
	}
}

func Exists(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

func CurrentTimestamp() string {
	return operating.System.Now().Format("20060102150405")
}

func HandleSingleDashes(args []string) []string {
	r, _ := regexp.Compile(`^-(\w{2,})`)
	var newArgs []string
	for _, arg := range args {
		newArg := r.ReplaceAllString(arg, "--$1")
		newArgs = append(newArgs, newArg)
	}
	return newArgs
}

func ReadTableFile(filename string) ([]string, error) {
	tables := make([]string, 0)

	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer f.Close()
	sc := bufio.NewScanner(f)

	for sc.Scan() {
		tables = append(tables, sc.Text())
	}
	return tables, nil
}

func OpenDataFile(filename string) *os.File {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	gplog.FatalOnError(err)
	return f
}

func WriteDataFile(f *os.File, line string) error {
	fmu.Lock()
	defer fmu.Unlock()
	_, err := f.WriteString(line)
	return err
}

func CloseDataFile(f *os.File) {
	f.Close()
}

func RedirectStream(reader io.Reader, writeCloser io.WriteCloser) error {
	buffer := make([]byte, 8*1024*1024)

	defer func() {
		// snappy.NewBufferedWriter() or net.Conn needs a Close()
		// don't Close() the os.Stdout, our test has a hack to remove
		// the last two summary lines generated by `go test`
		if conn, ok := writeCloser.(net.Conn); ok {
			writeCloser.Close()
			gplog.Debug("Connection to %v is closed.", conn.RemoteAddr())
		}
	}()

	written, err := io.CopyBuffer(writeCloser, reader, buffer)
	if written == 0 {
		gplog.Debug("Written zero byte")
	}

	if err != nil {
		return fmt.Errorf("failed to redirect stream: %v", err)
	}

	return nil
}

func GetVersion() string {
	return Version
}

type FqnStruct struct {
	SchemaName string
	TableName  string
}

// https://github.com/greenplum-db/gpbackup/commit/6e9829cf5c12fb8e66cecd8143ac0a7379e44d01
func QuoteTableNames(conn *dbconn.DBConn, tableNames []string) ([]string, error) {
	if len(tableNames) == 0 {
		return []string{}, nil
	}

	// Properly escape single quote before running quote ident. Postgres
	// quote_ident escapes single quotes by doubling them
	escapedTables := make([]string, 0)
	for _, v := range tableNames {
		escapedTables = append(escapedTables, EscapeSingleQuotes(v))
	}

	fqnSlice, err := SeparateSchemaAndTable(escapedTables)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)

	quoteIdentTableFQNQuery := `SELECT quote_ident('%s') AS schemaname, quote_ident('%s') AS tablename`
	for _, fqn := range fqnSlice {
		queryResultTable := make([]FqnStruct, 0)
		query := fmt.Sprintf(quoteIdentTableFQNQuery, fqn.SchemaName, fqn.TableName)
		gplog.Debug("QuoteTableNames, query is %v", query)
		err := conn.Select(&queryResultTable, query)
		if err != nil {
			return nil, err
		}
		quoted := queryResultTable[0].SchemaName + "." + queryResultTable[0].TableName
		result = append(result, quoted)
	}

	return result, nil
}

func SeparateSchemaAndTable(tableNames []string) ([]FqnStruct, error) {
	fqnSlice := make([]FqnStruct, 0)
	for _, fqn := range tableNames {
		parts := strings.Split(fqn, ".")
		if len(parts) > 2 {
			return nil, errors.Errorf("cannot process an Fully Qualified Name with embedded dots yet: %s", fqn)
		}
		if len(parts) < 2 {
			return nil, errors.Errorf("Fully Qualified Names require a minimum of one dot, specifying the schema and table. Cannot process: %s", fqn)
		}
		schema := parts[0]
		table := parts[1]
		if schema == "" || table == "" {
			return nil, errors.Errorf("Fully Qualified Names must specify the schema and table. Cannot process: %s", fqn)
		}

		currFqn := FqnStruct{
			SchemaName: schema,
			TableName:  table,
		}

		fqnSlice = append(fqnSlice, currFqn)
	}

	return fqnSlice, nil
}

func ReadTableFileByFlag(flagSet *pflag.FlagSet, flag string) ([]string, error) {
	filename, err := flagSet.GetString(flag)
	if err != nil {
		return nil, err
	}

	if len(filename) > 0 {
		tables, err := ReadTableFile(filename)
		if err != nil {
			return nil, err
		}
		return tables, nil
	}

	return nil, nil
}

func ParseMappingFile(lines []string) map[string]string {
	mapping := make(map[string]string)

	for _, l := range lines {
		items := strings.Split(l, ",")
		if len(items) != 2 {
			gplog.Fatal(errors.Errorf(`invalid mapping file content [%s]: every line
			should have two fields, seperated by comma. the first field is the source name,
			the second field is the target name`, l), "")
		}

		mapping[items[0]] = items[1]
	}

	return mapping
}

func ParseSchemaMappingFile(lines []string) ([]string, []string) {
	source := make([]string, 0)
	dest := make([]string, 0)

	for _, l := range lines {
		items := strings.Split(l, ",")
		if len(items) != 2 {
			gplog.Fatal(errors.Errorf(`invalid schema mapping file content [%s]: every line
			should have two fields, seperated by comma. the first field is the source schema 
			name, the second field is the target schema name`, l), "")
		}

		source = append(source, items[0])
		dest = append(dest, items[1])
	}

	if lines != nil {
		if len(source) == 0 {
			gplog.Fatal(errors.Errorf(`schema mapping file should have at lease one record`), "")
		}

		return source, dest
	}

	return nil, nil
}
