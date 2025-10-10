package dbconn

/*
 * This file contains structs and functions related to connecting to a database
 * and executing queries.
 */

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/cloudberry-go-libs/gplog"
	"github.com/apache/cloudberry-go-libs/operating"

	/*
	 * We previously used github.com/lib/pq as our Postgres driver,
	 * but it had a bug with the way it handled certain encodings.
	 * pgx seems to handle these encodings properly.
	 */
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

/*
 * While the sqlx.DB struct (and indirectly the sql.DB struct) maintains its own
 * connection pool, there is no guarantee of session-level consistency between
 * queries and we require that level of control in some cases.  Also, while
 * sql.Conn is a struct that represents a single session, there is no
 * sqlx.Conn equivalent we could use.
 *
 * Thus, DBConn maintains its own connection pool of sqlx.DBs (all set to have
 * exactly one database connection each) in an array, such that callers can
 * create NumConns goroutines and assign each an index from 0 to NumConns to
 * guarantee that each goroutine gets its own connection that exhibits single-
 * session behavior.  The Exec, Select, and Get functions are set up to default
 * to the first connection (index 0), so the DBConn will still exhibit session-
 * like behavior if no connection is specified, and other functions that want to
 * execute in serial should pass in a 0 wherever a connection number is needed.
 */

/*
for reference, gpbackup/go-common-go-lib DBConn,

type DBConn struct {
	ConnPool []*sqlx.DB
	NumConns int
	Driver   DBDriver
	User     string
	DBName   string
	Host     string
	Port     int
	Tx       []*sqlx.Tx
	Version  GPDBVersion
}
*/

// DBConn -- changed
type DBConn struct {
	ConnPool []*sqlx.DB
	NumConns int
	Driver   DBDriver
	User     string
	Password string // extra, vs go-common-go-lib DBConn
	DBName   string
	Host     string
	Port     int
	Tx       []*sqlx.Tx
	Version  GPDBVersion
}

/*
 * Structs and functions for testing database functions
 */

type DBDriver interface {
	Connect(driverName string, dataSourceName string) (*sqlx.DB, error)
}

type GPDBDriver struct {
}

func (driver GPDBDriver) Connect(driverName string, dataSourceName string) (*sqlx.DB, error) {
	return sqlx.Connect(driverName, dataSourceName)
}

/*
 * Database functions
 */

func NewDBConnFromEnvironment(dbname string) *DBConn {
	if dbname == "" {
		gplog.Fatal(errors.New("No database provided"), "")
	}

	username := operating.System.Getenv("PGUSER")
	if username == "" {
		currentUser, _ := operating.System.CurrentUser()
		username = currentUser.Username
	}
	host := operating.System.Getenv("PGHOST")
	if host == "" {
		host, _ = operating.System.Hostname()
	}
	port, err := strconv.Atoi(operating.System.Getenv("PGPORT"))
	if err != nil {
		port = 5432
	}

	return NewDBConn(dbname, username, host, port)
}

func NewDBConnWithPassword(dbname, username, password, host string, port int) *DBConn {
	conn := NewDBConn(dbname, username, host, port)
	conn.Password = password
	return conn
}

func NewDBConn(dbname, username, host string, port int) *DBConn {
	if dbname == "" {
		gplog.Fatal(errors.New("No database provided"), "")
	}

	if username == "" {
		gplog.Fatal(errors.New("No username provided"), "")
	}

	if host == "" {
		gplog.Fatal(errors.New("No host provided"), "")
	}

	return &DBConn{
		ConnPool: nil,
		NumConns: 0,
		Driver:   GPDBDriver{},
		User:     username,
		Password: "",
		DBName:   dbname,
		Host:     host,
		Port:     port,
		Tx:       nil,
		Version:  GPDBVersion{},
	}
}

func (dbconn *DBConn) MustBegin(whichConn ...int) {
	err := dbconn.Begin(whichConn...)
	gplog.FatalOnError(err)
}

func (dbconn *DBConn) Begin(whichConn ...int) error {
	connNum := dbconn.ValidateConnNum(whichConn...)
	if dbconn.Tx[connNum] != nil {
		return errors.New("Cannot begin transaction; there is already a transaction in progress")
	}
	var err error
	dbconn.Tx[connNum], err = dbconn.ConnPool[connNum].Beginx()
	if err != nil {
		return err
	}
	_, err = dbconn.Exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", connNum)
	return err
}

func (dbconn *DBConn) Close() {
	if dbconn.ConnPool != nil {
		for _, conn := range dbconn.ConnPool {
			if conn != nil {
				_ = conn.Close()
			}
		}
		dbconn.ConnPool = nil
		dbconn.Tx = nil
		dbconn.NumConns = 0
	}
}

func (dbconn *DBConn) MustCommit(whichConn ...int) {
	err := dbconn.Commit(whichConn...)
	gplog.FatalOnError(err)
}

func (dbconn *DBConn) Commit(whichConn ...int) error {
	connNum := dbconn.ValidateConnNum(whichConn...)
	if dbconn.Tx[connNum] == nil {
		return errors.New("Cannot commit transaction; there is no transaction in progress")
	}
	err := dbconn.Tx[connNum].Commit()
	dbconn.Tx[connNum] = nil
	return err
}

func (dbconn *DBConn) MustRollback(whichConn ...int) {
	err := dbconn.Rollback(whichConn...)
	gplog.FatalOnError(err)
}

func (dbconn *DBConn) Rollback(whichConn ...int) error {
	connNum := dbconn.ValidateConnNum(whichConn...)
	if dbconn.Tx[connNum] == nil {
		return errors.New("Cannot rollback transaction; there is no transaction in progress")
	}
	err := dbconn.Tx[connNum].Rollback()
	dbconn.Tx[connNum] = nil
	return err
}

func (dbconn *DBConn) MustConnect(numConns int) {
	err := dbconn.Connect(numConns)
	gplog.FatalOnError(err)
}

func (dbconn *DBConn) Connect(numConns int) error {
	var connStr string

	if numConns < 1 {
		return errors.Errorf("Must specify a connection pool size that is a positive integer")
	}
	if dbconn.ConnPool != nil {
		return errors.Errorf("The database connection must be closed before reusing the connection")
	}
	// This string takes in the literal user/database names. They do not need to be escaped or quoted.
	if len(dbconn.Password) == 0 {
		connStr = fmt.Sprintf("postgres://%s@%s:%d/%s?sslmode=disable", dbconn.User, dbconn.Host, dbconn.Port, dbconn.DBName)
	} else {
		connStr = fmt.Sprintf("user=%v password='%v' host=%v port=%v dbname=%v",
			dbconn.User, dbconn.Password, dbconn.Host, dbconn.Port, dbconn.DBName)
	}
	dbconn.ConnPool = make([]*sqlx.DB, numConns)
	for i := 0; i < numConns; i++ {
		conn, err := dbconn.Driver.Connect("pgx", connStr)
		err = dbconn.handleConnectionError(err)
		if err != nil {
			return err
		}
		conn.SetMaxOpenConns(1)
		conn.SetMaxIdleConns(1)
		dbconn.ConnPool[i] = conn
	}
	dbconn.Tx = make([]*sqlx.Tx, numConns)
	dbconn.NumConns = numConns
	version, err := InitializeVersion(dbconn)
	if err != nil {
		return errors.Wrap(err, "Failed to determine database version")
	}
	dbconn.Version = version
	return nil
}

func (dbconn *DBConn) handleConnectionError(err error) error {
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			if strings.Contains(err.Error(), "pq: role") {
				return errors.Errorf(`Role "%s" does not exist on %s:%d, exiting`, dbconn.User, dbconn.Host, dbconn.Port)
			} else if strings.Contains(err.Error(), "pq: database") {
				return errors.Errorf(`Database "%s" does not exist on %s:%d, exiting`, dbconn.DBName, dbconn.Host, dbconn.Port)
			}
		} else if strings.Contains(err.Error(), "connection refused") {
			return errors.Errorf(`could not connect to server: Connection refused
	Is the server running on host "%s" and accepting
	TCP/IP connections on port %d?`, dbconn.Host, dbconn.Port)
		} else {
			return errors.Errorf("%v (%s:%d)", err, dbconn.Host, dbconn.Port)
		}
	}

	return err
}

/*
 * Wrapper functions for built-in sqlx and database/sql functionality; they will
 * automatically execute the query as part of an existing transaction if one is
 * in progress, to ensure that successive queries occur in one transaction without
 * requiring that to be ensured at the call site.
 */

func (dbconn *DBConn) Exec(query string, whichConn ...int) (sql.Result, error) {
	connNum := dbconn.ValidateConnNum(whichConn...)
	if dbconn.Tx[connNum] != nil {
		return dbconn.Tx[connNum].Exec(query)
	}
	return dbconn.ConnPool[connNum].Exec(query)
}

func (dbconn *DBConn) MustExec(query string, whichConn ...int) {
	_, err := dbconn.Exec(query, whichConn...)
	gplog.FatalOnError(err)
}

func (dbconn *DBConn) ExecContext(queryContext context.Context, query string, whichConn ...int) (sql.Result, error) {
	connNum := dbconn.ValidateConnNum(whichConn...)
	if dbconn.Tx[connNum] != nil {
		return dbconn.Tx[connNum].ExecContext(queryContext, query)
	}
	return dbconn.ConnPool[connNum].ExecContext(queryContext, query)
}

func (dbconn *DBConn) MustExecContext(queryContext context.Context, query string, whichConn ...int) {
	_, err := dbconn.ExecContext(queryContext, query, whichConn...)
	gplog.FatalOnError(err)
}

func (dbconn *DBConn) GetWithArgs(destination interface{}, query string, args ...interface{}) error {
	if dbconn.Tx[0] != nil {
		return dbconn.Tx[0].Get(destination, query, args...)
	}
	return dbconn.ConnPool[0].Get(destination, query, args...)
}

func (dbconn *DBConn) Get(destination interface{}, query string, whichConn ...int) error {
	connNum := dbconn.ValidateConnNum(whichConn...)
	if dbconn.Tx[connNum] != nil {
		return dbconn.Tx[connNum].Get(destination, query)
	}
	return dbconn.ConnPool[connNum].Get(destination, query)
}

func (dbconn *DBConn) SelectWithArgs(destination interface{}, query string, args ...interface{}) error {
	if dbconn.Tx[0] != nil {
		return dbconn.Tx[0].Select(destination, query, args...)
	}
	return dbconn.ConnPool[0].Select(destination, query, args...)
}

func (dbconn *DBConn) Select(destination interface{}, query string, whichConn ...int) error {
	connNum := dbconn.ValidateConnNum(whichConn...)
	if dbconn.Tx[connNum] != nil {
		return dbconn.Tx[connNum].Select(destination, query)
	}
	return dbconn.ConnPool[connNum].Select(destination, query)
}

func (dbconn *DBConn) QueryWithArgs(query string, args ...interface{}) (*sqlx.Rows, error) {
	if dbconn.Tx[0] != nil {
		return dbconn.Tx[0].Queryx(query, args...)
	}
	return dbconn.ConnPool[0].Queryx(query, args...)
}

func (dbconn *DBConn) Query(query string, whichConn ...int) (*sqlx.Rows, error) {
	connNum := dbconn.ValidateConnNum(whichConn...)
	if dbconn.Tx[connNum] != nil {
		return dbconn.Tx[connNum].Queryx(query)
	}
	return dbconn.ConnPool[connNum].Queryx(query)
}

/*
 * Ensure there isn't a mismatch between the connection pool size and number of
 * jobs, and default to using the first connection if no number is given.
 */
func (dbconn *DBConn) ValidateConnNum(whichConn ...int) int {
	if len(whichConn) == 0 {
		return 0
	}
	if len(whichConn) != 1 {
		gplog.Fatal(errors.Errorf("At most one connection number may be specified for a given connection"), "")
	}
	if whichConn[0] < 0 || whichConn[0] >= dbconn.NumConns {
		gplog.Fatal(errors.Errorf("Invalid connection number: %d", whichConn[0]), "")
	}
	return whichConn[0]
}

/*
 * This is a convenience function for Select() when we're selecting a single
 * string that may be NULL or not exist.  We can't use Get() because that
 * expects exactly one string and will panic if no rows are returned, even if
 * using a sql.NullString.
 *
 * SelectString calls SelectStringSlice and returns the first value instead of
 * calling QueryRowx because that function doesn't indicate if there were more
 * rows available to be returned, and we don't want to silently ignore that if
 * only one row was expected for a given query but multiple were returned.
 */
func MustSelectString(connection *DBConn, query string, whichConn ...int) string {
	str, err := SelectString(connection, query, whichConn...)
	gplog.FatalOnError(err)
	return str
}

func SelectString(connection *DBConn, query string, whichConn ...int) (string, error) {
	results, err := SelectStringSlice(connection, query, whichConn...)
	if err != nil {
		return "", err
	}
	if len(results) == 1 {
		return results[0], nil
	} else if len(results) > 1 {
		return "", errors.Errorf("Too many rows returned from query: got %d rows, expected 1 row", len(results))
	}
	return "", nil
}

/*
 * This is a convenience function for Select() when we're selecting a single
 * column of strings that may be NULL.  Select requires defining a struct for
 * each call, and this function uses the underlying sql functions instead of
 * sqlx functions to avoid needing to "SELECT [column] AS [struct field]" with
 * a generic struct or the like.
 *
 * It also gives a nicer error message in the event that a query is called with
 * multiple columns, where using a generic struct gives an opaque "missing
 * destination name" error.
 */
func MustSelectStringSlice(connection *DBConn, query string, whichConn ...int) []string {
	str, err := SelectStringSlice(connection, query, whichConn...)
	gplog.FatalOnError(err)
	return str
}

func SelectStringSlice(connection *DBConn, query string, whichConn ...int) ([]string, error) {
	connNum := connection.ValidateConnNum(whichConn...)
	rows, err := connection.Query(query, connNum)
	if err != nil {
		return []string{}, err
	}
	if cols, _ := rows.Rows.Columns(); len(cols) > 1 {
		return []string{}, errors.Errorf("Too many columns returned from query: got %d columns, expected 1 column", len(cols))
	}
	retval := make([]string, 0)
	for rows.Rows.Next() {
		var result sql.NullString
		err = rows.Rows.Scan(&result)
		if err != nil {
			return []string{}, err
		}
		retval = append(retval, result.String)
	}
	if rows.Rows.Err() != nil {
		return []string{}, rows.Rows.Err()
	}
	return retval, nil
}
