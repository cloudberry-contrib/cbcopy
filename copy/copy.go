package copy

import (
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type SessionGUCs struct {
	ClientEncoding string `db:"client_encoding"`
}

type Application struct {
	srcManageConn       *dbconn.DBConn
	destManageConn      *dbconn.DBConn
	destSegmentsIpInfo  []utils.SegmentIpInfo
	srcSegmentsHostInfo []utils.SegmentHostInfo
	queryManager        *QueryManager
	queryWrapper        *QueryWrapper
	timestamp           string
	applicationName     string
	convertDDL          bool
	encodingGuc         SessionGUCs
}

func NewApplication() *Application {
	qm := NewQueryManager()
	return &Application{
		queryManager: qm,
		queryWrapper: NewQueryWrapper(qm),
	}
}

func (app *Application) Initialize(cmd *cobra.Command) {
	app.timestamp = utils.CurrentTimestamp()
	app.applicationName = "cbcopy_" + app.timestamp

	gplog.SetLogFileNameFunc(func(program, logdir string) string {
		return fmt.Sprintf("%v/%v.log", logdir, app.applicationName)
	})

	gplog.InitializeLogging("cbcopy", "")

	utils.CleanupGroup = &sync.WaitGroup{}
	utils.CleanupGroup.Add(1)

	app.SetFlagDefaults(cmd.Flags())
	utils.CmdFlags = cmd.Flags()

	utils.InitializeSignalHandler(app.doCleanup, "cbcopy process", &utils.WasTerminated)
}

func (app *Application) SetFlagDefaults(flagSet *pflag.FlagSet) {
	flagSet.Bool(option.APPEND, false, "Append destination table if it exists")
	flagSet.StringSlice(option.DBNAME, []string{}, "The database(s) to be copied, separated by commas")
	flagSet.Bool(option.DEBUG, false, "Print debug log messages")
	flagSet.StringSlice(option.DEST_DBNAME, []string{}, "The database(s) in destination cluster to copy to, separated by commas")
	flagSet.String(option.DEST_HOST, "127.0.0.1", "The host of destination cluster")
	flagSet.Int(option.DEST_PORT, 5432, "The port of destination cluster")
	flagSet.StringSlice(option.DEST_TABLE, []string{}, "The renamed dest table(s) for include-table, separated by commas")
	flagSet.String(option.DEST_TABLE_FILE, "", "The renamed dest table(s) for include-table-file, The line format is \"dbname.schema.table\"")
	flagSet.String(option.DEST_USER, "gpadmin", "The user of destination cluster")
	flagSet.StringSlice(option.EXCLUDE_TABLE, []string{}, "Copy all tables except the specified table(s), separated by commas")
	flagSet.String(option.EXCLUDE_TABLE_FILE, "", "Copy all tables except the specified table(s) listed in the file, The line format is \"dbname.schema.table\"")
	flagSet.Bool(option.FULL, false, "Copy full data cluster")
	flagSet.Bool("help", false, "Print help info and exit")
	flagSet.StringSlice(option.INCLUDE_TABLE, []string{}, "Copy only the specified table(s), separated by commas, in the format database.schema.table")
	flagSet.String(option.INCLUDE_TABLE_FILE, "", "Copy only the specified table(s) listed in the file, The line format is \"dbname.schema.table\"")
	flagSet.Int(option.COPY_JOBS, 4, "The maximum number of tables that concurrently copies, valid values are between 1 and 512")
	flagSet.Int(option.METADATA_JOBS, 2, "The maximum number of metadata restore tasks, valid values are between 1 and 512")
	flagSet.Bool(option.METADATA_ONLY, false, "Only copy metadata, do not copy data")
	flagSet.Bool(option.GLOBAL_METADATA_ONLY, false, "Only copy global metadata, do not copy data")
	flagSet.Bool(option.DATA_ONLY, false, "Only copy data, do not copy metadata")
	flagSet.Bool(option.WITH_GLOBALMETA, false, "Copy global meta objects (default: false)")
	flagSet.Bool(option.COMPRESSION, false, "Transfer the compression data, instead of the plain data")
	flagSet.Int(option.ON_SEGMENT_THRESHOLD, 1000000, "Copy between Coordinators directly, if the table has smaller or same number of rows")
	flagSet.Bool(option.QUIET, false, "Suppress non-warning, non-error log messages")
	flagSet.String(option.SOURCE_HOST, "127.0.0.1", "The host of source cluster")
	flagSet.Int(option.SOURCE_PORT, 5432, "The port of source cluster")
	flagSet.String(option.SOURCE_USER, "gpadmin", "The user of source cluster")
	flagSet.Bool(option.TRUNCATE, false, "Truncate destination table if it exists prior to copying data")
	flagSet.StringSlice(option.SCHEMA, []string{}, "The schema(s) to be copied, separated by commas, in the format database.schema")
	flagSet.StringSlice(option.DEST_SCHEMA, []string{}, "The schema(s) in destination database to copy to, separated by commas")
	flagSet.Bool(option.VERBOSE, false, "Print verbose log messages")
	flagSet.Bool(option.VALIDATE, true, "Perform data validation when copy is complete")
	flagSet.String(option.SCHEMA_MAPPING_FILE, "", "Schema mapping file, The line format is \"source_dbname.source_schema,dest_dbname.dest_schema\"")
	flagSet.String(option.OWNER_MAPPING_FILE, "", "Object owner mapping file, The line format is \"source_role_name,dest_role_name\"")
	flagSet.String(option.TABLESPACE, "", "Create objects in this tablespace")
	flagSet.Bool("version", false, "Print version number and exit")
	flagSet.String(option.DATA_PORT_RANGE, "1024-65535", "The range of listening port number to choose for receiving data on dest cluster")
}

func (app *Application) doFlagValidation(cmd *cobra.Command) {
	vm := NewValidatorManager(cmd.Flags())
	err := vm.ValidateAll()
	gplog.FatalOnError(err)
}

func (app *Application) setLoggerVerbosity() {
	if utils.MustGetFlagBool(option.QUIET) {
		gplog.SetVerbosity(gplog.LOGERROR)
	} else if utils.MustGetFlagBool(option.DEBUG) {
		gplog.SetVerbosity(gplog.LOGDEBUG)
	} else if utils.MustGetFlagBool(option.VERBOSE) {
		gplog.SetVerbosity(gplog.LOGVERBOSE)
	}
}

func (app *Application) initializeConnectionPool(dbname, username, host string, port, numConns int) *dbconn.DBConn {
	dbConn := dbconn.NewDBConn(dbname, username, host, port)
	dbConn.MustConnect(numConns)
	utils.ValidateGPDBVersionCompatibility(dbConn)

	qm := NewQueryManager()
	for connNum := 0; connNum < dbConn.NumConns; connNum++ {
		dbConn.MustExec(qm.GetSessionSetupQuery(dbConn, app.applicationName), connNum)
	}

	return dbConn
}

func (app *Application) doSetup() {
	app.setLoggerVerbosity()

	if utils.MustGetFlagBool(option.GLOBAL_METADATA_ONLY) {
		option.MakeIncludeOptions(utils.CmdFlags, CbcopyTestTable)
	}

	gplog.Debug("Starting cbcopy with arguments: [%s]", strings.Join(os.Args, " "))
	gplog.Info("Starting copy (timestamp=%s)...", app.timestamp)

	var err error
	config, err = option.NewOption(utils.CmdFlags)
	gplog.FatalOnError(err)

	gplog.Info("Establishing 1 source db management connection(s)...")
	app.srcManageConn = app.initializeConnectionPool("postgres",
		utils.MustGetFlagString(option.SOURCE_USER),
		utils.MustGetFlagString(option.SOURCE_HOST),
		utils.MustGetFlagInt(option.SOURCE_PORT),
		1)

	if utils.MustGetFlagBool(option.GLOBAL_METADATA_ONLY) {
		err = app.queryManager.CreateTestTable(app.srcManageConn, CbcopyTestTable)
		gplog.FatalOnError(err)
	}

	gplog.Info("Establishing %v dest db management connection(s)...",
		utils.MustGetFlagInt(option.COPY_JOBS))
	app.destManageConn = app.initializeConnectionPool("postgres",
		utils.MustGetFlagString(option.DEST_USER),
		utils.MustGetFlagString(option.DEST_HOST),
		utils.MustGetFlagInt(option.DEST_PORT),
		utils.MustGetFlagInt(option.COPY_JOBS))
}

func (app *Application) initializeConn(srcDbName, destDbName string) (*dbconn.DBConn, *dbconn.DBConn, *dbconn.DBConn, *dbconn.DBConn) {
	var srcMetaConn, destMetaConn, srcConn, destConn *dbconn.DBConn

	gplog.Info("Establishing 1 source db (%v) metadata connection(s)...", srcDbName)
	srcMetaConn = app.initializeConnectionPool(srcDbName,
		utils.MustGetFlagString(option.SOURCE_USER),
		utils.MustGetFlagString(option.SOURCE_HOST),
		utils.MustGetFlagInt(option.SOURCE_PORT),
		1)

	if config.ContainsMetadata(utils.MustGetFlagBool(option.METADATA_ONLY), utils.MustGetFlagBool(option.DATA_ONLY)) {
		app.queryManager.CreateDatabaseIfNotExists(app.destManageConn, destDbName)
	}

	gplog.Info("Establishing %v dest db (%v) metadata connection(s)...", utils.MustGetFlagInt(option.METADATA_JOBS), destDbName)
	destMetaConn = app.initializeConnectionPool(destDbName,
		utils.MustGetFlagString(option.DEST_USER),
		utils.MustGetFlagString(option.DEST_HOST),
		utils.MustGetFlagInt(option.DEST_PORT),
		utils.MustGetFlagInt(option.METADATA_JOBS))

	gplog.Info("Establishing %v dest db (%v) data connection(s)...", utils.MustGetFlagInt(option.COPY_JOBS), destDbName)
	destConn = app.initializeConnectionPool(destDbName,
		utils.MustGetFlagString(option.DEST_USER),
		utils.MustGetFlagString(option.DEST_HOST),
		utils.MustGetFlagInt(option.DEST_PORT),
		utils.MustGetFlagInt(option.COPY_JOBS))

	for i := 0; i < destMetaConn.NumConns; i++ {
		destMetaConn.MustExec("set gp_ignore_error_table to on", i)
		if len(utils.MustGetFlagString(option.TABLESPACE)) > 0 {
			destMetaConn.MustExec("set default_tablespace to "+utils.MustGetFlagString(option.TABLESPACE), i)
		}
	}

	numJobs := utils.MustGetFlagInt(option.COPY_JOBS)
	gplog.Info("Establishing %v source db (%v) data connection(s)...", numJobs, srcDbName)
	srcConn = app.initializeConnectionPool(srcDbName,
		utils.MustGetFlagString(option.SOURCE_USER),
		utils.MustGetFlagString(option.SOURCE_HOST),
		utils.MustGetFlagInt(option.SOURCE_PORT),
		numJobs)

	app.encodingGuc = SessionGUCs{}
	err := srcConn.Get(&app.encodingGuc, "SHOW client_encoding;")
	gplog.FatalOnError(err)

	return srcMetaConn, destMetaConn, srcConn, destConn
}

func (app *Application) initializeClusterResources() {
	if app.destManageConn.Version.IsHDW() && app.destManageConn.Version.AtLeast("3") {
		app.convertDDL = true
	}

	app.destSegmentsIpInfo = utils.GetSegmentsIpAddress(app.destManageConn, app.timestamp)
	app.srcSegmentsHostInfo = utils.GetSegmentsHost(app.srcManageConn)

	ph := NewPortHelper(app.destManageConn)
	err := ph.CreateHelperPortTable(app.timestamp)
	gplog.FatalOnError(err)
}

func (app *Application) needGlobalMetaData(isFirstDB bool) bool {
	if utils.MustGetFlagBool(option.WITH_GLOBALMETA) {
		return true
	}

	if config.GetCopyMode() == option.CopyModeFull && isFirstDB {
		return true
	}

	return false
}

func (app *Application) doCopy() {
	start := time.Now()

	app.initializeClusterResources()

	i := 0
	dbMap := app.queryWrapper.GetDbNameMap(app.srcManageConn)
	for srcDbName, destDbName := range dbMap {
		srcMetaConn, destMetaConn, srcConn, destConn := app.initializeConn(srcDbName, destDbName)
		srcTables, destTables, partNameMap := app.queryWrapper.GetUserTables(srcConn, destConn)

		if len(srcTables) == 0 {
			continue
		}

		metaManager := NewMetadataManager(srcMetaConn, destMetaConn, app.queryManager, app.queryWrapper,
			app.convertDDL, app.needGlobalMetaData(i == 0), utils.MustGetFlagBool(option.METADATA_ONLY),
			app.timestamp, partNameMap, app.queryWrapper.FormUserTableMap(srcTables, destTables), config.GetOwnerMap())
		metaManager.Open()

		tablec, pgsd := metaManager.MigrateMetadata(srcTables, destTables)
		if utils.MustGetFlagBool(option.METADATA_ONLY) {
			metaManager.Wait()
		} else {
			copyManager := NewCopyManager(srcConn, destConn, app.destManageConn,
				app.srcSegmentsHostInfo, app.destSegmentsIpInfo, app.timestamp,
				app.applicationName, &app.encodingGuc, pgsd)
			copyManager.Copy(tablec)
			copyManager.Close()
		}

		if pgsd != nil {
			pgsd.Finish()
		}

		metaManager.RestorePostMetadata(srcDbName, app.timestamp)
		metaManager.Close()
		app.queryWrapper.ResetCache()

		i++
	}

	gplog.Info("Total elapsed time: %v", time.Since(start))
}

func (app *Application) doTeardown() {
	failed := false
	defer func() {
		app.doCleanup(failed)

		errorCode := gplog.GetErrorCode()
		if errorCode == 0 {
			gplog.Info("Copy completed successfully")
		}
		os.Exit(errorCode)
	}()

	errStr := ""
	if err := recover(); err != nil {
		// gplog's Fatal will cause a panic with error code 2
		if gplog.GetErrorCode() != 2 {
			gplog.Error(fmt.Sprintf("%v: %s", err, debug.Stack()))
			gplog.SetErrorCode(2)
		} else {
			errStr = fmt.Sprintf("%v", err)
		}
		failed = true
	}

	if utils.WasTerminated {
		/*
		 * Don't print an error if the copy was canceled, as the signal handler will
		 * take care of cleanup and return codes. Just wait until the signal handler
		 * 's DoCleanup completes so the main goroutine doesn't exit while cleanup
		 * is still in progress.
		 */
		utils.CleanupGroup.Wait()
		failed = true
		return
	}

	if errStr != "" {
		fmt.Println(errStr)
	}
}

func (app *Application) doCleanup(failed bool) {
	defer func() {
		if err := recover(); err != nil {
			gplog.Warn("Encountered error during cleanup: %v", err)
		}
		gplog.Verbose("Cleanup complete")
		utils.CleanupGroup.Done()
	}()

	gplog.Verbose("Beginning cleanup")

	if utils.WasTerminated {
		// It is possible for the COPY command to become orphaned if an agent process is killed
		utils.TerminateHangingCopySessions(app.srcManageConn, app.applicationName)
		if app.destManageConn != nil {
			utils.TerminateHangingCopySessions(app.destManageConn, app.applicationName)
		}
	}

	if app.srcManageConn != nil {
		app.srcManageConn.Close()
	}

	if app.destManageConn != nil {
		app.destManageConn.Close()
	}
}

func (app *Application) Run(cmd *cobra.Command) {
	defer app.doTeardown()

	app.doFlagValidation(cmd)
	app.doSetup()
	app.doCopy()
}
