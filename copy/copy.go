package copy

import (
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/meta"
	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type SessionGUCs struct {
	ClientEncoding string `db:"client_encoding"`
}

/*
 * We define and initialize flags separately to avoid import conflicts in tests.
 * The flag variables, and setter functions for them, are in global_variables.go.
 */

func initializeFlags(cmd *cobra.Command) {
	SetFlagDefaults(cmd.Flags())
	utils.CmdFlags = cmd.Flags()
}

func SetFlagDefaults(flagSet *pflag.FlagSet) {
	flagSet.Bool(option.APPEND, false, "Append destination table if it exists")
	flagSet.StringSlice(option.DBNAME, []string{}, "The database(s) to be copied, separated by commas")
	flagSet.Bool(option.DEBUG, false, "Print debug log messages")
	flagSet.StringSlice(option.DEST_DBNAME, []string{}, "The database(s) in destination cluster to copy to, separated by commas")
	flagSet.String(option.DEST_HOST, "", "The host of destination cluster")
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
	flagSet.Int(option.COPY_JOBS, 4, "The maximum number of tables that concurrently copies, valid values are between 1 and 64512")
	flagSet.Int(option.METADATA_JOBS, 2, "The maximum number of metadata restore tasks, valid values are between 1 and 64512")
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

// This function handles setup that can be done before parsing flags.
func DoInit(cmd *cobra.Command) {
	timestamp = utils.CurrentTimestamp()
	applicationName = "cbcopy_" + timestamp

	gplog.SetLogFileNameFunc(logFileName)
	gplog.InitializeLogging("cbcopy", "")

	utils.CleanupGroup = &sync.WaitGroup{}
	utils.CleanupGroup.Add(1)
	initializeFlags(cmd)
	utils.InitializeSignalHandler(DoCleanup, "copy process", &utils.WasTerminated)
	objectCounts = make(map[string]int)
}

func logFileName(program, logdir string) string {
	return fmt.Sprintf("%v/%v.log", logdir, applicationName)
}

func DoFlagValidation(cmd *cobra.Command) {
	validateFlagCombinations(cmd.Flags())
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	var err error

	SetLoggerVerbosity()

	if utils.MustGetFlagBool(option.GLOBAL_METADATA_ONLY) {
		option.MakeIncludeOptions(utils.CmdFlags, CbcopyTestTable)
	}

	gplog.Debug("I'm called with: [%s]", strings.Join(os.Args, " "))
	gplog.Info("Starting copy...")
	gplog.Info("Copy Timestamp = %s", timestamp)

	config, err = option.NewOption(utils.CmdFlags)
	gplog.FatalOnError(err)

	gplog.Info("Establishing 1 source db management connection(s)...")
	srcManageConn = initializeConnectionPool("postgres",
		utils.MustGetFlagString(option.SOURCE_USER),
		utils.MustGetFlagString(option.SOURCE_HOST),
		utils.MustGetFlagInt(option.SOURCE_PORT),
		1)
	gplog.Info("Finished establishing source db management connection")

	if utils.MustGetFlagBool(option.GLOBAL_METADATA_ONLY) {
		CreateTestTable(srcManageConn, CbcopyTestTable)
	}

	gplog.Info("Establishing %v dest db management connection(s)...",
		utils.MustGetFlagInt(option.COPY_JOBS))
	destManageConn = initializeConnectionPool("postgres",
		utils.MustGetFlagString(option.DEST_USER),
		utils.MustGetFlagString(option.DEST_HOST),
		utils.MustGetFlagInt(option.DEST_PORT),
		utils.MustGetFlagInt(option.COPY_JOBS))
	gplog.Info("Finished establishing dest db management connection")

	ValidateDbnames(GetDbNameMap())
}

func initializeConn(srcDbName, destDbName string) (*dbconn.DBConn, *dbconn.DBConn, *dbconn.DBConn, *dbconn.DBConn) {
	var srcMetaConn, destMetaConn, srcConn, destConn *dbconn.DBConn

	gplog.Info("Establishing 1 source db (%v) metadata connection(s)...", srcDbName)
	srcMetaConn = initializeConnectionPool(srcDbName,
		utils.MustGetFlagString(option.SOURCE_USER),
		utils.MustGetFlagString(option.SOURCE_HOST),
		utils.MustGetFlagInt(option.SOURCE_PORT),
		1)
	gplog.Info("Finished establishing 1 source db (%v) metadata connection", srcDbName)

	if config.ContainsMetadata(utils.MustGetFlagBool(option.METADATA_ONLY), utils.MustGetFlagBool(option.DATA_ONLY)) {
		CreateDbIfNotExist(destManageConn, destDbName)
	}

	gplog.Info("Establishing %v dest db (%v) metadata connection(s)...",
		utils.MustGetFlagInt(option.METADATA_JOBS), destDbName)
	destMetaConn = initializeConnectionPool(destDbName,
		utils.MustGetFlagString(option.DEST_USER),
		utils.MustGetFlagString(option.DEST_HOST),
		utils.MustGetFlagInt(option.DEST_PORT),
		utils.MustGetFlagInt(option.METADATA_JOBS))
	gplog.Info("Finished establishing dest db (%v) metadata connection", destDbName)

	gplog.Info("Establishing %v dest db (%v) data connection(s)...",
		utils.MustGetFlagInt(option.COPY_JOBS), destDbName)
	destConn = initializeConnectionPool(destDbName,
		utils.MustGetFlagString(option.DEST_USER),
		utils.MustGetFlagString(option.DEST_HOST),
		utils.MustGetFlagInt(option.DEST_PORT),
		utils.MustGetFlagInt(option.COPY_JOBS))
	gplog.Info("Finished establishing dest db (%v) data connection", destDbName)

	for i := 0; i < destMetaConn.NumConns; i++ {
		destMetaConn.MustExec("set gp_ignore_error_table to on", i)
		if len(utils.MustGetFlagString(option.TABLESPACE)) > 0 {
			destMetaConn.MustExec("set default_tablespace to "+utils.MustGetFlagString(option.TABLESPACE), i)
		}
	}

	numJobs := utils.MustGetFlagInt(option.COPY_JOBS)

	gplog.Info("Establishing %v source db (%v) data connection(s)...", numJobs, srcDbName)
	srcConn = initializeConnectionPool(srcDbName,
		utils.MustGetFlagString(option.SOURCE_USER),
		utils.MustGetFlagString(option.SOURCE_HOST),
		utils.MustGetFlagInt(option.SOURCE_PORT),
		numJobs)
	gplog.Info("Finished establishing source db (%v) data connection", srcDbName)

	encodingGuc = SessionGUCs{}
	err := srcConn.Get(&encodingGuc, "SHOW client_encoding;")
	gplog.FatalOnError(err)

	return srcMetaConn, destMetaConn, srcConn, destConn
}

func createResources() {
	showDbVersion()
	currentUser, _ := operating.System.CurrentUser()

	fFailed = utils.OpenDataFile(fmt.Sprintf("%s/gpAdminLogs/%v_%v",
		currentUser.HomeDir, FailedFileName, timestamp))
	destSegmentsIpInfo = utils.GetSegmentsIpAddress(destManageConn, timestamp)
	srcSegmentsHostInfo = utils.GetSegmentsHost(srcManageConn)
	CreateHelperPortTable(destManageConn, timestamp)

	fCopySucced = utils.OpenDataFile(fmt.Sprintf("%s/gpAdminLogs/%v_%v",
		currentUser.HomeDir, CopySuccedFileName, timestamp))
	fSkipped = utils.OpenDataFile(fmt.Sprintf("%s/gpAdminLogs/%v_%v",
		currentUser.HomeDir, SkippedFileName, timestamp))
}

func destroyResources() {
	utils.CloseDataFile(fFailed)
	utils.CloseDataFile(fCopySucced)
	utils.CloseDataFile(fSkipped)
}

func DoCopy() {
	start := time.Now()

	createResources()
	defer destroyResources()

	i := 0
	dbMap := GetDbNameMap()
	for srcDbName, destDbName := range dbMap {
		srcMetaConn, destMetaConn, srcConn, destConn := initializeConn(srcDbName, destDbName)
		srcTables, destTables, partNameMap := GetUserTables(srcConn, destConn)

		if len(srcTables) == 0 {
			gplog.Info("db %v, no table, move to next db", srcDbName)
			continue
		}
		metaOps = meta.CreateMetaImpl(convertDDL,
			needGlobalMetaData(i == 0),
			utils.MustGetFlagBool(option.METADATA_ONLY),
			timestamp,
			partNameMap,
			formUserTableMap(srcTables, destTables),
			config.GetOwnerMap())
		metaOps.Open(srcMetaConn, destMetaConn)

		tablec, donec, pgsd := doPreDataTask(srcMetaConn, destMetaConn, srcTables, destTables)
		if utils.MustGetFlagBool(option.METADATA_ONLY) {
			<-donec
		} else {
			copyManager := NewCopyManager(srcConn, destConn, pgsd)
			copyManager.Copy(tablec)
		}

		if pgsd != nil {
			pgsd.Finish()
		}

		doPostDataTask(srcDbName, timestamp)

		// toc and meta file are removed in Close function.
		metaOps.Close()

		ResetCache()
		if srcConn != nil {
			srcConn.Close()
		}
		if destConn != nil {
			destConn.Close()
		}
		i++
	}

	gplog.Info("Total elapsed time: %v", time.Since(start))
}

func needGlobalMetaData(isFirstDB bool) bool {
	if utils.MustGetFlagBool(option.WITH_GLOBALMETA) {
		return true
	}

	if config.GetCopyMode() == option.CopyModeFull && isFirstDB {
		return true
	}

	return false
}

func showDbVersion() {
	srcDb := "GPDB"
	destDb := "GPDB"
	srcVersion := srcManageConn.Version
	if srcManageConn.HdwVersion.AtLeast("2") {
		srcVersion = srcManageConn.HdwVersion
		srcDb = "HDW"

		// existing below code did 'convertDDL = true' for xxx->3x. Let's also do same for 3x --> cbdb
		if srcVersion.Is("3") {
			gplog.Info("Converting DDL in %v", srcVersion.VersionString)
			convertDDL = true
		}
	}

	gplog.Info("Source cluster version %v %v", srcDb, srcVersion.VersionString)

	destVersion := destManageConn.Version
	if destManageConn.HdwVersion.AtLeast("2") {
		destVersion = destManageConn.HdwVersion
		destDb = "HDW"

		if destVersion.Is("3") {
			gplog.Info("Converting DDL in %v", destVersion.VersionString)
			convertDDL = true
		}
	}
	gplog.Info("Destination cluster version %v %v", destDb, destVersion.VersionString)
}

func DoTeardown() {
	failed := false
	defer func() {
		DoCleanup(failed)

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

func DoCleanup(failed bool) {
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
		utils.TerminateHangingCopySessions(srcManageConn, "copy", applicationName)
		if destManageConn != nil {
			utils.TerminateHangingCopySessions(destManageConn, "copy", applicationName)
		}
	}

	if srcManageConn != nil {
		srcManageConn.Close()
	}

	if destManageConn != nil {
		destManageConn.Close()
	}
}
