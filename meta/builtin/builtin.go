package builtin

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/meta/builtin/toc"
	"github.com/cloudberrydb/cbcopy/meta/common"
	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/vbauerster/mpb/v5"
)

type BuiltinMeta struct {
	common.MetaCommon
	SrcConn  *dbconn.DBConn
	DestConn *dbconn.DBConn
	MetaFile string
	TocFile  string
}

func NewBuiltinMeta(convert, withGlobal, metaOnly bool,
	timestamp string,
	partNameMap map[string][]string,
	tableMap map[string]string,
	ownerMap map[string]string) *BuiltinMeta {
	b := &BuiltinMeta{}

	b.ConvertDdl = convert
	b.Timestamp = timestamp
	b.WithGlobal = withGlobal
	b.MetaOnly = metaOnly
	b.PartNameMap = partNameMap
	b.TableMap = tableMap
	b.OwnerMap = ownerMap
	return b
}

func (b *BuiltinMeta) Open(srcConn, destConn *dbconn.DBConn) {
	if srcConn == nil {
		return
	}

	b.SrcConn = srcConn
	b.DestConn = destConn

	InitializeMetadataParams(srcConn)

	gpdbVersion = srcConn.Version
	gpdbVersionDst = destConn.Version
	hdwVersion = srcConn.HdwVersion
	hdwVersionDst = destConn.HdwVersion

	// current code many place has connectionPool, which is not very clear how it's used, let's comment it out to see effect.
	// connectionPool = srcConn
	connectionPoolDst = destConn

	globalTOC = &toc.TOC{}
	globalTOC.InitializeMetadataEntryMap()
	getQuotedRoleNames(srcConn)
	filterRelationClause = ""
	excludeRelations = make([]string, 0)
	includeRelations = make([]string, 0)
	includeSchemas = make([]string, 0)
	excludeSchemas = make([]string, 0)
	objectCounts = make(map[string]int)

	errorTablesMetadata = make(map[string]Empty)
	errorTablesData = make(map[string]Empty)
	redirectSchema = make(map[string]string)
	inclDestSchema = ""
	needConvert = b.ConvertDdl
	ownerMap = b.OwnerMap
}

func (b *BuiltinMeta) CopyDatabaseMetaData(tablec chan option.TablePair, donec chan struct{}) utils.ProgressBar {
	gplog.Info("Copying metadata from database \"%v\" to \"%v\"", b.SrcConn.DBName, b.DestConn.DBName)

	b.extractDDL(nil, nil)
	return b.executeDDL(tablec, donec)
}

func (b *BuiltinMeta) CopySchemaMetaData(sschemas, dschemas []*option.DbSchema, tablec chan option.TablePair, donec chan struct{}) utils.ProgressBar {
	i := 0

	for _, v := range sschemas {
		dschema := v.Schema
		if len(dschemas) > 0 {
			dschema = dschemas[i].Schema
			redirectSchema[v.Schema] = dschema
		}
		i++

		includeSchemas = append(includeSchemas, v.Schema)
		gplog.Info("Copying metadata from schema \"%v.%v\" => \"%v.%v\"",
			b.SrcConn.DBName, v.Schema, b.DestConn.DBName, dschema)
	}

	b.extractDDL(includeSchemas, nil)
	return b.executeDDL(tablec, donec)
}

func (b *BuiltinMeta) CopyTableMetaData(dschemas []*option.DbSchema,
	sschemas []string,
	tables []string,
	tablec chan option.TablePair,
	donec chan struct{}) utils.ProgressBar {
	gplog.Info("Copying table metadata")

	includeSchemas = append(includeSchemas, sschemas...)
	includeRelations = append(includeRelations, tables...)

	if len(dschemas) > 0 {
		inclDestSchema = dschemas[0].Schema
	}

	b.extractDDL(includeSchemas, includeRelations)
	return b.executeDDL(tablec, donec)
}

func (b *BuiltinMeta) CopyPostData() {
	restorePostdata(b.DestConn, b.MetaFile)
}

func (b *BuiltinMeta) GetErrorTableMetaData() map[string]Empty {
	return errorTablesMetadata
}

func (b *BuiltinMeta) Close() {
	if b.SrcConn == nil {
		return
	}

	// todo, add a flag to control whether keep meta file and toc file for debug purpose
	metaFileBackupName := b.MetaFile + "." + b.SrcConn.DBName + "." + b.DestConn.DBName + "." + "bk"
	tocFileBackupName := b.TocFile + "." + b.SrcConn.DBName + "." + b.DestConn.DBName + "." + "bk"

	gplog.Info("file rename, MetaFile %v --> %v, TocFile %v --> %v", b.MetaFile, metaFileBackupName, b.TocFile, tocFileBackupName)
	os.Rename(b.MetaFile, metaFileBackupName)
	os.Rename(b.TocFile, tocFileBackupName)

	//os.Remove(b.MetaFile)
	//os.Remove(b.TocFile)

	b.SrcConn.Close()
	b.DestConn.Close()
}

func (b *BuiltinMeta) extractDDL(inSchemas, inTables []string) {
	currentUser, _ := operating.System.CurrentUser()
	b.MetaFile = fmt.Sprintf("%s/gpAdminLogs/cbcopy_meta_%v", currentUser.HomeDir, b.Timestamp)
	gplog.Info("Metadata will be written to %s", b.MetaFile)

	metadataTables, _ := RetrieveAndProcessTables(b.SrcConn, inTables)
	metadataFile := utils.NewFileWithByteCountFromFile(b.MetaFile)

	backupSessionGUC(b.SrcConn, metadataFile)
	if len(inTables) == 0 || b.WithGlobal {
		backupGlobals(b.SrcConn, b.ConvertDdl, metadataFile)
	}
	backupPredata(b.SrcConn, metadataFile, inSchemas, metadataTables, len(inTables) > 0)
	backupPostdata(b.SrcConn, b.ConvertDdl, metadataFile, inSchemas)

	b.TocFile = fmt.Sprintf("%s/gpAdminLogs/cbcopy_toc_%v", currentUser.HomeDir, b.Timestamp)
	globalTOC.WriteToFileAndMakeReadOnly(b.TocFile)
	metadataFile.Close()

	gplog.Info("Metadata file written done")
}

func (b *BuiltinMeta) executeDDL(tablec chan option.TablePair, donec chan struct{}) utils.ProgressBar {
	gplog.Info("Metadata will be restored from %s, WithGlobal: %v", b.MetaFile, b.WithGlobal)

	if b.WithGlobal {
		restoreGlobal(b.DestConn, b.MetaFile)
	}

	filters := NewFilters(nil, nil, nil, nil)
	schemaStatements := GetRestoreMetadataStatementsFiltered("predata", b.MetaFile, []string{"SCHEMA"}, []string{}, filters)
	statements := GetRestoreMetadataStatementsFiltered("predata", b.MetaFile, []string{}, []string{"SCHEMA"}, filters)

	pgsm, pgsd := initlizeProgressBar(len(schemaStatements)+len(statements), len(b.TableMap), b.MetaOnly)
	go restorePredata(b.DestConn, b.MetaFile, b.PartNameMap, b.TableMap, tablec, donec, schemaStatements, pgsm)
	return pgsd
}

func initlizeProgressBar(numStmts, numTables int, metaonly bool) (utils.ProgressBar, utils.ProgressBar) {
	gplog.Debug("Creating progress bar")
	defer gplog.Debug("Finished creating progress bar")

	var pgsm utils.ProgressBar

	progress := mpb.New(mpb.WithWidth(60), mpb.WithRefreshRate(180*time.Millisecond))

	if numStmts > 0 {
		pgsm = utils.NewProgressBarEx(progress, numStmts, "Pre-data objects restored: ")
	}

	if !metaonly && numTables > 0 {
		pgsd := utils.NewProgressBarEx(progress, numTables, "Table copied: ")
		return pgsm, pgsd
	}

	return pgsm, pgsm
}

func backupGlobals(conn *dbconn.DBConn, convert bool, metadataFile *utils.FileWithByteCount) {
	gplog.Info("Writing global database metadata")

	backupResourceQueues(conn, metadataFile)
	backupResourceGroups(conn, metadataFile)
	backupRoles(conn, metadataFile)
	backupRoleGrants(conn, metadataFile)
	if !convert {
		backupTablespaces(conn, metadataFile)
		backupCreateDatabase(conn, metadataFile)
	} else {
		gplog.Info("convert is true, backupTablespaces and backupCreateDatabase are skipped")
	}
	backupDatabaseGUCs(conn, metadataFile)
	backupRoleGUCs(conn, metadataFile)

	gplog.Info("Global database metadata backup complete")
}

// https://github.com/greenplum-db/gpbackup/commit/5db5e23b61775e5bc831fd566845a3b99f8eca05
// note: change in this function, current function content has some kind of degree diff as gpbackup code, which makes manual part change uncertain, so to be simple, try to copy whole this function first
// https://github.com/greenplum-db/gpbackup/commit/be64bd87bd1c61e3b6ff78268748618ab44488ad
// https://github.com/greenplum-db/gpbackup/commit/a8d3ff7ab78669197de7002ba33ec1f34786e3aa
// https://github.com/greenplum-db/gpbackup/commit/0c421c6238d51ce5b75f555f5fbf2413bb53f0c0

/*
func backupPredata(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount, inSchemas []string, tables []Table, tableOnly bool) {
	gplog.Info("Writing pre-data metadata")

	objects := make([]Sortable, 0)
	metadataMap := make(MetadataMap)
	objects = append(objects, convertToSortableSlice(tables)...)
	relationMetadata := GetMetadataForObjectType(conn, TYPE_RELATION)
	addToMetadataMap(relationMetadata, metadataMap)

	var protocols []ExternalProtocol
	funcInfoMap := GetFunctionOidToInfoMap(conn)

	if !tableOnly {
		backupSchemas(conn, metadataFile, createAlteredPartitionSchemaSet(tables))
		if len(inSchemas) == 0 && conn.Version.AtLeast("5") {
			backupExtensions(conn, metadataFile)
		}

		if conn.Version.AtLeast("6") {
			backupCollations(conn, metadataFile)
		}

		procLangs := GetProceduralLanguages(conn)
		langFuncs, functionMetadata := retrieveFunctions(conn, &objects, metadataMap, procLangs)

		if len(inSchemas) == 0 {
			backupProceduralLanguages(conn, metadataFile, procLangs, langFuncs, functionMetadata, funcInfoMap)
		}
		retrieveAndBackupTypes(conn, metadataFile, &objects, metadataMap)

		if len(inSchemas) == 0 &&
			conn.Version.AtLeast("6") {
			retrieveForeignDataWrappers(conn, &objects, metadataMap)
			retrieveForeignServers(conn, &objects, metadataMap)
			retrieveUserMappings(conn, &objects)
		}

		protocols = retrieveProtocols(conn, &objects, metadataMap)

		if conn.Version.AtLeast("5") {
			retrieveTSParsers(conn, &objects, metadataMap)
			retrieveTSConfigurations(conn, &objects, metadataMap)
			retrieveTSTemplates(conn, &objects, metadataMap)
			retrieveTSDictionaries(conn, &objects, metadataMap)

			backupOperatorFamilies(conn, metadataFile)
		}

		retrieveOperators(conn, &objects, metadataMap)
		retrieveOperatorClasses(conn, &objects, metadataMap)
		retrieveAggregates(conn, &objects, metadataMap)
		retrieveCasts(conn, &objects, metadataMap)
	}

	retrieveViews(conn, &objects)
	sequences, sequenceOwnerColumns := retrieveSequences(conn)
	backupCreateSequences(metadataFile, sequences, sequenceOwnerColumns, relationMetadata)
	constraints, conMetadata := retrieveConstraints(conn)

	backupDependentObjects(conn, metadataFile, tables, protocols, metadataMap, constraints, objects, funcInfoMap, tableOnly)
	PrintAlterSequenceStatements(metadataFile, globalTOC, sequences, sequenceOwnerColumns)

	backupConversions(conn, metadataFile)
	backupConstraints(metadataFile, constraints, conMetadata)
}
*/

func backupPredata(connectionPool *dbconn.DBConn, metadataFile *utils.FileWithByteCount, inSchemas []string, tables []Table, tableOnly bool) {
	gplog.Info("Writing pre-data metadata")

	var protocols []ExternalProtocol
	var functions []Function
	var funcInfoMap map[uint32]FunctionInfo
	objects := make([]Sortable, 0)
	metadataMap := make(MetadataMap)

	// backup function first, refer: https://github.com/greenplum-db/gpbackup/commit/0c3ab91e550bd0ae076314b75ab18e8a6907a1f6
	if !tableOnly {
		functions, funcInfoMap = retrieveFunctions(connectionPool, &objects, metadataMap)
	}
	objects = append(objects, convertToSortableSlice(tables)...)
	relationMetadata := GetMetadataForObjectType(connectionPool, TYPE_RELATION)
	addToMetadataMap(relationMetadata, metadataMap)

	if !tableOnly {
		protocols = retrieveProtocols(connectionPool, &objects, metadataMap)
		backupSchemas(connectionPool, metadataFile, createAlteredPartitionSchemaSet(tables))
		backupExtensions(connectionPool, metadataFile)
		backupCollations(connectionPool, metadataFile)
		retrieveAndBackupTypes(connectionPool, metadataFile, &objects, metadataMap)

		if len(inSchemas) == 0 {
			/*
				(todo: some kinds of duplicate) note :
				e.g. execute below in source db
				CREATE FUNCTION plperl_call_handler() RETURNS
					language_handler
					AS '$libdir/plperl'
					LANGUAGE C;

				CREATE PROCEDURAL LANGUAGE plperlabc HANDLER plperl_call_handler;

				in backupProceduralLanguages(), it does backup language referenced function. The backup meta file does look like
				CREATE FUNCTION public.plperl_call_handler() RETURNS language_handler AS
					'$libdir/plperl', 'plperl_call_handler'
					LANGUAGE c NO SQL PARALLEL UNSAFE;

				CREATE OR REPLACE PROCEDURAL LANGUAGE plperlabc HANDLER public.plperl_call_handler;

				in later function backupDependentObjects, it includes function backup, which does have this function again
				CREATE FUNCTION public.mysfunc_accum(numeric, numeric, numeric) RETURNS numeric AS
					$_$select $1 + $2 + $3$_$
					LANGUAGE sql CONTAINS SQL STRICT PARALLEL UNSAFE;

				CREATE FUNCTION public.plperl_call_handler() RETURNS language_handler AS
					'$libdir/plperl', 'plperl_call_handler'
					LANGUAGE c NO SQL PARALLEL UNSAFE;

				(gpbackup has same behavior)
			*/
			backupProceduralLanguages(connectionPool, metadataFile, functions, funcInfoMap, metadataMap)

			retrieveTransforms(connectionPool, &objects)
			retrieveFDWObjects(connectionPool, &objects, metadataMap)
		}

		retrieveTSObjects(connectionPool, &objects, metadataMap)
		backupOperatorFamilies(connectionPool, metadataFile)
		retrieveOperatorObjects(connectionPool, &objects, metadataMap)
		retrieveAggregates(connectionPool, &objects, metadataMap)
		retrieveCasts(connectionPool, &objects, metadataMap)
		backupAccessMethods(connectionPool, metadataFile)
	}

	retrieveViews(connectionPool, &objects)
	sequences := retrieveAndBackupSequences(connectionPool, metadataFile, relationMetadata)
	domainConstraints := retrieveConstraints(connectionPool, &objects, metadataMap)

	backupDependentObjects(connectionPool, metadataFile, tables, protocols, metadataMap, domainConstraints, objects, sequences, funcInfoMap, tableOnly)

	backupConversions(connectionPool, metadataFile)

	gplog.Info("Pre-data metadata backup complete")
}

func backupPostdata(conn *dbconn.DBConn, convert bool, metadataFile *utils.FileWithByteCount, inSchemas []string) {
	gplog.Info("Writing post-data metadata")

	if !convert {
		backupIndexes(conn, metadataFile)
	}
	backupRules(conn, metadataFile)
	backupTriggers(conn, metadataFile)
	if conn.Version.AtLeast("6") {
		backupDefaultPrivileges(conn, metadataFile)
		if len(inSchemas) == 0 {
			backupEventTriggers(conn, metadataFile)
		}
	}

	if conn.Version.AtLeast("7") {
		backupRowLevelSecurityPolicies(conn, metadataFile) // https://github.com/greenplum-db/gpbackup/commit/5051cd4cfecfe7bc396baeeb9b0ac6ea13c21010
		backupExtendedStatistic(conn, metadataFile)        // https://github.com/greenplum-db/gpbackup/commit/7072d534d48ba32946c4112ad03f52fbef372c8c
	}

	gplog.Info("Post-data metadata backup complete")
}

func restoreGlobal(conn *dbconn.DBConn, metadataFilename string) {
	gplog.Info("Restoring global metadata")

	objectTypes := []string{"SESSION GUCS", "DATABASE GUC", "DATABASE METADATA", "RESOURCE QUEUE", "RESOURCE GROUP", "ROLE", "ROLE GUCS", "ROLE GRANT", "TABLESPACE"}
	statements := GetRestoreMetadataStatements("global", metadataFilename, objectTypes, []string{})
	statements = toc.RemoveActiveRole(conn.User, statements)
	ExecuteRestoreMetadataStatements(conn, statements, "Global objects", nil, utils.PB_VERBOSE, false)

	gplog.Info("Global database metadata restore complete")
}

func restorePredata(conn *dbconn.DBConn,
	metadataFilename string,
	partNameMap map[string][]string,
	tabMap map[string]string,
	tablec chan option.TablePair,
	donec chan struct{},
	statements []toc.StatementWithType,
	progressBar utils.ProgressBar) {

	gplog.Info("Restoring pre-data metadata")

	RestoreSchemas(conn, statements, progressBar)
	RestoreExtensions(conn, metadataFilename, progressBar)
	RestoreCollations(conn, metadataFilename, progressBar)
	RestoreTypes(conn, metadataFilename, progressBar)
	RestoreOperatorFamilies(conn, metadataFilename, progressBar)
	RestoreCreateSequences(conn, metadataFilename, progressBar)

	RestoreDependentObjects(conn, metadataFilename, progressBar, partNameMap, tabMap, tablec)

	/*
		procedure language might have dependent on some function (see comment backupPredata),
		so here it need to put procedure language after function restore, or modify to restore its dependent function together with procedure language
	*/
	RestoreProceduralLanguages(conn, metadataFilename, progressBar)

	RestoreExternalParts(conn, metadataFilename, progressBar)
	RestoreSequenceOwner(conn, metadataFilename, progressBar)
	RestoreConversions(conn, metadataFilename, progressBar)
	RestoreConstraints(conn, metadataFilename, progressBar)
	RestoreCleanup(tabMap, tablec)

	close(tablec)
	close(donec)

	gplog.Info("Pre-data metadata restore complete")
}

// todo: this part code is not same as gpbackup, e.g. function interface: (statements []toc.StatementWithType, redirectSchema string) , is it expected? seems it's ok.
// GP7: https://github.com/greenplum-db/gpbackup/commit/c90eadb3c6fac8b7a6b2513b5063c69554c028fa, not bring, current this code looks already achieved that change purpose, although in different way
func editStatementsRedirectSchema(statements []toc.StatementWithType, isSchema bool) {
	if len(redirectSchema) == 0 && inclDestSchema == "" {
		return
	}

	for i, statement := range statements {
		schema := ""
		if len(redirectSchema) > 0 {
			ss, exists := redirectSchema[statement.Schema]
			if !exists {
				continue
			}
			schema = ss
		} else {
			schema = inclDestSchema
		}

		oldSchema := fmt.Sprintf("%s.", statement.Schema)
		newSchema := fmt.Sprintf("%s.", schema)
		statements[i].Schema = schema
		if isSchema {
			statements[i].Statement = strings.Replace(statement.Statement, statement.Schema, schema, 2)
		} else {
			statements[i].Statement = strings.Replace(statement.Statement, oldSchema, newSchema, -1)
		}
		// only postdata will have a reference object
		if statement.ReferenceObject != "" {
			statements[i].ReferenceObject = strings.Replace(statement.ReferenceObject, oldSchema, newSchema, 1)
		}
	}
}

func restorePostdata(conn *dbconn.DBConn, metadataFilename string) {
	gplog.Info("Restoring post-data metadata")

	filters := NewFilters(nil, nil, nil, nil)
	statements := GetRestoreMetadataStatementsFiltered("postdata", metadataFilename, []string{}, []string{}, filters)
	editStatementsRedirectSchema(statements, false)
	firstBatch, secondBatch, thirdBatch := BatchPostdataStatements(statements)

	if len(statements) > 0 {
		progressBar := utils.NewProgressBar(len(statements), "Post-data objects restored: ", utils.PB_VERBOSE)
		progressBar.Start()
		ExecuteRestoreMetadataStatements(conn, firstBatch, "", progressBar, utils.PB_VERBOSE, conn.NumConns > 1)
		ExecuteRestoreMetadataStatements(conn, secondBatch, "", progressBar, utils.PB_VERBOSE, conn.NumConns > 1)
		ExecuteRestoreMetadataStatements(conn, thirdBatch, "", progressBar, utils.PB_VERBOSE, conn.NumConns > 1)
		progressBar.Finish()
	}

	gplog.Info("Post-data metadata restore complete")
}
