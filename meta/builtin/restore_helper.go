package builtin

import (
	"fmt"
	"strings"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/meta/builtin/toc"
	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
)

/*
 * This file contains wrapper functions that group together functions relating
 * to querying and restoring metadata, so that the logic for each object type
 * can all be in one place and restore.go can serve as a high-level look at the
 * overall restore flow.
 */

/*
 * Setup and validation wrapper functions
 */

/*
 * Filter structure to filter schemas and relations
 */
type Filters struct {
	includeSchemas   []string
	excludeSchemas   []string
	includeRelations []string
	excludeRelations []string
}

func NewFilters(inSchema []string, exSchemas []string, inRelations []string, exRelations []string) Filters {
	f := Filters{}
	f.includeSchemas = inSchema
	f.excludeSchemas = exSchemas
	f.includeRelations = inRelations
	f.excludeRelations = exRelations
	return f
}

func filtersEmpty(filters Filters) bool {
	return len(filters.includeSchemas) == 0 && len(filters.excludeSchemas) == 0 && len(filters.includeRelations) == 0 && len(filters.excludeRelations) == 0
}

/*
 * Metadata and/or data restore wrapper functions
 */
func GetRestoreMetadataStatements(section string, filename string, includeObjectTypes []string, excludeObjectTypes []string) []toc.StatementWithType {
	var statements []toc.StatementWithType
	statements = GetRestoreMetadataStatementsFiltered(section, filename, includeObjectTypes, excludeObjectTypes, Filters{})
	return statements
}

func GetRestoreMetadataStatementsFiltered(section string, filename string, includeObjectTypes []string, excludeObjectTypes []string, filters Filters) []toc.StatementWithType {
	metadataFile := iohelper.MustOpenFileForReading(filename)
	var statements []toc.StatementWithType
	var inSchemas, exSchemas, inRelations, exRelations []string
	if !filtersEmpty(filters) {
		inSchemas = filters.includeSchemas
		exSchemas = filters.excludeSchemas
		inRelations = filters.includeRelations
		exRelations = filters.excludeRelations
		// Update include schemas for schema restore if include table is set
		if utils.Exists(includeObjectTypes, "SCHEMA") {
			for _, inRelation := range inRelations {
				schema := inRelation[:strings.Index(inRelation, ".")]
				if !utils.Exists(inSchemas, schema) {
					inSchemas = append(inSchemas, schema)
				}
			}
			// reset relation list as these were required only to extract schemas from inRelations
			inRelations = nil
			exRelations = nil
		}
	}
	statements = globalTOC.GetSQLStatementForObjectTypes(section, metadataFile, includeObjectTypes, excludeObjectTypes, inSchemas, exSchemas, inRelations, exRelations)
	return statements
}

func ExecuteRestoreMetadataStatements(conn *dbconn.DBConn, statements []toc.StatementWithType, objectsTitle string, progressBar utils.ProgressBar, showProgressBar int, executeInParallel bool) {
	if len(statements) == 0 {
		return
	}

	if progressBar == nil {
		ExecuteStatementsAndCreateProgressBar(conn, statements, objectsTitle, showProgressBar, executeInParallel)
	} else {
		ExecuteStatements(conn, statements, progressBar, executeInParallel)
	}
}

/*
 * The first time this function is called, it retrieves the session GUCs from the
 * predata file and processes them appropriately, then it returns them so they
 * can be used in later calls without the file access and processing overhead.
 */
func setGUCsForConnection(conn *dbconn.DBConn, filename string, gucStatements []toc.StatementWithType, whichConn int) []toc.StatementWithType {
	if gucStatements == nil {
		objectTypes := []string{"SESSION GUCS"}
		gucStatements = GetRestoreMetadataStatements("global", filename, objectTypes, []string{})
	}
	ExecuteStatementsAndCreateProgressBar(conn, gucStatements, "", utils.PB_NONE, false, whichConn)
	return gucStatements
}

func RestoreSchemas(conn *dbconn.DBConn, schemaStatements []toc.StatementWithType, progressBar utils.ProgressBar) {
	gplog.Verbose("Restoring schemas")

	editStatementsRedirectSchema(schemaStatements, true)

	numErrors := 0
	for _, schema := range schemaStatements {
		gplog.Debug("RestoreSchemas, query is %v", schema.Statement)
		_, err := conn.Exec(schema.Statement, 0)
		if err != nil {
			if strings.Contains(err.Error(), "already exists") {
			} else {
				errMsg := fmt.Sprintf("Error encountered while creating schema %s", schema.Name)
				gplog.Verbose(fmt.Sprintf("%s: %s", errMsg, err.Error()))
				numErrors++
			}
		}
		progressBar.Increment()
	}
	if numErrors > 0 {
		gplog.Error("Encountered %d errors during schema restore; see log file %s for a list of errors.", numErrors, gplog.GetLogFilePath())
	}
}

func RestoreExtensions(conn *dbconn.DBConn, metadataFile string, progressBar utils.ProgressBar) {
	gplog.Verbose("Restoring extensions")

	stmts := GetRestoreMetadataStatements("predata", metadataFile, []string{"EXTENSION"}, []string{})
	editStatementsRedirectSchema(stmts, false)
	ExecuteRestoreMetadataStatements(conn, stmts, "Pre-data extension objects", progressBar, utils.PB_VERBOSE, false)
}

func RestoreCollations(conn *dbconn.DBConn, metadataFile string, progressBar utils.ProgressBar) {
	gplog.Verbose("Restoring collations")

	stmts := GetRestoreMetadataStatements("predata", metadataFile, []string{"COLLATION"}, []string{})
	editStatementsRedirectSchema(stmts, false)
	ExecuteRestoreMetadataStatements(conn, stmts, "Pre-data collation objects", progressBar, utils.PB_VERBOSE, false)
}

func RestoreProceduralLanguages(conn *dbconn.DBConn, metadataFile string, progressBar utils.ProgressBar) {
	gplog.Verbose("Restoring procedural languages")

	stmts := GetRestoreMetadataStatements("predata", metadataFile, []string{"LANGUAGE"}, []string{})
	editStatementsRedirectSchema(stmts, false)
	ExecuteRestoreMetadataStatements(conn, stmts, "Pre-data procedural languages objects", progressBar, utils.PB_VERBOSE, false)
}

func RestoreTypes(conn *dbconn.DBConn, metadataFile string, progressBar utils.ProgressBar) {
	gplog.Verbose("Restoring types")

	stmts := GetRestoreMetadataStatements("predata", metadataFile, []string{"TYPE"}, []string{})
	editStatementsRedirectSchema(stmts, false)
	ExecuteRestoreMetadataStatements(conn, stmts, "Pre-data type objects", progressBar, utils.PB_VERBOSE, false)
}

func RestoreOperatorFamilies(conn *dbconn.DBConn, metadataFile string, progressBar utils.ProgressBar) {
	gplog.Verbose("Restoring operator families")

	stmts := GetRestoreMetadataStatements("predata", metadataFile, []string{"OPERATOR FAMILY"}, []string{})
	editStatementsRedirectSchema(stmts, false)
	ExecuteRestoreMetadataStatements(conn, stmts, "Pre-data operator familiy objects", progressBar, utils.PB_VERBOSE, false)
}

func RestoreCreateSequences(conn *dbconn.DBConn, metadataFile string, progressBar utils.ProgressBar) {
	gplog.Verbose("Restoring sequences")

	stmts := GetRestoreMetadataStatements("predata", metadataFile, []string{"SEQUENCE"}, []string{})
	editStatementsRedirectSchema(stmts, false)
	ExecuteRestoreMetadataStatements(conn, stmts, "Pre-data sequence objects", progressBar, utils.PB_VERBOSE, false)
}

func RestoreExternalParts(conn *dbconn.DBConn, metadataFile string, progressBar utils.ProgressBar) {
	gplog.Verbose("Restoring exchange partitions")

	stmts := GetRestoreMetadataStatements("predata", metadataFile, []string{"EXCHANGE PARTITION"}, []string{})
	editStatementsRedirectSchema(stmts, false)
	ExecuteRestoreMetadataStatements(conn, stmts, "Pre-data exchange part objects", progressBar, utils.PB_VERBOSE, false)
}

func RestoreSequenceOwner(conn *dbconn.DBConn, metadataFile string, progressBar utils.ProgressBar) {
	gplog.Verbose("Restoring sequence owner")

	stmts := GetRestoreMetadataStatements("predata", metadataFile, []string{"SEQUENCE OWNER"}, []string{})
	editStatementsRedirectSchema(stmts, false)
	ExecuteRestoreMetadataStatements(conn, stmts, "Pre-data sequence owner objects", progressBar, utils.PB_VERBOSE, false)
}

func RestoreConversions(conn *dbconn.DBConn, metadataFile string, progressBar utils.ProgressBar) {
	gplog.Verbose("Restoring conversions")

	stmts := GetRestoreMetadataStatements("predata", metadataFile, []string{"CONVERSION"}, []string{})
	editStatementsRedirectSchema(stmts, false)
	ExecuteRestoreMetadataStatements(conn, stmts, "Pre-data conversion objects", progressBar, utils.PB_VERBOSE, false)
}

func RestoreConstraints(conn *dbconn.DBConn, metadataFile string, progressBar utils.ProgressBar) {
	gplog.Verbose("Restoring constraints")

	stmts := GetRestoreMetadataStatements("predata", metadataFile, []string{"CONSTRAINT"}, []string{})
	editStatementsRedirectSchema(stmts, false)
	ExecuteRestoreMetadataStatements(conn, stmts, "Pre-data constraint objects", progressBar, utils.PB_VERBOSE, false)
}

func RestoreDependentObjects(conn *dbconn.DBConn, metadataFile string, progressBar utils.ProgressBar, partNameMap map[string][]string, tabMap map[string]string, tablec chan option.TablePair) {
	gplog.Verbose("Restoring dependent objects in parallel mode")

	// the reason for those exclude object types is the function calling this function has restored them, see this function caller
	stmts := GetRestoreMetadataStatements("predata", metadataFile, []string{}, []string{"EXTENSION", "COLLATION", "LANGUAGE", "TYPE", "OPERATOR FAMILY", "SEQUENCE", "EXCHANGE PARTITION", "SEQUENCE OWNER", "CONVERSION", "CONSTRAINT", "SCHEMA"})
	editStatementsRedirectSchema(stmts, false)
	ExecuteDependentStatements(conn, stmts, progressBar, partNameMap, tabMap, tablec)
}

// although there's such a function, but it's not used, in gpbackup, it's used in restore.go/verifyIncrementalState().
// so https://github.com/greenplum-db/gpbackup/commit/1f063269734b41b8390f6d5124fa7a25530cc0d2 is not neccesary.
func GetExistingTableFQNs(connectionPool *dbconn.DBConn) ([]string, error) {
	existingTableFQNs := make([]string, 0)
	var relkindFilter string

	if connectionPool.Version.Before("6") {
		relkindFilter = "'r', 'S'"
	} else if connectionPool.Version.Is("6") {
		relkindFilter = "'r', 'S', 'f'"
	} else if connectionPool.Version.AtLeast("7") {
		relkindFilter = "'r', 'S', 'f', 'p'"
	}
	query := fmt.Sprintf(`SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname)
			  FROM pg_catalog.pg_class c
				LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			  WHERE c.relkind IN (%s)
				 AND n.nspname !~ '^pg_'
				 AND n.nspname !~ '^gp_'
				 AND n.nspname <> 'information_schema'
			  ORDER BY 1;`, relkindFilter)

	gplog.Debug("GetExistingTableFQNs, query is %v", query)
	err := connectionPool.Select(&existingTableFQNs, query)
	return existingTableFQNs, err
}

func GetExistingSchemas(conn *dbconn.DBConn) ([]string, error) {
	existingSchemas := make([]string, 0)

	query := `SELECT n.nspname AS "Name"
			  FROM pg_catalog.pg_namespace n
			  WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
			  ORDER BY 1;`

	gplog.Debug("GetExistingSchemas, query is %v", query)
	err := conn.Select(&existingSchemas, query)
	return existingSchemas, err
}
