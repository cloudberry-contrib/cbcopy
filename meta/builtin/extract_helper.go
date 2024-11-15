package builtin

import (
	"reflect"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

/*
 * Metadata retrieval wrapper functions
 */

func RetrieveAndProcessTables(conn *dbconn.DBConn, includeTables []string) ([]Table, []Table) {
	quotedIncludeRelations, err := option.QuoteTableNames(conn, includeTables)
	gplog.FatalOnError(err)

	tableRelations := GetIncludedUserTableRelations(conn, quotedIncludeRelations)

	if (conn.Version.IsGPDB() && conn.Version.AtLeast("6")) || conn.Version.IsCBDB() {
		tableRelations = append(tableRelations, GetForeignTableRelations(conn)...)
	}

	tables := ConstructDefinitionsForTables(conn, tableRelations)

	metadataTables, dataTables := SplitTablesByPartitionType(conn, tables, quotedIncludeRelations)
	objectCounts["Tables"] = len(metadataTables)

	return metadataTables, dataTables
}

func retrieveFunctions(connectionPool *dbconn.DBConn, sortables *[]Sortable, metadataMap MetadataMap) ([]Function, map[uint32]FunctionInfo) {
	gplog.Verbose("Retrieving function information")
	functionMetadata := GetMetadataForObjectType(connectionPool, TYPE_FUNCTION)
	addToMetadataMap(functionMetadata, metadataMap)
	functions := GetFunctionsAllVersions(connectionPool)
	funcInfoMap := GetFunctionOidToInfoMap(connectionPool)
	objectCounts["Functions"] = len(functions)
	*sortables = append(*sortables, convertToSortableSlice(functions)...)

	return functions, funcInfoMap
}

func retrieveTransforms(conn *dbconn.DBConn, sortables *[]Sortable) {
	if conn.Version.IsGPDB() && conn.Version.Before("7") {
		return
	}
	gplog.Verbose("Retrieving transform information")
	transforms := GetTransforms(conn)
	objectCounts["Transforms"] = len(transforms)
	*sortables = append(*sortables, convertToSortableSlice(transforms)...)
}

func retrieveAndBackupTypes(connectionPool *dbconn.DBConn, metadataFile *utils.FileWithByteCount, sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving type information")
	shells := GetShellTypes(connectionPool)
	bases := GetBaseTypes(connectionPool)
	composites := GetCompositeTypes(connectionPool)
	domains := GetDomainTypes(connectionPool)
	rangeTypes := make([]RangeType, 0)
	if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDB() {
		rangeTypes = GetRangeTypes(connectionPool)
	}
	typeMetadata := GetMetadataForObjectType(connectionPool, TYPE_TYPE)

	backupShellTypes(metadataFile, shells, bases, rangeTypes)
	if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("5")) || connectionPool.Version.IsCBDB() {
		backupEnumTypes(connectionPool, metadataFile, typeMetadata)
	}

	objectCounts["Types"] += len(shells)
	objectCounts["Types"] += len(bases)
	objectCounts["Types"] += len(composites)
	objectCounts["Types"] += len(domains)
	objectCounts["Types"] += len(rangeTypes)
	*sortables = append(*sortables, convertToSortableSlice(bases)...)
	*sortables = append(*sortables, convertToSortableSlice(composites)...)
	*sortables = append(*sortables, convertToSortableSlice(domains)...)
	*sortables = append(*sortables, convertToSortableSlice(rangeTypes)...)
	addToMetadataMap(typeMetadata, metadataMap)
}

// https://github.com/greenplum-db/gpbackup/commit/5895076a2678b00e50455e889d7df93d003fd3e8
func retrieveConstraints(conn *dbconn.DBConn, sortables *[]Sortable, metadataMap MetadataMap, tables ...Relation) []Constraint {
	gplog.Verbose("Retrieving constraints")
	constraints := GetConstraints(conn, tables...)
	if len(constraints) > 0 && ((conn.Version.IsGPDB() && conn.Version.AtLeast("7")) || conn.Version.IsCBDB()) {
		RenameExchangedPartitionConstraints(conn, &constraints)
	}

	//split into domain constraints and all others, as they are handled differently downstream
	domainConstraints := make([]Constraint, 0)
	nonDomainConstraints := make([]Constraint, 0)
	for _, con := range constraints {
		if con.IsDomainConstraint {
			domainConstraints = append(domainConstraints, con)
		} else {
			nonDomainConstraints = append(nonDomainConstraints, con)
		}
	}

	objectCounts["Constraints"] = len(nonDomainConstraints)
	conMetadata := GetCommentsForObjectType(conn, TYPE_CONSTRAINT)
	*sortables = append(*sortables, convertToSortableSlice(nonDomainConstraints)...)
	addToMetadataMap(conMetadata, metadataMap)
	return domainConstraints
}

func retrieveAndBackupSequences(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount,
	relationMetadata MetadataMap) []Sequence {
	gplog.Verbose("Writing CREATE SEQUENCE statements to metadata file")
	sequences := GetAllSequences(conn)
	objectCounts["Sequences"] = len(sequences)
	PrintCreateSequenceStatements(metadataFile, globalTOC, sequences, relationMetadata)
	return sequences
}

func retrieveProtocols(conn *dbconn.DBConn, sortables *[]Sortable, metadataMap MetadataMap) []ExternalProtocol {
	gplog.Verbose("Retrieving protocols")
	protocols := GetExternalProtocols(conn)
	objectCounts["Protocols"] = len(protocols)
	protoMetadata := GetMetadataForObjectType(conn, TYPE_PROTOCOL)

	*sortables = append(*sortables, convertToSortableSlice(protocols)...)
	addToMetadataMap(protoMetadata, metadataMap)

	return protocols
}

func retrieveViews(conn *dbconn.DBConn, sortables *[]Sortable) {
	gplog.Verbose("Retrieving views")
	views := GetAllViews(conn)
	objectCounts["Views"] = len(views)

	*sortables = append(*sortables, convertToSortableSlice(views)...)
}

// https://github.com/greenplum-db/gpbackup/commit/abdf8cf26dddf97238bee4c4a3e1341eb0077fd5
func retrieveTSObjects(conn *dbconn.DBConn, sortables *[]Sortable, metadataMap MetadataMap) {
	if !(conn.Version.IsGPDB() && conn.Version.AtLeast("5")) && !conn.Version.IsCBDB() {
		return
	}
	gplog.Verbose("Retrieving Text Search Parsers")
	retrieveTSParsers(conn, sortables, metadataMap)
	retrieveTSConfigurations(conn, sortables, metadataMap)
	retrieveTSTemplates(conn, sortables, metadataMap)
	retrieveTSDictionaries(conn, sortables, metadataMap)
}

func retrieveTSParsers(conn *dbconn.DBConn, sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving Text Search Parsers")
	parsers := GetTextSearchParsers(conn)
	objectCounts["Text Search Parsers"] = len(parsers)
	parserMetadata := GetCommentsForObjectType(conn, TYPE_TSPARSER)

	*sortables = append(*sortables, convertToSortableSlice(parsers)...)
	addToMetadataMap(parserMetadata, metadataMap)
}

func retrieveTSTemplates(conn *dbconn.DBConn, sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving TEXT SEARCH TEMPLATE information")
	templates := GetTextSearchTemplates(conn)
	objectCounts["Text Search Templates"] = len(templates)
	templateMetadata := GetCommentsForObjectType(conn, TYPE_TSTEMPLATE)

	*sortables = append(*sortables, convertToSortableSlice(templates)...)
	addToMetadataMap(templateMetadata, metadataMap)
}

func retrieveTSDictionaries(conn *dbconn.DBConn, sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving TEXT SEARCH DICTIONARY information")
	dictionaries := GetTextSearchDictionaries(conn)
	objectCounts["Text Search Dictionaries"] = len(dictionaries)
	dictionaryMetadata := GetMetadataForObjectType(conn, TYPE_TSDICTIONARY)

	*sortables = append(*sortables, convertToSortableSlice(dictionaries)...)
	addToMetadataMap(dictionaryMetadata, metadataMap)
}

func retrieveTSConfigurations(conn *dbconn.DBConn, sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving TEXT SEARCH CONFIGURATION information")
	configurations := GetTextSearchConfigurations(conn)
	objectCounts["Text Search Configurations"] = len(configurations)
	configurationMetadata := GetMetadataForObjectType(conn, TYPE_TSCONFIGURATION)

	*sortables = append(*sortables, convertToSortableSlice(configurations)...)
	addToMetadataMap(configurationMetadata, metadataMap)
}

func retrieveOperatorObjects(conn *dbconn.DBConn, sortables *[]Sortable, metadataMap MetadataMap) {
	retrieveOperators(conn, sortables, metadataMap)
	retrieveOperatorClasses(conn, sortables, metadataMap)
}

func retrieveOperators(conn *dbconn.DBConn, sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving OPERATOR information")
	operators := GetOperators(conn)
	objectCounts["Operators"] = len(operators)
	operatorMetadata := GetMetadataForObjectType(conn, TYPE_OPERATOR)

	*sortables = append(*sortables, convertToSortableSlice(operators)...)
	addToMetadataMap(operatorMetadata, metadataMap)
}

func retrieveOperatorClasses(conn *dbconn.DBConn, sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving OPERATOR CLASS information")
	operatorClasses := GetOperatorClasses(conn)
	objectCounts["Operator Classes"] = len(operatorClasses)
	operatorClassMetadata := GetMetadataForObjectType(conn, TYPE_OPERATORCLASS)

	*sortables = append(*sortables, convertToSortableSlice(operatorClasses)...)
	addToMetadataMap(operatorClassMetadata, metadataMap)
}

func retrieveAggregates(conn *dbconn.DBConn, sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving AGGREGATE information")
	aggregates := GetAggregates(conn)
	objectCounts["Aggregates"] = len(aggregates)
	/* This call to get Metadata for Aggregates, although redundant, is preserved for
	 * consistency with other, similar methods.  The metadata for aggregate
	 * are located on the same catalog table as functions (pg_proc). This means that when we
	 * get function metadata we also get aggregate metadata at the same time.
	 */
	aggMetadata := GetMetadataForObjectType(conn, TYPE_AGGREGATE)

	*sortables = append(*sortables, convertToSortableSlice(aggregates)...)
	addToMetadataMap(aggMetadata, metadataMap)
}

func retrieveCasts(conn *dbconn.DBConn, sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving CAST information")
	casts := GetCasts(conn)
	objectCounts["Casts"] = len(casts)
	castMetadata := GetCommentsForObjectType(conn, TYPE_CAST)

	*sortables = append(*sortables, convertToSortableSlice(casts)...)
	addToMetadataMap(castMetadata, metadataMap)
}

func retrieveFDWObjects(conn *dbconn.DBConn, sortables *[]Sortable, metadataMap MetadataMap) {
	if !(conn.Version.IsGPDB() && conn.Version.AtLeast("6")) && !conn.Version.IsCBDB() {
		return
	}
	retrieveForeignDataWrappers(conn, sortables, metadataMap)
	retrieveForeignServers(conn, sortables, metadataMap)
	retrieveUserMappings(conn, sortables)
}

func retrieveForeignDataWrappers(conn *dbconn.DBConn, sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Writing CREATE FOREIGN DATA WRAPPER statements to metadata file")
	wrappers := GetForeignDataWrappers(conn)
	objectCounts["Foreign Data Wrappers"] = len(wrappers)
	fdwMetadata := GetMetadataForObjectType(conn, TYPE_FOREIGNDATAWRAPPER)

	*sortables = append(*sortables, convertToSortableSlice(wrappers)...)
	addToMetadataMap(fdwMetadata, metadataMap)
}

func retrieveForeignServers(conn *dbconn.DBConn, sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Writing CREATE SERVER statements to metadata file")
	servers := GetForeignServers(conn)
	objectCounts["Foreign Servers"] = len(servers)
	serverMetadata := GetMetadataForObjectType(conn, TYPE_FOREIGNSERVER)

	*sortables = append(*sortables, convertToSortableSlice(servers)...)
	addToMetadataMap(serverMetadata, metadataMap)
}

func retrieveUserMappings(conn *dbconn.DBConn, sortables *[]Sortable) {
	gplog.Verbose("Writing CREATE USER MAPPING statements to metadata file")
	mappings := GetUserMappings(conn)
	objectCounts["User Mappings"] = len(mappings)
	// No comments, owners, or ACLs on UserMappings so no need to get metadata

	*sortables = append(*sortables, convertToSortableSlice(mappings)...)
}

func backupSessionGUC(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing Session Configuration Parameters to metadata file")
	gucs := GetSessionGUCs(conn)
	PrintSessionGUCs(metadataFile, globalTOC, gucs)
}

/*
 * Global metadata wrapper functions
 */

func backupTablespaces(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE TABLESPACE statements to metadata file")
	tablespaces := GetTablespaces(conn)
	objectCounts["Tablespaces"] = len(tablespaces)
	tablespaceMetadata := GetMetadataForObjectType(conn, TYPE_TABLESPACE)
	PrintCreateTablespaceStatements(metadataFile, globalTOC, tablespaces, tablespaceMetadata)
}

func backupCreateDatabase(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE DATABASE statement to metadata file")
	defaultDB := GetDefaultDatabaseEncodingInfo(conn)
	db := GetDatabaseInfo(conn)
	dbMetadata := GetMetadataForObjectType(conn, TYPE_DATABASE)
	PrintCreateDatabaseStatement(metadataFile, globalTOC, defaultDB, db, dbMetadata)
}

func backupDatabaseGUCs(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing Database Configuration Parameters to metadata file")
	databaseGucs := GetDatabaseGUCs(conn)
	objectCounts["Database GUCs"] = len(databaseGucs)
	PrintDatabaseGUCs(metadataFile, globalTOC, databaseGucs, conn.DBName)
}

func backupResourceQueues(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE RESOURCE QUEUE statements to metadata file")
	resQueues := GetResourceQueues(conn)
	objectCounts["Resource Queues"] = len(resQueues)
	resQueueMetadata := GetCommentsForObjectType(conn, TYPE_RESOURCEQUEUE)
	PrintCreateResourceQueueStatements(metadataFile, globalTOC, resQueues, resQueueMetadata)
}

func backupResourceGroups(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	if !(conn.Version.IsGPDB() && conn.Version.AtLeast("5")) && !conn.Version.IsCBDB() {
		return
	}
	gplog.Verbose("Writing CREATE RESOURCE GROUP statements to metadata file")

	// at resource group part, CBDB is still same as 3x/GP6.
	if (conn.Version.IsGPDB() && conn.Version.Before("7")) || conn.Version.IsCBDB() {
		resGroups := GetResourceGroups[ResourceGroupBefore7](conn)
		objectCounts["Resource Groups"] = len(resGroups)
		resGroupMetadata := GetCommentsForObjectType(conn, TYPE_RESOURCEGROUP)
		PrintResetResourceGroupStatements(metadataFile, globalTOC)
		PrintCreateResourceGroupStatementsBefore7(metadataFile, globalTOC, resGroups, resGroupMetadata)
	} else { // GPDB7+
		resGroups := GetResourceGroups[ResourceGroupAtLeast7](conn)
		objectCounts["Resource Groups"] = len(resGroups)
		resGroupMetadata := GetCommentsForObjectType(conn, TYPE_RESOURCEGROUP)
		PrintResetResourceGroupStatements(metadataFile, globalTOC)
		PrintCreateResourceGroupStatementsAtLeast7(metadataFile, globalTOC, resGroups, resGroupMetadata)
	}
}

func backupRoles(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE ROLE statements to metadata file")
	roles := GetRoles(conn)
	objectCounts["Roles"] = len(roles)
	roleMetadata := GetMetadataForObjectType(conn, TYPE_ROLE)
	PrintCreateRoleStatements(metadataFile, globalTOC, roles, roleMetadata)
}

func backupRoleGUCs(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing ROLE Configuration Parameter to meadata file")
	roleGUCs := GetRoleGUCs(conn)
	PrintRoleGUCStatements(metadataFile, globalTOC, roleGUCs)
}

func backupRoleGrants(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing GRANT ROLE statements to metadata file")
	roleMembers := GetRoleMembers(conn)
	PrintRoleMembershipStatements(metadataFile, globalTOC, roleMembers)
}

/*
 * Predata wrapper functions
 */

func backupSchemas(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount, partitionAlteredSchemas map[string]bool) {
	gplog.Verbose("Writing CREATE SCHEMA statements to metadata file")
	schemas := GetAllUserSchemas(conn, partitionAlteredSchemas)
	objectCounts["Schemas"] = len(schemas)
	schemaMetadata := GetMetadataForObjectType(conn, TYPE_SCHEMA)
	PrintCreateSchemaStatements(metadataFile, globalTOC, schemas, schemaMetadata)
}

func backupProceduralLanguages(connectionPool *dbconn.DBConn, metadataFile *utils.FileWithByteCount,
	functions []Function, funcInfoMap map[uint32]FunctionInfo, functionMetadata MetadataMap) {
	gplog.Verbose("Writing CREATE PROCEDURAL LANGUAGE statements to metadata file")
	procLangs := GetProceduralLanguages(connectionPool)
	objectCounts["Procedural Languages"] = len(procLangs)
	langFuncs, _ := ExtractLanguageFunctions(functions, procLangs)
	for _, langFunc := range langFuncs {
		PrintCreateFunctionStatement(metadataFile, globalTOC, langFunc, functionMetadata[langFunc.GetUniqueID()])
	}
	procLangMetadata := GetMetadataForObjectType(connectionPool, TYPE_PROCLANGUAGE)
	PrintCreateLanguageStatements(metadataFile, globalTOC, procLangs, funcInfoMap, procLangMetadata)
}

func backupShellTypes(metadataFile *utils.FileWithByteCount, shellTypes []ShellType, baseTypes []BaseType, rangeTypes []RangeType) {
	gplog.Verbose("Writing CREATE TYPE statements for shell types to metadata file")
	PrintCreateShellTypeStatements(metadataFile, globalTOC, shellTypes, baseTypes, rangeTypes)
}

func backupEnumTypes(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount, typeMetadata MetadataMap) {
	gplog.Verbose("Writing CREATE TYPE statements for enum types to metadata file")
	enums := GetEnumTypes(conn)
	objectCounts["Types"] += len(enums)
	PrintCreateEnumTypeStatements(metadataFile, globalTOC, enums, typeMetadata)
}

// not used any more, https://github.com/greenplum-db/gpbackup/commit/0c421c6238d51ce5b75f555f5fbf2413bb53f0c0
func backupCreateSequences(metadataFile *utils.FileWithByteCount, sequences []Sequence, sequenceOwnerColumns map[string]string, relationMetadata MetadataMap) {
	gplog.Verbose("Writing CREATE SEQUENCE statements to metadata file")
	objectCounts["Sequences"] = len(sequences)
	PrintCreateSequenceStatements(metadataFile, globalTOC, sequences, relationMetadata)
}

func backupAccessMethods(connectionPool *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("7") {
		return
	}
	gplog.Verbose("Writing CREATE ACCESS METHOD statements to metadata file")
	accessMethods := GetAccessMethods(connectionPool)
	objectCounts["Access Methods"] = len(accessMethods)
	accessMethodsMetadata := GetMetadataForObjectType(connectionPool, TYPE_ACCESS_METHOD)
	PrintAccessMethodStatements(metadataFile, globalTOC, accessMethods, accessMethodsMetadata)
}

func createBackupSet(objSlice []Sortable) (backupSet map[UniqueID]bool) {
	backupSet = make(map[UniqueID]bool)
	for _, obj := range objSlice {
		backupSet[obj.GetUniqueID()] = true
	}

	return backupSet
}

func convertToSortableSlice(objSlice interface{}) []Sortable {
	sortableSlice := make([]Sortable, 0)
	s := reflect.ValueOf(objSlice)

	ret := make([]interface{}, s.Len())

	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Index(i).Interface()
	}

	for _, obj := range ret {
		newObj := obj.(Sortable)
		sortableSlice = append(sortableSlice, newObj)
	}

	return sortableSlice
}

func addToMetadataMap(newMetadata MetadataMap, metadataMap MetadataMap) {
	for k, v := range newMetadata {
		metadataMap[k] = v
	}
}

// This function is fairly unwieldy, but there's not really a good way to break it down
func backupDependentObjects(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount, tables []Table,
	protocols []ExternalProtocol, filteredMetadata MetadataMap, domainConstraints []Constraint,
	sortables []Sortable, sequences []Sequence, funcInfoMap map[uint32]FunctionInfo, tableOnly bool) {

	gplog.Verbose("Writing CREATE statements for dependent objects to metadata file")

	backupSet := createBackupSet(sortables)
	relevantDeps := GetDependencies(conn, backupSet, tables)
	if conn.Version.IsGPDB() && conn.Version.Is("4") && !tableOnly {
		AddProtocolDependenciesForGPDB4(relevantDeps, tables, protocols)
	}

	sortedSlice := TopologicalSort(sortables, relevantDeps)

	PrintDependentObjectStatements(metadataFile, globalTOC, sortedSlice, filteredMetadata, domainConstraints, funcInfoMap)

	// https://github.com/greenplum-db/gpbackup/commit/8768bebfec46a01c2862738de9c91ee7adbf47b0 "gpbackup should dump identity column metadata",
	// it's a little change later, https://github.com/greenplum-db/gpbackup/commit/5519469f02c3b785628e163c9096e3cbcbd10a0d
	PrintIdentityColumns(metadataFile, globalTOC, sequences)

	// https://github.com/greenplum-db/gpbackup/commit/0c421c6238d51ce5b75f555f5fbf2413bb53f0c0 "Refactor sequences"
	PrintAlterSequenceStatements(metadataFile, globalTOC, sequences)

	extPartInfo, partInfoMap := GetExternalPartitionInfo(conn)
	if conn.Version.IsGPDB() && conn.Version.Before("7") && len(extPartInfo) > 0 {
		gplog.Verbose("Writing EXCHANGE PARTITION statements to metadata file")
		PrintExchangeExternalPartitionStatements(metadataFile, globalTOC, extPartInfo, partInfoMap, tables)
	}
}

func backupConversions(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE CONVERSION statements to metadata file")
	conversions := GetConversions(conn)
	objectCounts["Conversions"] = len(conversions)
	convMetadata := GetMetadataForObjectType(conn, TYPE_CONVERSION)
	PrintCreateConversionStatements(metadataFile, globalTOC, conversions, convMetadata)
}

func backupOperatorFamilies(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	if !(conn.Version.IsGPDB() && conn.Version.AtLeast("5")) && !conn.Version.IsCBDB() {
		return
	}
	gplog.Verbose("Writing CREATE OPERATOR FAMILY statements to metadata file")
	operatorFamilies := GetOperatorFamilies(conn)
	objectCounts["Operator Families"] = len(operatorFamilies)
	operatorFamilyMetadata := GetMetadataForObjectType(conn, TYPE_OPERATORFAMILY)
	PrintCreateOperatorFamilyStatements(metadataFile, globalTOC, operatorFamilies, operatorFamilyMetadata)
}

func backupCollations(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	if !(conn.Version.IsGPDB() && conn.Version.AtLeast("6")) && !conn.Version.IsCBDB() {
		return
	}
	gplog.Verbose("Writing CREATE COLLATION statements to metadata file")
	collations := GetCollations(conn)
	objectCounts["Collations"] = len(collations)
	collationMetadata := GetMetadataForObjectType(conn, TYPE_COLLATION)
	PrintCreateCollationStatements(metadataFile, globalTOC, collations, collationMetadata)
}

func backupExtensions(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	if !(conn.Version.IsGPDB() && conn.Version.AtLeast("5")) && !conn.Version.IsCBDB() {
		return
	}
	gplog.Verbose("Writing CREATE EXTENSION statements to metadata file")
	extensions := GetExtensions(conn)
	objectCounts["Extensions"] = len(extensions)
	extensionMetadata := GetCommentsForObjectType(conn, TYPE_EXTENSION)
	PrintCreateExtensionStatements(metadataFile, globalTOC, extensions, extensionMetadata)
}

/*
func backupConstraints(metadataFile *utils.FileWithByteCount, constraints []Constraint, conMetadata MetadataMap) {
	gplog.Verbose("Writing ADD CONSTRAINT statements to metadata file")
	objectCounts["Constraints"] = len(constraints)
	PrintConstraintStatements(metadataFile, globalTOC, constraints, conMetadata)
}
*/

/*
 * Postdata wrapper functions
 */

func backupIndexes(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE INDEX statements to metadata file")
	indexes := GetIndexes(conn)
	objectCounts["Indexes"] = len(indexes)
	if objectCounts["Indexes"] > 0 && conn.Version.IsGPDB() && conn.Version.Is("6") {
		// This bug is not addressed in versions prior to GPDB6
		// New partition exchange syntax in GPDB7+ obviates the need for this renaming
		RenameExchangedPartitionIndexes(conn, &indexes)
	}
	indexMetadata := GetCommentsForObjectType(conn, TYPE_INDEX)
	PrintCreateIndexStatements(metadataFile, globalTOC, indexes, indexMetadata)
}

func backupRules(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE RULE statements to metadata file")
	rules := GetRules(conn)
	objectCounts["Rules"] = len(rules)
	ruleMetadata := GetCommentsForObjectType(conn, TYPE_RULE)
	PrintCreateRuleStatements(metadataFile, globalTOC, rules, ruleMetadata)
}

func backupTriggers(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE TRIGGER statements to metadata file")
	triggers := GetTriggers(conn)
	objectCounts["Triggers"] = len(triggers)
	triggerMetadata := GetCommentsForObjectType(conn, TYPE_TRIGGER)
	PrintCreateTriggerStatements(metadataFile, globalTOC, triggers, triggerMetadata)
}

func backupEventTriggers(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE EVENT TRIGGER statements to metadata file")
	eventTriggers := GetEventTriggers(conn)
	objectCounts["Event Triggers"] = len(eventTriggers)
	eventTriggerMetadata := GetMetadataForObjectType(conn, TYPE_EVENTTRIGGER)
	PrintCreateEventTriggerStatements(metadataFile, globalTOC, eventTriggers, eventTriggerMetadata)
}

func backupDefaultPrivileges(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing ALTER DEFAULT PRIVILEGES statements to metadata file")
	defaultPrivileges := GetDefaultPrivileges(conn)
	objectCounts["DEFAULT PRIVILEGES"] = len(defaultPrivileges)
	PrintDefaultPrivilegesStatements(metadataFile, globalTOC, defaultPrivileges)
}

func backupRowLevelSecurityPolicies(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE POLICY statements to metadata file")
	policies := GetPolicies(conn)
	objectCounts["Policies"] = len(policies)
	ruleMetadata := GetCommentsForObjectType(conn, TYPE_RULE)
	PrintCreatePolicyStatements(metadataFile, globalTOC, policies, ruleMetadata)
}

func backupExtendedStatistic(conn *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE STATISTICS statements to metadata file (for extended statistics)")
	statisticsExt := GetExtendedStatistics(conn)
	objectCounts["STATISTICS EXT"] = len(statisticsExt)
	statisticExtMetadata := GetMetadataForObjectType(conn, TYPE_STATISTIC_EXT)
	PrintCreateExtendedStatistics(metadataFile, globalTOC, statisticsExt, statisticExtMetadata)
}

/*
 * Data wrapper functions
 */

func backupTableStatistics(conn *dbconn.DBConn, statisticsFile *utils.FileWithByteCount, tables []Table) {
	attStats := GetAttributeStatistics(conn, tables)
	tupleStats := GetTupleStatistics(conn, tables)

	backupSessionGUC(conn, statisticsFile)
	PrintStatisticsStatements(statisticsFile, globalTOC, tables, attStats, tupleStats)
}
