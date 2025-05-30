package builtin

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_shared.go.
 */

import (
	"database/sql"
	"fmt"
	"sort"

	"github.com/cloudberrydb/cbcopy/internal/dbconn"
	"github.com/cloudberrydb/cbcopy/meta/builtin/toc"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

/*
 * Structs and functions relating to generic metadata handling.
 */

type MetadataQueryParams struct {
	NameField    string
	SchemaField  string
	ObjectType   string // https://github.com/greenplum-db/gpbackup/commit/5db5e23b61775e5bc831fd566845a3b99f8eca05
	OidField     string
	ACLField     string
	OwnerField   string
	OidTable     string
	CommentTable string
	CatalogTable string
	FilterClause string
	Shared       bool
}

var (
	TYPE_ACCESS_METHOD      MetadataQueryParams // https://github.com/greenplum-db/gpbackup/commit/4371988362a0a3690c1d3e3f18aa9d074d79fc48
	TYPE_AGGREGATE          MetadataQueryParams
	TYPE_CAST               MetadataQueryParams
	TYPE_COLLATION          MetadataQueryParams
	TYPE_CONSTRAINT         MetadataQueryParams
	TYPE_CONVERSION         MetadataQueryParams
	TYPE_DATABASE           MetadataQueryParams
	TYPE_EVENTTRIGGER       MetadataQueryParams
	TYPE_EXTENSION          MetadataQueryParams
	TYPE_FOREIGNDATAWRAPPER MetadataQueryParams
	TYPE_FOREIGNSERVER      MetadataQueryParams
	TYPE_FUNCTION           MetadataQueryParams
	TYPE_INDEX              MetadataQueryParams
	TYPE_PROCLANGUAGE       MetadataQueryParams
	TYPE_TRANSFORM          MetadataQueryParams
	TYPE_OPERATOR           MetadataQueryParams
	TYPE_OPERATORCLASS      MetadataQueryParams
	TYPE_OPERATORFAMILY     MetadataQueryParams
	TYPE_PROTOCOL           MetadataQueryParams
	TYPE_RELATION           MetadataQueryParams
	TYPE_RESOURCEGROUP      MetadataQueryParams
	TYPE_RESOURCEQUEUE      MetadataQueryParams
	TYPE_ROLE               MetadataQueryParams
	TYPE_RULE               MetadataQueryParams
	TYPE_SCHEMA             MetadataQueryParams
	TYPE_STATISTIC_EXT      MetadataQueryParams
	TYPE_TABLESPACE         MetadataQueryParams
	TYPE_TSCONFIGURATION    MetadataQueryParams
	TYPE_TSDICTIONARY       MetadataQueryParams
	TYPE_TSPARSER           MetadataQueryParams
	TYPE_TSTEMPLATE         MetadataQueryParams
	TYPE_TRIGGER            MetadataQueryParams
	TYPE_TYPE               MetadataQueryParams
	TYPE_POLICY             MetadataQueryParams
)

func InitializeMetadataParams(connectionPool *dbconn.DBConn) {
	TYPE_ACCESS_METHOD = MetadataQueryParams{ObjectType: "ACCESS METHOD", NameField: "amname", OidField: "oid", CatalogTable: "pg_am"}
	TYPE_AGGREGATE = MetadataQueryParams{ObjectType: "AGGREGATE", NameField: "proname", SchemaField: "pronamespace", ACLField: "proacl", OwnerField: "proowner", CatalogTable: "pg_proc"}
	if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily() {
		TYPE_AGGREGATE.FilterClause = "prokind = 'a'"
	} else {
		TYPE_AGGREGATE.FilterClause = "proisagg = 't'"
	}
	TYPE_CAST = MetadataQueryParams{ObjectType: "CAST", NameField: "typname", OidField: "oid", OidTable: "pg_type", CatalogTable: "pg_cast"}
	TYPE_COLLATION = MetadataQueryParams{ObjectType: "COLLATION", NameField: "collname", OidField: "oid", SchemaField: "collnamespace", OwnerField: "collowner", CatalogTable: "pg_collation"}
	TYPE_CONSTRAINT = MetadataQueryParams{ObjectType: "CONSTRAINT", NameField: "conname", SchemaField: "connamespace", OidField: "oid", CatalogTable: "pg_constraint"}
	TYPE_CONVERSION = MetadataQueryParams{ObjectType: "CONVERSION", NameField: "conname", OidField: "oid", SchemaField: "connamespace", OwnerField: "conowner", CatalogTable: "pg_conversion"}
	TYPE_DATABASE = MetadataQueryParams{ObjectType: "DATABASE", NameField: "datname", ACLField: "datacl", OwnerField: "datdba", CatalogTable: "pg_database", Shared: true}
	TYPE_EVENTTRIGGER = MetadataQueryParams{ObjectType: "EVENT TRIGGER", NameField: "evtname", OidField: "oid", OwnerField: "evtowner", CatalogTable: "pg_event_trigger"}
	TYPE_EXTENSION = MetadataQueryParams{ObjectType: "EXTENSION", NameField: "extname", OidField: "oid", CatalogTable: "pg_extension"}
	TYPE_FOREIGNDATAWRAPPER = MetadataQueryParams{ObjectType: "FOREIGN DATA WRAPPER", NameField: "fdwname", ACLField: "fdwacl", OwnerField: "fdwowner", CatalogTable: "pg_foreign_data_wrapper"}
	TYPE_FOREIGNSERVER = MetadataQueryParams{ObjectType: "SERVER", NameField: "srvname", ACLField: "srvacl", OwnerField: "srvowner", CatalogTable: "pg_foreign_server"}
	TYPE_FUNCTION = MetadataQueryParams{ObjectType: "FUNCTION", NameField: "proname", SchemaField: "pronamespace", ACLField: "proacl", OwnerField: "proowner", CatalogTable: "pg_proc"}
	if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily() {
		TYPE_FUNCTION.FilterClause = "prokind <> 'a'"
	} else {
		TYPE_FUNCTION.FilterClause = "proisagg = 'f'"
	}
	TYPE_INDEX = MetadataQueryParams{ObjectType: "INDEX", NameField: "relname", OidField: "indexrelid", OidTable: "pg_class", CommentTable: "pg_class", CatalogTable: "pg_index"}
	TYPE_PROCLANGUAGE = MetadataQueryParams{ObjectType: "LANGUAGE", NameField: "lanname", ACLField: "lanacl", CatalogTable: "pg_language"}
	if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("5") {
		TYPE_PROCLANGUAGE.OwnerField = "10" // In GPDB 4.3, there is no lanowner field in pg_language, but languages have an implicit owner
	} else {
		TYPE_PROCLANGUAGE.OwnerField = "lanowner"
	}
	TYPE_OPERATOR = MetadataQueryParams{ObjectType: "OPERATOR", NameField: "oprname", SchemaField: "oprnamespace", OidField: "oid", OwnerField: "oprowner", CatalogTable: "pg_operator"}
	TYPE_OPERATORCLASS = MetadataQueryParams{ObjectType: "OPERATOR CLASS", NameField: "opcname", SchemaField: "opcnamespace", OidField: "oid", OwnerField: "opcowner", CatalogTable: "pg_opclass"}
	TYPE_OPERATORFAMILY = MetadataQueryParams{ObjectType: "OPERATOR FAMILY", NameField: "opfname", SchemaField: "opfnamespace", OidField: "oid", OwnerField: "opfowner", CatalogTable: "pg_opfamily"}
	TYPE_PROTOCOL = MetadataQueryParams{ObjectType: "PROTOCOL", NameField: "ptcname", ACLField: "ptcacl", OwnerField: "ptcowner", CatalogTable: "pg_extprotocol"}
	TYPE_RELATION = MetadataQueryParams{ObjectType: "RELATION", NameField: "relname", SchemaField: "relnamespace", ACLField: "relacl", OwnerField: "relowner", CatalogTable: "pg_class"}
	TYPE_RESOURCEGROUP = MetadataQueryParams{ObjectType: "RESOURCE GROUP", NameField: "rsgname", OidField: "oid", CatalogTable: "pg_resgroup", Shared: true}
	TYPE_RESOURCEQUEUE = MetadataQueryParams{ObjectType: "RESOURCE QUEUE", NameField: "rsqname", OidField: "oid", CatalogTable: "pg_resqueue", Shared: true}
	TYPE_ROLE = MetadataQueryParams{ObjectType: "ROLE", NameField: "rolname", OidField: "oid", CatalogTable: "pg_authid", Shared: true}
	TYPE_RULE = MetadataQueryParams{ObjectType: "RULE", NameField: "rulename", OidField: "oid", CatalogTable: "pg_rewrite"}
	TYPE_SCHEMA = MetadataQueryParams{ObjectType: "SCHEMA", NameField: "nspname", ACLField: "nspacl", OwnerField: "nspowner", CatalogTable: "pg_namespace"}
	TYPE_STATISTIC_EXT = MetadataQueryParams{ObjectType: "STATISTIC_EXT", NameField: "stxname", OwnerField: "stxowner", CatalogTable: "pg_statistic_ext"}
	TYPE_TABLESPACE = MetadataQueryParams{ObjectType: "TABLESPACE", NameField: "spcname", ACLField: "spcacl", OwnerField: "spcowner", CatalogTable: "pg_tablespace", Shared: true}
	TYPE_TRANSFORM = MetadataQueryParams{ObjectType: "TRANSFORM", CatalogTable: "pg_transform"}
	TYPE_TSCONFIGURATION = MetadataQueryParams{ObjectType: "TEXT SEARCH CONFIGURATION", NameField: "cfgname", OidField: "oid", SchemaField: "cfgnamespace", OwnerField: "cfgowner", CatalogTable: "pg_ts_config"}
	TYPE_TSDICTIONARY = MetadataQueryParams{ObjectType: "TEXT SEARCH DICTIONARY", NameField: "dictname", OidField: "oid", SchemaField: "dictnamespace", OwnerField: "dictowner", CatalogTable: "pg_ts_dict"}
	TYPE_TSPARSER = MetadataQueryParams{ObjectType: "TEXT SEARCH PARSER", NameField: "prsname", OidField: "oid", SchemaField: "prsnamespace", CatalogTable: "pg_ts_parser"}
	TYPE_TSTEMPLATE = MetadataQueryParams{ObjectType: "TEXT SEARCH TEMPLATE", NameField: "tmplname", OidField: "oid", SchemaField: "tmplnamespace", CatalogTable: "pg_ts_template"}
	TYPE_TRIGGER = MetadataQueryParams{ObjectType: "TRIGGER", NameField: "tgname", OidField: "oid", CatalogTable: "pg_trigger"}
	TYPE_TYPE = MetadataQueryParams{ObjectType: "TYPE", NameField: "typname", SchemaField: "typnamespace", OwnerField: "typowner", CatalogTable: "pg_type"}
	if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
		TYPE_TYPE.ACLField = "typacl"
	}
}

// https://github.com/greenplum-db/gpbackup/commit/5db5e23b61775e5bc831fd566845a3b99f8eca05 ( 2020/3. cbcopy was likely before 2020/2 )
type MetadataQueryStruct struct {
	UniqueID
	Name                  string
	ObjectType            string
	Schema                string
	Owner                 string
	Comment               string
	Privileges            sql.NullString
	Kind                  string
	SecurityLabel         string
	SecurityLabelProvider string
}

func GetMetadataForObjectType(connectionPool *dbconn.DBConn, params MetadataQueryParams) MetadataMap {
	gplog.Verbose("Getting object type metadata from " + params.CatalogTable)

	tableName := params.CatalogTable
	nameCol := "''"
	if params.NameField != "" {
		nameCol = params.NameField
	}
	aclCols := "''"
	kindCol := "''"
	aclLateralJoin := ""
	if params.ACLField != "" {
		// Cannot use unnest() in CASE statements anymore in GPDB 7+ so convert
		// it to a LEFT JOIN LATERAL. We do not use LEFT JOIN LATERAL for GPDB 6
		// because the CASE unnest() logic is more performant.
		if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily() {
			aclLateralJoin = fmt.Sprintf(
				`LEFT JOIN LATERAL unnest(o.%[1]s) ljl_unnest ON o.%[1]s IS NOT NULL AND array_length(o.%[1]s, 1) != 0`, params.ACLField)
			aclCols = "ljl_unnest"
		} else {
			aclCols = fmt.Sprintf(`CASE
			WHEN %[1]s IS NULL THEN NULL
			WHEN array_upper(%[1]s, 1) = 0 THEN %[1]s[0]
			ELSE unnest(%[1]s) END`, params.ACLField)
		}

		kindCol = fmt.Sprintf(`CASE
		WHEN %[1]s IS NULL THEN ''
		WHEN array_upper(%[1]s, 1) = 0 THEN 'Empty'
		ELSE '' END`, params.ACLField)
	}
	schemaCol := "''"
	joinClause := ""
	filterClause := "1 = 1"
	if params.SchemaField != "" {
		schemaCol = "n.nspname"
		joinClause = fmt.Sprintf(`JOIN pg_namespace n ON o.%s = n.oid`, params.SchemaField)
		filterClause = SchemaFilterClause("n")
	}
	descTable := "pg_description"
	subidFilter := " AND d.objsubid = 0"
	if params.Shared {
		descTable = "pg_shdescription"
		subidFilter = ""
	}
	ownerCol := "''"
	if params.OwnerField != "" {
		ownerCol = fmt.Sprintf("quote_ident(pg_get_userbyid(%s))", params.OwnerField)
	}
	secCols := ""
	if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
		secCols = `coalesce(sec.label,'') AS securitylabel,
		coalesce(sec.provider, '') AS securitylabelprovider,`
		secTable := "pg_seclabel"
		secSubidFilter := " AND sec.objsubid = 0"
		if params.Shared {
			secTable = "pg_shseclabel"
			secSubidFilter = ""
		}
		joinClause += fmt.Sprintf(
			` LEFT JOIN %s sec ON (sec.objoid = o.oid AND sec.classoid = '%s'::regclass%s)`,
			secTable, tableName, secSubidFilter)
	}
	if params.FilterClause != "" {
		filterClause += " AND " + params.FilterClause
	}

	query := fmt.Sprintf(`
		SELECT
		'%s' AS objecttype,
		'%s'::regclass::oid AS classid,
		o.oid,
		coalesce(quote_ident(%s),'') AS name,
		%s AS kind,
		coalesce(quote_ident(%s),'') AS schema,
		%s AS owner,
		%s AS privileges,
		%s
		coalesce(description,'') AS comment
	FROM %s o LEFT JOIN %s d ON (d.objoid = o.oid AND d.classoid = '%s'::regclass%s)
		%s
		%s
	WHERE %s
	ORDER BY o.oid`,
		params.ObjectType, tableName, nameCol, kindCol, schemaCol, ownerCol, aclCols, secCols,
		tableName, descTable, tableName, subidFilter, joinClause, aclLateralJoin, filterClause)

	gplog.Debug("GetMetadataForObjectType, query is %v", query)
	results := make([]MetadataQueryStruct, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	return ConstructMetadataMap(results)
}

func sortACLs(privileges []ACL) []ACL {
	sort.Slice(privileges, func(i, j int) bool {
		return privileges[i].Grantee < privileges[j].Grantee
	})
	return privileges
}

func GetCommentsForObjectType(connectionPool *dbconn.DBConn, params MetadataQueryParams) MetadataMap {
	selectClause := fmt.Sprintf(`
	SELECT '%s'::regclass::oid AS classid,
		o.%s AS oid,
		coalesce(description,'') AS comment
		`, params.CatalogTable, params.OidField)

	joinStr := ""
	if params.SchemaField != "" {
		joinStr = fmt.Sprintf(`JOIN pg_namespace n ON o.%s = n.oid`, params.SchemaField)
	}
	descTable := "pg_description"
	subidStr := " AND d.objsubid = 0"
	if params.Shared {
		descTable = "pg_shdescription"
		subidStr = ""
	}
	commentTable := params.CatalogTable
	if params.CommentTable != "" {
		commentTable = params.CommentTable
	}

	fromClause := fmt.Sprintf(`
	FROM %s o
		JOIN %s d ON (d.objoid = %s AND d.classoid = '%s'::regclass%s)
		%s`, params.CatalogTable, descTable, params.OidField, commentTable,
		subidStr, joinStr)

	whereClause := fmt.Sprintf("\nWHERE o.%s >= %d", params.OidField, FIRST_NORMAL_OBJECT_ID)
	if params.SchemaField != "" {
		whereClause += " AND " + SchemaFilterClause("n")
	}

	query := selectClause + fromClause + whereClause

	gplog.Debug("GetCommentsForObjectType, query is %v", query)
	results := make([]struct {
		UniqueID
		Comment string
	}, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	metadataMap := make(MetadataMap)
	if len(results) > 0 {
		for _, result := range results {
			metadataMap[result.UniqueID] = ObjectMetadata{
				[]ACL{},
				params.ObjectType,
				"",
				result.Comment,
				"",
				""}
		}
	}
	return metadataMap
}

type DefaultPrivilegesQueryStruct struct {
	Oid        uint32
	Owner      string
	Schema     string
	Privileges sql.NullString
	Kind       string
	ObjectType string
}

type DefaultPrivileges struct {
	Owner      string
	Schema     string
	Privileges []ACL
	ObjectType string
}

func (dp DefaultPrivileges) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "postdata",
		toc.MetadataEntry{
			Schema:          dp.Schema,
			Name:            "",
			ObjectType:      "DEFAULT PRIVILEGES",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
			//Oid:             0,
			//ClassOID:        0,
		}
}

func GetDefaultPrivileges(connectionPool *dbconn.DBConn) []DefaultPrivileges {
	// Cannot use unnest() in CASE statements anymore in GPDB 7+ so convert
	// it to a LEFT JOIN LATERAL. We do not use LEFT JOIN LATERAL for GPDB 6
	// because the CASE unnest() logic is more performant.
	aclCols := "''"
	aclLateralJoin := ""
	if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily() {
		aclLateralJoin =
			`LEFT JOIN LATERAL unnest(a.defaclacl) ljl_unnest ON a.defaclacl IS NOT NULL AND array_length(a.defaclacl, 1) != 0`
		aclCols = "ljl_unnest"
	} else {
		aclCols = `CASE
			WHEN a.defaclacl IS NULL THEN NULL
			WHEN array_upper(a.defaclacl, 1) = 0 THEN a.defaclacl[0]
			ELSE unnest(a.defaclacl) END`
	}
	query := fmt.Sprintf(`
	SELECT a.oid,
		quote_ident(r.rolname) AS owner,
		coalesce(quote_ident(n.nspname),'') AS schema,
		%s AS privileges,
		CASE
			WHEN a.defaclacl IS NULL THEN ''
			WHEN array_upper(a.defaclacl, 1) = 0 THEN 'Empty'
			ELSE ''
		END AS kind,
		a.defaclobjtype AS objecttype
	FROM pg_default_acl a
		JOIN pg_roles r ON r.oid = a.defaclrole
		LEFT JOIN pg_namespace n ON n.oid = a.defaclnamespace
		%s
	ORDER BY n.nspname, a.defaclobjtype, r.rolname`,
		aclCols, aclLateralJoin)

	gplog.Debug("GetDefaultPrivileges, query is %v", query)
	results := make([]DefaultPrivilegesQueryStruct, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	return ConstructDefaultPrivileges(results)
}

func getQuotedRoleNames(connectionPool *dbconn.DBConn) map[string]string {
	query := `
		SELECT rolname AS rolename, quote_ident(rolname) AS quotedrolename FROM pg_authid`

	gplog.Debug("getQuotedRoleNames, query is %v", query)
	results := make([]struct {
		RoleName       string
		QuotedRoleName string
	}, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	quotedRoleNames = make(map[string]string)
	for _, result := range results {
		quotedRoleNames[result.RoleName] = result.QuotedRoleName
	}
	return quotedRoleNames
}
