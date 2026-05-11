package builtin

import (
	"strings"

	"github.com/cloudberry-contrib/cbcopy/internal/dbconn"
	"github.com/cloudberry-contrib/cbcopy/meta/builtin/toc"
	"github.com/cloudberry-contrib/cbcopy/option"
	"github.com/spf13/pflag"
)

// gpbackup, backup/global_variables.go

/*
 * Empty struct type used for value with 0 bytes
 */
type Empty struct{}

var (
	srcDBVersion  dbconn.GPDBVersion
	destDBVersion dbconn.GPDBVersion

	// srcDbName and destDbName name the source and destination databases
	// currently being processed. Set by BuiltinMeta.Open; consumed by
	// FilterTablesByDestExisting and the skip-existing recorder so each
	// recorded entry knows which db pair it belongs to (relevant in
	// multi-db copy modes like --full or --dbname db1,db2).
	srcDbName  string
	destDbName string

	// runtimeOption is a handle to the global *option.Option used by
	// non-pure-flag-driven decisions inside meta/builtin, primarily the
	// --skip-existing existence check. Set by SetOption from the copy package
	// once option.NewOption has produced an Option instance.
	runtimeOption *option.Option

	// --> automation test purpose, start
	cmdFlags *pflag.FlagSet
	// <-- automation test purpose, end

	globalTOC            *toc.TOC
	quotedRoleNames      map[string]string
	filterRelationClause string
	excludeRelations     []string
	includeRelations     []string
	includeSchemas       []string
	excludeSchemas       []string
	objectCounts         map[string]int

	errorTablesMetadata map[string]Empty
	relevantDeps        DependencyMap
	redirectSchema      map[string]string
	inclDestSchema      string
	ownerMap            map[string]string

	destTablespace    string
	destTablespaceMap map[string]string
)

// SetOption installs the runtime *option.Option for use by meta/builtin
// helpers that need to consult Option state (e.g. --skip-existing existence
// checks). Call after option.NewOption returns. Mirrors utils.SetCmdFlags.
func SetOption(o *option.Option) {
	runtimeOption = o
}

// --> automation test purpose, start
func SetCmdFlags(flagSet *pflag.FlagSet) {
	cmdFlags = flagSet
}

func SetConnection(conn *dbconn.DBConn) {
	// current code many place has connectionPool, which is not very clear how it's used, let's comment it out to see effect.
	// connectionPool = conn
	srcDBVersion = conn.Version
	destDBVersion = conn.Version
}

func SetQuotedRoleNames(quotedRoles map[string]string) {
	quotedRoleNames = quotedRoles
}

func SetFilterRelationClause(filterClause string) {
	filterRelationClause = filterClause
}

func SetSchemaFilter(optionName string, optionValue string) {
	if optionName == option.SCHEMA {
		if strings.TrimSpace(optionValue) != "" {
			includeSchemas = append(includeSchemas, optionValue)
		} else {
			includeSchemas = includeSchemas[0:0]
		}
	} else if optionName == option.EXCLUDE_SCHEMA {
		if strings.TrimSpace(optionValue) != "" {
			excludeSchemas = append(excludeSchemas, optionValue)
		} else {
			excludeSchemas = excludeSchemas[0:0]
		}
	}
}

func SetRelationFilter(optionName string, optionValue string) {
	if optionName == option.INCLUDE_TABLE {
		if strings.TrimSpace(optionValue) != "" {
			includeRelations = append(includeRelations, optionValue)
		} else {
			includeRelations = includeRelations[0:0]
		}
	} else if optionName == option.EXCLUDE_TABLE {
		if strings.TrimSpace(optionValue) != "" {
			excludeRelations = append(excludeRelations, optionValue)
		} else {
			excludeRelations = excludeRelations[0:0]
		}
	}
}

// <-- automation test purpose, end
