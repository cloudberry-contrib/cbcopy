package builtin

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in postdata.go.
 */

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/cloudberry-contrib/cbcopy/internal/dbconn"
	"github.com/cloudberry-contrib/cbcopy/meta/builtin/toc"
	"github.com/cloudberry-contrib/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

/*
 * This function constructs the names of implicit indexes created by
 * unique constraints on tables, so they can be filtered out of the
 * index list.
 *
 * Primary key indexes can only be created implicitly by a primary
 * key constraint, so they can be filtered out directly in the query
 * to get indexes, but multiple unique indexes can be created on the
 * same column so we only want to filter out the implicit ones.
 */
// https://github.com/greenplum-db/gpbackup/commit/629a32dab4a7a285bcf7850958b28a777716949f
func ConstructImplicitIndexOidList(connectionPool *dbconn.DBConn) string {
	query := fmt.Sprintf(`
	SELECT i.indexrelid
	FROM pg_index i
		JOIN pg_depend d on i.indexrelid = d.objid
		JOIN pg_constraint c on d.refobjid = c.oid
	WHERE i.indexrelid >= %d
		AND i.indisunique is true
		AND i.indisprimary is false;`, FIRST_NORMAL_OBJECT_ID)
	indexNames := dbconn.MustSelectStringSlice(connectionPool, query)
	return utils.SliceToQuotedString(indexNames)
}

type IndexDefinition struct {
	Oid                uint32
	Name               string
	OwningSchema       string
	OwningTable        string
	Tablespace         string
	Def                sql.NullString
	IsClustered        bool
	SupportsConstraint bool
	IsReplicaIdentity  bool
	// https://github.com/greenplum-db/gpbackup/commit/2b0ffa69e541becf7a15b6c3fffa3ab735cc3170
	StatisticsColumns string
	StatisticsValues  string
	// https://github.com/greenplum-db/gpbackup/commit/e28433d09febb0c10fea516388ec34eeeda1c9db
	ParentIndex    uint32
	ParentIndexFQN string
}

func (i IndexDefinition) GetMetadataEntry() (string, toc.MetadataEntry) {
	tableFQN := utils.MakeFQN(i.OwningSchema, i.OwningTable)
	return "postdata",
		toc.MetadataEntry{
			Schema:          i.OwningSchema,
			Name:            i.Name,
			ObjectType:      "INDEX",
			ReferenceObject: tableFQN,
			StartByte:       0,
			EndByte:         0,
			//Oid:             i.Oid,
			//ClassOID:        PG_INDEX_OID,
		}
}

func (i IndexDefinition) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_INDEX_OID, Oid: i.Oid}
}

func (i IndexDefinition) FQN() string {
	return utils.MakeFQN(i.OwningSchema, i.Name)
}

/*
 * GetIndexes queries for all user and implicitly created indexes, since
 * implicitly created indexes could still have metadata to be backed up.
 * e.g. comments on implicitly created indexes
 */
func GetIndexes(connectionPool *dbconn.DBConn) []IndexDefinition {
	implicitIndexStr := ""
	if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("6") {
		indexOidList := ConstructImplicitIndexOidList(connectionPool)

		if indexOidList != "" {
			implicitIndexStr = fmt.Sprintf("OR i.indexrelid IN (%s)", indexOidList)
		}
	}
	before6Query := fmt.Sprintf(`
	SELECT DISTINCT i.indexrelid AS oid,
		quote_ident(ic.relname) AS name,
		quote_ident(n.nspname) AS owningschema,
		quote_ident(c.relname) AS owningtable,
		coalesce(quote_ident(s.spcname), '') AS tablespace,
		pg_get_indexdef(i.indexrelid) AS def,
		i.indisclustered AS isclustered,
		CASE
			WHEN i.indisprimary = 't' %s THEN 't'
			ELSE 'f'
		END AS supportsconstraint
	FROM pg_index i
		JOIN pg_class ic ON (ic.oid = i.indexrelid)
		JOIN pg_namespace n ON (ic.relnamespace = n.oid)
		JOIN pg_class c ON (c.oid = i.indrelid)
		LEFT JOIN pg_tablespace s ON (ic.reltablespace = s.oid)
	WHERE %s
		AND i.indisvalid
		AND NOT EXISTS (SELECT 1 FROM pg_partition_rule r WHERE r.parchildrelid = c.oid)
		AND %s
	ORDER BY name`,
		implicitIndexStr, relationAndSchemaFilterClause(connectionPool), ExtensionFilterClause("c"))

	version6Query := fmt.Sprintf(`
	SELECT DISTINCT i.indexrelid AS oid,
		quote_ident(ic.relname) AS name,
		quote_ident(n.nspname) AS owningschema,
		quote_ident(c.relname) AS owningtable,
		coalesce(quote_ident(s.spcname), '') AS tablespace,
		pg_get_indexdef(i.indexrelid) AS def,
		i.indisclustered AS isclustered,
		i.indisreplident AS isreplicaidentity,
		CASE
			WHEN conindid > 0 THEN 't'
			ELSE 'f'
		END as supportsconstraint
	FROM pg_index i
		JOIN pg_class ic ON ic.oid = i.indexrelid
		JOIN pg_namespace n ON ic.relnamespace = n.oid
		JOIN pg_class c ON c.oid = i.indrelid
		LEFT JOIN pg_tablespace s ON ic.reltablespace = s.oid
		LEFT JOIN pg_constraint con ON i.indexrelid = con.conindid
	WHERE %s
		AND i.indisvalid
		AND i.indisready
		AND i.indisprimary = 'f'
		AND NOT EXISTS (SELECT 1 FROM pg_partition_rule r WHERE r.parchildrelid = c.oid)
		AND %s
	ORDER BY name`,
		relationAndSchemaFilterClause(connectionPool), ExtensionFilterClause("c")) // The index itself does not have a dependency on the extension, but the index's table does

	atLeast7Query := fmt.Sprintf(`
		SELECT DISTINCT i.indexrelid AS oid,
			coalesce(inh.inhparent, '0') AS parentindex,
			quote_ident(ic.relname) AS name,
			quote_ident(n.nspname) AS owningschema,
			quote_ident(c.relname) AS owningtable,
			coalesce(quote_ident(s.spcname), '') AS tablespace,
			pg_get_indexdef(i.indexrelid) AS def,
			i.indisclustered AS isclustered,
			i.indisreplident AS isreplicaidentity,
			CASE
				WHEN conindid > 0 THEN 't'
				ELSE 'f'
			END as supportsconstraint,
			coalesce(array_to_string((SELECT pg_catalog.array_agg(attnum ORDER BY attnum) FROM pg_catalog.pg_attribute WHERE attrelid = i.indexrelid AND attstattarget >= 0), ','), '') as statisticscolumns,
			coalesce(array_to_string((SELECT pg_catalog.array_agg(attstattarget ORDER BY attnum) FROM pg_catalog.pg_attribute WHERE attrelid = i.indexrelid AND attstattarget >= 0), ','), '') as statisticsvalues	
		FROM pg_index i
			JOIN pg_class ic ON ic.oid = i.indexrelid
			JOIN pg_namespace n ON ic.relnamespace = n.oid
			JOIN pg_class c ON c.oid = i.indrelid
			LEFT JOIN pg_tablespace s ON ic.reltablespace = s.oid
			LEFT JOIN pg_constraint con ON i.indexrelid = con.conindid
			LEFT JOIN pg_catalog.pg_inherits inh ON inh.inhrelid = i.indexrelid
		WHERE %s
			AND i.indisready
			AND i.indisprimary = 'f'
			AND i.indexrelid >= %d
			AND %s
		ORDER BY name`,
		relationAndSchemaFilterClause(connectionPool), FIRST_NORMAL_OBJECT_ID, ExtensionFilterClause("c"))

	query := ""
	if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("6") {
		query = before6Query
	} else if connectionPool.Version.IsGPDB() && connectionPool.Version.Is("6") {
		query = version6Query
	} else {
		query = atLeast7Query
	}

	gplog.Debug("GetIndexes, query is %v", query)
	resultIndexes := make([]IndexDefinition, 0)
	err := connectionPool.Select(&resultIndexes, query)
	gplog.FatalOnError(err)

	// Remove all indexes that have NULL definitions. This can happen
	// if a concurrent index drop happens before the associated table
	// lock is acquired earlier during gpbackup execution.
	verifiedResultIndexes := make([]IndexDefinition, 0)
	indexMap := make(map[uint32]IndexDefinition, 0)
	for _, index := range resultIndexes {
		if index.Def.Valid {
			verifiedResultIndexes = append(verifiedResultIndexes, index)
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily() {
				indexMap[index.Oid] = index // hash index for topological sort
			}
		} else {
			gplog.Warn("Index '%s' on table '%s.%s' not backed up, most likely dropped after gpbackup had begun.",
				index.Name, index.OwningSchema, index.OwningTable)
		}
	}

	if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("7") {
		return verifiedResultIndexes
	}

	// Since GPDB 7+ partition indexes can now be ALTERED to attach to a parent
	// index. Topological sort indexes to ensure parent indexes are printed
	// before their child indexes.
	visited := make(map[uint32]struct{})
	sortedIndexes := make([]IndexDefinition, 0)
	stack := make([]uint32, 0)
	var seen struct{}
	for _, index := range verifiedResultIndexes {
		currIndex := index
		// Depth-first search loop. Store visited indexes to a stack
		for {
			if _, indexWasVisited := visited[currIndex.Oid]; indexWasVisited {
				break // exit DFS if a visited index is found.
			}

			stack = append(stack, currIndex.Oid)
			visited[currIndex.Oid] = seen
			parentIndex, parentIsPresent := indexMap[currIndex.ParentIndex]
			if currIndex.ParentIndex == 0 || !parentIsPresent {
				break // exit DFS if index has no parent.
			} else {
				currIndex = parentIndex
			}
		}

		// "Pop" indexes found by DFS
		for i := len(stack) - 1; i >= 0; i-- {
			indexOid := stack[i]
			popIndex := indexMap[indexOid]
			if popIndex.ParentIndex != 0 {
				// Preprocess parent index FQN for GPDB 7+ partition indexes
				popIndex.ParentIndexFQN = indexMap[popIndex.ParentIndex].FQN()
			}
			sortedIndexes = append(sortedIndexes, popIndex)
		}
		stack = stack[:0] // empty slice but keep memory allocation
	}

	return sortedIndexes
}

func GetRenameExchangedPartitionQuery(connectionPool *dbconn.DBConn) string {
	// In the case of exchanged partition tables, restoring index constraints with system-generated
	// will cause a name collision in GPDB7+.  Rename those constraints to match their new owning
	// tables.  In GPDB6 and below this renaming was done automatically by server code.
	cteClause := ""
	if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("7") {
		cteClause = `SELECT DISTINCT cl.relname
            FROM pg_class cl
                INNER JOIN pg_partitions pts
					ON cl.relname = pts.partitiontablename
					AND cl.relname != pts.tablename
            WHERE cl.relkind IN ('r', 'f')`
	} else {
		cteClause = `SELECT DISTINCT cl.relname
             FROM pg_class cl
             WHERE
                cl.relkind IN ('r', 'f')
                AND cl.relispartition = true
                AND cl.relhassubclass = false`
	}
	query := fmt.Sprintf(`
        WITH table_cte AS (%s)
        SELECT
            ic.relname AS origname,
            rc.relname || SUBSTRING(ic.relname, LENGTH(ch.relname)+1, LENGTH(ch.relname)) AS newname
        FROM
            pg_index i
            JOIN pg_class ic ON i.indexrelid = ic.oid
            JOIN pg_class rc
                ON i.indrelid = rc.oid
                AND rc.relname != SUBSTRING(ic.relname, 1, LENGTH(rc.relname))
            JOIN pg_namespace n ON rc.relnamespace = n.oid
            INNER JOIN table_cte ch
                ON SUBSTRING(ic.relname, 1, LENGTH(ch.relname)) = ch.relname
                AND rc.relname != ch.relname
        WHERE %s;`, cteClause, SchemaFilterClause("n"))
	return query
}

func RenameExchangedPartitionIndexes(connectionPool *dbconn.DBConn, indexes *[]IndexDefinition) {
	query := GetRenameExchangedPartitionQuery(connectionPool)

	gplog.Debug("RenameExchangedPartitionIndexes, query is %v", query)
	names := make([]ExchangedPartitionName, 0)
	err := connectionPool.Select(&names, query)
	gplog.FatalOnError(err)

	nameMap := make(map[string]string)
	for _, name := range names {
		nameMap[name.OrigName] = name.NewName
	}

	for idx := range *indexes {
		newName, hasNewName := nameMap[(*indexes)[idx].Name]
		if hasNewName {
			(*indexes)[idx].Def.String = strings.Replace((*indexes)[idx].Def.String, (*indexes)[idx].Name, newName, 1)
			(*indexes)[idx].Name = newName
		}
	}
}

// GetPartitionColumnsByTable queries the database for partition key information of all partitioned tables
// and returns the result as a map where key is "schema.table" and value is a list of column names sorted by partition level
func GetPartitionColumnsByTable(connectionPool *dbconn.DBConn) map[string][]string {
	// Map to store results, with key as schema.table and value as array of partition column names
	partitionColumnsByTable := make(map[string][]string)

	// Query to get partition columns for each table, sorted by partition level
	query := `
	WITH distinct_columns AS (
		SELECT DISTINCT schemaname, tablename, columnname, partitionlevel
		FROM pg_partition_columns
	)
	SELECT 
		schemaname, 
		tablename,
		array_to_string(array_agg(columnname ORDER BY partitionlevel), ',') AS columnnames
	FROM distinct_columns
	GROUP BY schemaname, tablename`

	// Define struct to receive query results
	type partitionColumnResult struct {
		SchemaName  string
		TableName   string
		ColumnNames string // Comma-separated column names
	}

	results := make([]partitionColumnResult, 0)

	gplog.Debug("GetPartitionColumnsByTable, query is %v", query)
	err := connectionPool.Select(&results, query)
	if err != nil {
		gplog.Warn("Failed to get partition column information: %v", err)
		return partitionColumnsByTable
	}

	// Process query results, convert comma-separated column names to array and store in map
	for _, result := range results {
		tableFQN := fmt.Sprintf("%s.%s", result.SchemaName, result.TableName)
		columnNames := strings.Split(result.ColumnNames, ",")

		// Clean each column name (remove possible spaces)
		for i, col := range columnNames {
			columnNames[i] = strings.TrimSpace(col)
		}

		partitionColumnsByTable[tableFQN] = columnNames
	}

	gplog.Debug("Retrieved partition key information for %d partitioned tables", len(partitionColumnsByTable))
	return partitionColumnsByTable
}

// EnsurePartitionKeysInUniqueIndexDefs modifies unique index definitions on partitioned tables
// to ensure they include all partition key columns
func EnsurePartitionKeysInUniqueIndexDefs(connectionPool *dbconn.DBConn, indexes *[]IndexDefinition) {
	gplog.Debug("Starting EnsurePartitionKeysInUniqueIndexDefs")

	// Get partition key information for all partitioned tables from database
	partitionColumnsByTable := GetPartitionColumnsByTable(connectionPool)

	for i := range *indexes {
		idx := &(*indexes)[i] // Work with a pointer to modify the original slice element

		// Step 1: Check if the index is unique
		if !strings.HasPrefix(strings.ToUpper(idx.Def.String), "CREATE UNIQUE INDEX") {
			continue
		}

		// Step 2: Get partition keys for the table this index belongs to
		tableFQN := fmt.Sprintf("%s.%s", idx.OwningSchema, idx.OwningTable)
		partitionKeys, tableHasPartitionKeys := partitionColumnsByTable[tableFQN]
		if !tableHasPartitionKeys || len(partitionKeys) == 0 {
			continue
		}

		// Step 3: Extract the current column/expression definition string from the index's Def
		currentColumnsDefsStr, err := extractIndexColumnDefinitionString(idx.Def.String)
		if err != nil {
			gplog.Warn("Failed to extract column definition string from index '%s' (OID %d): %v. Def: %s",
				idx.Name, idx.Oid, err, idx.Def.String)
			continue
		}

		// Step 4: Determine which partition keys are missing from the current index definition
		missingKeys := findMissingPartitionKeys(currentColumnsDefsStr, partitionKeys)

		// Step 5: If any partition keys are missing, rewrite the index definition
		if len(missingKeys) > 0 {
			newDefString, err := rewriteIndexDefToAddKeys(idx.Def.String, currentColumnsDefsStr, missingKeys)
			if err != nil {
				gplog.Warn("Failed to rewrite definition for index '%s': %v. Original def: %s",
					idx.Name, err, idx.Def.String)
				continue
			}

			if idx.Def.String != newDefString {
				gplog.Info("Index '%s' on partitioned table '%s' needs to be rewritten to include partition keys: %v. "+
					"Original definition: %s New definition: %s",
					idx.Name, tableFQN, missingKeys, idx.Def.String, newDefString)
				idx.Def.String = newDefString
				idx.Def.Valid = true
			}
		}
	}

	gplog.Debug("Finished EnsurePartitionKeysInUniqueIndexDefs")
}

// extractIndexColumnDefinitionString extracts the column definition part from a CREATE INDEX statement.
// It returns the content between the first pair of parentheses that follows "ON table_name [USING method]".
// For example, from: "CREATE UNIQUE INDEX idx ON table USING btree (col1, lower(col2)) WHERE..."
// it returns: "col1, lower(col2)"
func extractIndexColumnDefinitionString(defString string) (string, error) {
	// Regex to match "ON table_name [USING method] ("
	// This handles schema-qualified table names and optional USING clause
	re := regexp.MustCompile(`(?i)ON\s+[\w\."]+(?:\s+USING\s+\w+)?\s*\(`)

	matchLocations := re.FindStringIndex(defString)
	if matchLocations == nil {
		return "", fmt.Errorf("could not find column definition pattern 'ON table_name [USING method] (' in: %s", defString)
	}

	// Find the position of the opening parenthesis in the matched string
	openParenPos := -1
	for i := matchLocations[1] - 1; i >= matchLocations[0]; i-- {
		if defString[i] == '(' {
			openParenPos = i
			break
		}
	}

	if openParenPos == -1 {
		return "", fmt.Errorf("could not find opening parenthesis after ON clause in: %s", defString)
	}

	// Skip the opening parenthesis
	contentStartIndex := openParenPos + 1

	// Find the matching closing parenthesis, accounting for nested parentheses
	balance := 1
	endContentIndex := -1

	for i := contentStartIndex; i < len(defString); i++ {
		switch defString[i] {
		case '(':
			balance++
		case ')':
			balance--
			if balance == 0 {
				endContentIndex = i
				goto foundEnd // Exit loop once matching parenthesis is found
			}
		}
	}

foundEnd:
	if endContentIndex == -1 {
		return "", fmt.Errorf("could not find matching closing parenthesis for column definition in: %s", defString)
	}

	return strings.TrimSpace(defString[contentStartIndex:endContentIndex]), nil
}

// findMissingPartitionKeys compares the index column definition string with partition key columns
// and returns a list of partition key columns that are missing from the index definition
func findMissingPartitionKeys(currentColumnsDefsStr string, partitionKeyCols []string) []string {
	var missingKeys []string

	// Normalize the definition: remove extra whitespace, convert to lowercase for case-insensitive matching
	normalizedDef := strings.ToLower(regexp.MustCompile(`\s+`).ReplaceAllString(currentColumnsDefsStr, " "))

	for _, pkCol := range partitionKeyCols {
		// Check if the column appears as a standalone identifier with word boundaries
		// This handles cases like "col_name" or "col_name ASC"
		pattern := fmt.Sprintf(`\b%s\b`, regexp.QuoteMeta(strings.ToLower(pkCol)))

		// Also check if the column appears inside expressions like functions or calculations
		// This handles cases like "lower(col_name)" or "(col_name + 1)"
		patternInExpression := fmt.Sprintf(`\W%s\W`, regexp.QuoteMeta(strings.ToLower(pkCol)))

		// If the column isn't found with either pattern, consider it missing
		if !regexp.MustCompile(pattern).MatchString(normalizedDef) &&
			!regexp.MustCompile(patternInExpression).MatchString(normalizedDef) {
			missingKeys = append(missingKeys, pkCol)
		}
	}

	return missingKeys
}

// rewriteIndexDefToAddKeys takes the original index definition string,
// the extracted string of current columns/expressions, and a list of missing key strings.
// It returns a new index definition string with the missing keys appended.
func rewriteIndexDefToAddKeys(originalDef, currentColumnsDefsStr string, missingKeys []string) (string, error) {
	if len(missingKeys) == 0 {
		return originalDef, nil // No changes needed
	}

	// Use the same regex as extractIndexColumnDefinitionString for consistency
	re := regexp.MustCompile(`(?i)ON\s+[\w\."]+(?:\s+USING\s+\w+)?\s*\(`)

	matchLocations := re.FindStringIndex(originalDef)
	if matchLocations == nil {
		return "", fmt.Errorf("could not find column definition pattern 'ON table_name [USING method] (' in: %s", originalDef)
	}

	// Find the position of the opening parenthesis in the matched string
	openParenIndexOriginal := -1
	for i := matchLocations[1] - 1; i >= matchLocations[0]; i-- {
		if originalDef[i] == '(' {
			openParenIndexOriginal = i
			break
		}
	}

	if openParenIndexOriginal == -1 {
		return "", fmt.Errorf("could not find opening parenthesis after ON clause in: %s", originalDef)
	}

	// Find the matching closing parenthesis, accounting for nested parentheses
	balance := 1
	closeParenIndexOriginal := -1

	for i := openParenIndexOriginal + 1; i < len(originalDef); i++ {
		switch originalDef[i] {
		case '(':
			balance++
		case ')':
			balance--
			if balance == 0 {
				closeParenIndexOriginal = i
				goto foundEnd // Exit loop once matching parenthesis is found
			}
		}
	}

foundEnd:
	if closeParenIndexOriginal == -1 {
		return "", fmt.Errorf("could not find matching closing parenthesis for column definition in: %s", originalDef)
	}

	// Construct the new column definition string
	newColumnsDefStr := currentColumnsDefsStr
	if strings.TrimSpace(newColumnsDefStr) != "" && len(strings.Split(currentColumnsDefsStr, ",")) > 0 {
		newColumnsDefStr += ", "
	}
	newColumnsDefStr += strings.Join(missingKeys, ", ")

	// Assemble the new full definition
	prefix := originalDef[:openParenIndexOriginal+1] // Includes the opening '('
	suffix := originalDef[closeParenIndexOriginal:]  // Includes the closing ')' and rest of statement

	return prefix + newColumnsDefStr + suffix, nil
}

type ExchangedPartitionName struct {
	OrigName string
	NewName  string
}

type RuleDefinition struct {
	Oid          uint32
	Name         string
	OwningSchema string
	OwningTable  string
	// https://github.com/greenplum-db/gpbackup/commit/1d33a57abc23f2b7c0a6b0c5ae98622bb6d57fc7
	Def sql.NullString
}

func (r RuleDefinition) GetMetadataEntry() (string, toc.MetadataEntry) {
	tableFQN := utils.MakeFQN(r.OwningSchema, r.OwningTable)
	return "postdata",
		toc.MetadataEntry{
			Schema:          r.OwningSchema,
			Name:            r.Name,
			ObjectType:      "RULE",
			ReferenceObject: tableFQN,
			StartByte:       0,
			EndByte:         0,
			//Oid:             r.Oid,
			//ClassOID:        PG_REWRITE_OID,
		}
}

func (r RuleDefinition) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_REWRITE_OID, Oid: r.Oid}
}

func (r RuleDefinition) FQN() string {
	return r.Name
}

/*
 * Rules named "_RETURN", "pg_settings_n", and "pg_settings_u" are
 * built-in rules and we don't want to back them up. We use two `%` to
 * prevent Go from interpolating the % symbol.
 */
func GetRules(connectionPool *dbconn.DBConn) []RuleDefinition {
	query := fmt.Sprintf(`
	SELECT r.oid AS oid,
		quote_ident(r.rulename) AS name,
		quote_ident(n.nspname) AS owningschema,
		quote_ident(c.relname) AS owningtable,
		pg_get_ruledef(r.oid) AS def
	FROM pg_rewrite r
		JOIN pg_class c ON c.oid = r.ev_class
		JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE %s
		AND rulename NOT LIKE '%%RETURN'
		AND rulename NOT LIKE 'pg_%%'
		AND %s
	ORDER BY rulename`,
		relationAndSchemaFilterClause(connectionPool), ExtensionFilterClause("c"))

	gplog.Debug("GetRules, query is %v", query)
	results := make([]RuleDefinition, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	// Remove all rules that have NULL definitions. Not sure how
	// this can happen since pg_get_ruledef uses an SPI query but
	// handle the NULL just in case.
	verifiedResults := make([]RuleDefinition, 0)
	for _, result := range results {
		if result.Def.Valid {
			verifiedResults = append(verifiedResults, result)
		} else {
			gplog.Warn("Rule '%s' on table '%s.%s' not backed up, most likely dropped after gpbackup had begun.",
				result.Name, result.OwningSchema, result.OwningTable)
		}
	}

	return verifiedResults
}

type TriggerDefinition RuleDefinition

func (t TriggerDefinition) GetMetadataEntry() (string, toc.MetadataEntry) {
	tableFQN := utils.MakeFQN(t.OwningSchema, t.OwningTable)
	return "postdata",
		toc.MetadataEntry{
			Schema:          t.OwningSchema,
			Name:            t.Name,
			ObjectType:      "TRIGGER",
			ReferenceObject: tableFQN,
			StartByte:       0,
			EndByte:         0,
			//Oid:             t.Oid,
			//ClassOID:        PG_TRIGGER_OID,
		}
}

func (t TriggerDefinition) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TRIGGER_OID, Oid: t.Oid}
}

func (t TriggerDefinition) FQN() string {
	return t.Name
}

func GetTriggers(connectionPool *dbconn.DBConn) []TriggerDefinition {
	constraintClause := "NOT tgisinternal"
	if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("6") {
		constraintClause = "tgisconstraint = 'f'"
	}
	query := fmt.Sprintf(`
	SELECT t.oid AS oid,
		quote_ident(t.tgname) AS name,
		quote_ident(n.nspname) AS owningschema,
		quote_ident(c.relname) AS owningtable,
		pg_get_triggerdef(t.oid) AS def
	FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid
		JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE %s
		AND tgname NOT LIKE 'pg_%%'
		AND %s
		AND %s
	ORDER BY tgname`,
		relationAndSchemaFilterClause(connectionPool), constraintClause, ExtensionFilterClause("c"))

	gplog.Debug("GetTriggers, query is %v", query)
	results := make([]TriggerDefinition, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	// Remove all triggers that have NULL definitions. This can happen
	// if the query above is run and a concurrent trigger drop happens
	// just before the pg_get_triggerdef function executes.
	verifiedResults := make([]TriggerDefinition, 0)
	for _, result := range results {
		if result.Def.Valid {
			verifiedResults = append(verifiedResults, result)
		} else {
			gplog.Warn("Trigger '%s' on table '%s.%s' not backed up, most likely dropped after gpbackup had begun.",
				result.Name, result.OwningSchema, result.OwningTable)
		}
	}

	return verifiedResults
}

type EventTrigger struct {
	Oid          uint32
	Name         string
	Event        string
	FunctionName string
	Enabled      string
	EventTags    string
}

func (et EventTrigger) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "postdata",
		toc.MetadataEntry{
			Schema:          "",
			Name:            et.Name,
			ObjectType:      "EVENT TRIGGER",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
			//Oid:             et.Oid,
			//ClassOID:        PG_EVENT_TRIGGER,
		}
}

func (et EventTrigger) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_EVENT_TRIGGER, Oid: et.Oid}
}

func (et EventTrigger) FQN() string {
	return et.Name
}

func GetEventTriggers(connectionPool *dbconn.DBConn) []EventTrigger {
	query := fmt.Sprintf(`
	SELECT et.oid,
		quote_ident(et.evtname) AS name,
		et.evtevent AS event,
		array_to_string(array(select quote_literal(x) from unnest(evttags) as t(x)), ', ') AS eventtags,
		et.evtfoid::regproc AS functionname,
		et.evtenabled AS enabled
	FROM pg_event_trigger et
	WHERE %s
	ORDER BY name`, ExtensionFilterClause("et"))

	gplog.Debug("GetEventTriggers, query is %v", query)
	results := make([]EventTrigger, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	return results
}

type RLSPolicy struct {
	Oid        uint32
	Name       string
	Cmd        string
	Permissive string
	Schema     string
	Table      string
	Roles      string
	Qual       string
	WithCheck  string
}

func GetPolicies(connectionPool *dbconn.DBConn) []RLSPolicy {
	query := `
	SELECT
		p.oid as oid,
		quote_ident(p.polname) as name,
		p.polcmd as cmd,
		p.polpermissive as permissive,
		quote_ident(c.relnamespace::regnamespace::text) as schema,
		quote_ident(c.relname) as table,
		CASE
			WHEN polroles = '{0}' THEN ''
			ELSE coalesce(pg_catalog.array_to_string(ARRAY(SELECT pg_catalog.quote_ident(rolname) from pg_catalog.pg_roles WHERE oid = ANY(polroles)), ', '), '')
		END AS roles,
		coalesce(pg_catalog.pg_get_expr(polqual, polrelid), '') AS qual,
		coalesce(pg_catalog.pg_get_expr(polwithcheck, polrelid), '') AS withcheck
	FROM pg_catalog.pg_policy p
		JOIN pg_catalog.pg_class c ON p.polrelid = c.oid
	ORDER BY p.polname`

	gplog.Debug("GetPolicies, query is %v", query)
	results := make([]RLSPolicy, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	return results
}

func (p RLSPolicy) GetMetadataEntry() (string, toc.MetadataEntry) {
	tableFQN := utils.MakeFQN(p.Schema, p.Table)
	return "postdata",
		toc.MetadataEntry{
			Schema:          p.Schema,
			Name:            p.Table,
			ObjectType:      "POLICY",
			ReferenceObject: tableFQN,
			StartByte:       0,
			EndByte:         0,
		}
}

func (p RLSPolicy) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_REWRITE_OID, Oid: p.Oid}
}

func (p RLSPolicy) FQN() string {
	return p.Name
}
