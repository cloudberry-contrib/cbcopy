package copy

import (
	"strconv"
	"strings"

	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

/*
 * This file contains functions related to validating user input.
 */
func ValidateDbnames(dbnameMap map[string]string) {
	for k, v := range dbnameMap {
		if utils.Exists(excludedSourceDb, k) {
			gplog.Fatal(errors.Errorf("Cannot copy from \"%v\" database", k), "")
		}

		if utils.Exists(excludedDestDb, v) && !utils.MustGetFlagBool(options.GLOBAL_METADATA_ONLY) {
			gplog.Fatal(errors.Errorf("Cannot copy to \"%v\" database", v), "")
		}
	}
}

func validateCopyMode(flags *pflag.FlagSet) {
	if !utils.MustGetFlagBool(options.FULL) &&
		!utils.MustGetFlagBool(options.GLOBAL_METADATA_ONLY) &&
		len(utils.MustGetFlagStringSlice(options.SCHEMA)) == 0 &&
		len(utils.MustGetFlagStringSlice(options.DBNAME)) == 0 &&
		len(utils.MustGetFlagStringSlice(options.INCLUDE_TABLE)) == 0 &&
		len(utils.MustGetFlagString(options.INCLUDE_TABLE_FILE)) == 0 &&
		len(utils.MustGetFlagString(options.SCHEMA_MAPPING_FILE)) == 0 {
		gplog.Fatal(errors.Errorf("One and only one of the following flags must be specified: full, dbname, schema, include-table, include-table-file, global-metadata-only, schema-mapping-file"), "")
	}
}

func validateTableMode(flags *pflag.FlagSet) {
	if !utils.MustGetFlagBool(options.STATISTICS_ONLY) &&
		!utils.MustGetFlagBool(options.METADATA_ONLY) &&
		!utils.MustGetFlagBool(options.GLOBAL_METADATA_ONLY) &&
		!utils.MustGetFlagBool(options.TRUNCATE) &&
		!utils.MustGetFlagBool(options.APPEND) {
		gplog.Fatal(errors.Errorf("One and only one of the following flags must be specified: truncate, append"), "")
	}
}

func validateDatabase(flags *pflag.FlagSet) {
	srcNum := len(utils.MustGetFlagStringSlice(options.DBNAME))
	destNum := len(utils.MustGetFlagStringSlice(options.DEST_DBNAME))
	if destNum > 0 && (srcNum == 0 && len(utils.MustGetFlagString(options.INCLUDE_TABLE_FILE)) == 0) {
		gplog.Fatal(errors.Errorf("Option[s] \"--dest-dbname\" only supports with option \"--dbname or --include-table-file\""), "")
	}

	if destNum > 0 && srcNum > 0 && destNum != srcNum {
		gplog.Fatal(errors.Errorf("The number of databases specified by \"--dbname\" should be equal to that specified by \"--dest-dbname\""), "")
	}

	if utils.ArrayIsDuplicated(utils.MustGetFlagStringSlice(options.DBNAME)) {
		gplog.Fatal(errors.Errorf("Option \"dbname\" has duplicated items"), "")
	}
	if utils.ArrayIsDuplicated(utils.MustGetFlagStringSlice(options.DEST_DBNAME)) {
		gplog.Fatal(errors.Errorf("Option \"dest-dbname\" has duplicated items"), "")
	}

	if len(utils.MustGetFlagString(options.INCLUDE_TABLE_FILE)) > 0 && destNum > 1 {
		gplog.Fatal(errors.Errorf("Only support one database in \"dest-dbname\" option when include-table-file is enable"), "")
	}
}

func validateSchema(flags *pflag.FlagSet) {
	srcNum := len(utils.MustGetFlagStringSlice(options.SCHEMA))
	destNum := len(utils.MustGetFlagStringSlice(options.DEST_SCHEMA))
	if destNum > 0 && (srcNum == 0 && len(utils.MustGetFlagString(options.INCLUDE_TABLE_FILE)) == 0) {
		gplog.Fatal(errors.Errorf("Option[s] \"--dest-schema\" only supports with option \"--schema or --include-table-file\""), "")
	}

	if destNum > 0 && srcNum > 0 && destNum != srcNum {
		gplog.Fatal(errors.Errorf("The number of databases specified by \"--schema\" should be equal to that specified by \"--dest-schema\""), "")
	}

	if utils.ArrayIsDuplicated(utils.MustGetFlagStringSlice(options.SCHEMA)) {
		gplog.Fatal(errors.Errorf("Option \"schema\" has duplicated items"), "")
	}

	if utils.ArrayIsDuplicated(utils.MustGetFlagStringSlice(options.DEST_SCHEMA)) {
		gplog.Fatal(errors.Errorf("Option \"dest-schema\" has duplicated items"), "")
	}

	if len(utils.MustGetFlagString(options.INCLUDE_TABLE_FILE)) > 0 && destNum > 1 {
		gplog.Fatal(errors.Errorf("Only support one schema in \"dest-schema\" option when include-table-file is enable"), "")
	}
}

func validateTable(flags *pflag.FlagSet) {
	validateTableCombination(options.INCLUDE_TABLE, options.DEST_TABLE, utils.MustGetFlagStringSlice(options.INCLUDE_TABLE), utils.MustGetFlagStringSlice(options.DEST_TABLE))
	if len(utils.MustGetFlagString(options.DEST_TABLE_FILE)) > 0 {
		if len(utils.MustGetFlagString(options.INCLUDE_TABLE_FILE)) == 0 {
			gplog.Fatal(errors.Errorf("Option[s] \"--dest-table-file\" only supports with option \"--include-table-file\""), "")
		}

		srcTables, err := utils.ReadTableFile(utils.MustGetFlagString(options.INCLUDE_TABLE_FILE))
		if err != nil {
			gplog.Fatal(errors.Errorf("failed to read file \"%v\": %v ", utils.MustGetFlagString(options.INCLUDE_TABLE_FILE), err), "")
		}

		destTables, err := utils.ReadTableFile(utils.MustGetFlagString(options.DEST_TABLE_FILE))
		if err != nil {
			gplog.Fatal(errors.Errorf("failed to read file \"%v\": %v ", utils.MustGetFlagString(options.DEST_TABLE_FILE), err), "")
		}

		validateTableCombination(options.INCLUDE_TABLE_FILE, options.DEST_TABLE_FILE, srcTables, destTables)
	}
}

func validateDestHost(flags *pflag.FlagSet) {
	if !utils.MustGetFlagBool(options.STATISTICS_ONLY) &&
		len(utils.MustGetFlagString(options.DEST_HOST)) == 0 {
		gplog.Fatal(errors.Errorf("missing option \"--dest-host\""), "")
	}
}

func validateStatisticsFile(flags *pflag.FlagSet) {
	if len(utils.MustGetFlagString(options.STATISTICS_FILE)) > 0 {
		_, err := utils.ReadTableFile(utils.MustGetFlagString(options.STATISTICS_FILE))

		if err != nil {
			gplog.Fatal(errors.Errorf("failed to read file \"%v\": %v ", utils.MustGetFlagString(options.STATISTICS_FILE), err), "")
		}
	}
}

func validateFlagCombinations(flags *pflag.FlagSet) {
	options.CheckExclusiveFlags(flags, options.DEBUG, options.QUIET)
	options.CheckExclusiveFlags(flags, options.FULL, options.DBNAME, options.SCHEMA, options.INCLUDE_TABLE, options.INCLUDE_TABLE_FILE, options.GLOBAL_METADATA_ONLY, options.SCHEMA_MAPPING_FILE)
	options.CheckExclusiveFlags(flags, options.METADATA_ONLY, options.GLOBAL_METADATA_ONLY, options.STATISTICS_ONLY, options.TRUNCATE, options.APPEND)
	options.CheckExclusiveFlags(flags, options.COPY_JOBS, options.METADATA_ONLY, options.GLOBAL_METADATA_ONLY, options.STATISTICS_ONLY)
	options.CheckExclusiveFlags(flags, options.METADATA_ONLY, options.GLOBAL_METADATA_ONLY, options.STATISTICS_ONLY, options.DEST_TABLE, options.DEST_TABLE_FILE)
	options.CheckExclusiveFlags(flags, options.EXCLUDE_TABLE, options.EXCLUDE_TABLE_FILE)
	options.CheckExclusiveFlags(flags, options.DEST_TABLE, options.DEST_TABLE_FILE)
	options.CheckExclusiveFlags(flags, options.INCLUDE_TABLE, options.INCLUDE_TABLE_FILE)
	options.CheckExclusiveFlags(flags, options.FULL, options.WITH_GLOBALMETA)
	options.CheckExclusiveFlags(flags, options.METADATA_ONLY, options.GLOBAL_METADATA_ONLY, options.DATA_ONLY, options.STATISTICS_ONLY)
	options.CheckExclusiveFlags(flags, options.DATA_ONLY, options.STATISTICS_ONLY, options.WITH_GLOBALMETA, options.GLOBAL_METADATA_ONLY)
	options.CheckExclusiveFlags(flags, options.DATA_ONLY, options.STATISTICS_ONLY, options.METADATA_JOBS, options.GLOBAL_METADATA_ONLY)
	options.CheckExclusiveFlags(flags, options.DEST_TABLE, options.STATISTICS_ONLY, options.DEST_TABLE_FILE, options.METADATA_JOBS, options.GLOBAL_METADATA_ONLY)
	options.CheckExclusiveFlags(flags, options.DEST_TABLE, options.STATISTICS_ONLY, options.DEST_TABLE_FILE, options.WITH_GLOBALMETA, options.GLOBAL_METADATA_ONLY)
	options.CheckExclusiveFlags(flags, options.METADATA_ONLY, options.GLOBAL_METADATA_ONLY, options.STATISTICS_ONLY)
	options.CheckExclusiveFlags(flags, options.INCLUDE_TABLE_FILE, options.DEST_TABLE)
	options.CheckExclusiveFlags(flags, options.STATISTICS_ONLY, options.DEST_DBNAME, options.DEST_SCHEMA)
	options.CheckExclusiveFlags(flags, options.STATISTICS_ONLY, options.DEST_HOST)
	options.CheckExclusiveFlags(flags, options.STATISTICS_ONLY, options.DEST_PORT)
	options.CheckExclusiveFlags(flags, options.STATISTICS_ONLY, options.DEST_USER)
	options.CheckExclusiveFlags(flags, options.STATISTICS_JOBS, options.METADATA_ONLY, options.GLOBAL_METADATA_ONLY, options.DATA_ONLY)
	options.CheckExclusiveFlags(flags, options.STATISTICS_JOBS, options.TRUNCATE, options.APPEND)
	options.CheckExclusiveFlags(flags, options.METADATA_ONLY, options.GLOBAL_METADATA_ONLY, options.STATISTICS_ONLY, options.STATISTICS_FILE)
	options.CheckExclusiveFlags(flags, options.METADATA_ONLY, options.GLOBAL_METADATA_ONLY, options.STATISTICS_ONLY, options.ANALYZE)
	options.CheckExclusiveFlags(flags, options.GLOBAL_METADATA_ONLY, options.EXCLUDE_TABLE_FILE)
	options.CheckExclusiveFlags(flags, options.GLOBAL_METADATA_ONLY, options.ON_SEGMENT_THRESHOLD, options.STATISTICS_ONLY, options.METADATA_ONLY)
	options.CheckExclusiveFlags(flags, options.DEST_TABLE_FILE, options.DEST_DBNAME, options.DEST_SCHEMA, options.SCHEMA_MAPPING_FILE)
	options.CheckExclusiveFlags(flags, options.OWNER_MAPPING_FILE, options.DATA_ONLY)
	options.CheckExclusiveFlags(flags, options.TABLESPACE, options.DATA_ONLY, options.GLOBAL_METADATA_ONLY)
	options.CheckExclusiveFlags(flags, options.TABLESPACE, options.STATISTICS_ONLY)
	options.CheckExclusiveFlags(flags, options.TABLESPACE, options.DEST_TABLE, options.DEST_TABLE_FILE)

	validateCopyMode(flags)
	validateTableMode(flags)
	validateDatabase(flags)
	validateSchema(flags)
	validateTable(flags)
	validateDestHost(flags)
	validateStatisticsFile(flags)
	validateOwnerMappingFile(flags)
	validateDataPortRange(flags)
}

func validateTableCombination(optSrcName, optDestName string, srcTables, destTables []string) {
	srcNum := len(srcTables)
	destNum := len(destTables)
	if destNum > 0 && srcNum == 0 {
		gplog.Fatal(errors.Errorf("Option[s] \"--%v\" option only supports with option \"--%v\"", optDestName, optSrcName), "")
	}
	if destNum > 0 && srcNum > 0 && destNum != srcNum {
		gplog.Fatal(errors.Errorf("The number of table specified by \"--%v\" should be equal to that specified by \"--%v\"", optSrcName, optDestName), "")
	}
	if utils.ArrayIsDuplicated(destTables) {
		gplog.Fatal(errors.Errorf("Option \"%v\" has duplicated items", optDestName), "")
	}
	if utils.ArrayIsDuplicated(srcTables) {
		gplog.Fatal(errors.Errorf("Option \"%v\" has duplicated items", optSrcName), "")
	}
}

func validateOwnerMappingFile(flags *pflag.FlagSet) {
	if len(utils.MustGetFlagString(options.OWNER_MAPPING_FILE)) > 0 {
		schema := len(utils.MustGetFlagStringSlice(options.SCHEMA))
		inclTabFile := len(utils.MustGetFlagString(options.INCLUDE_TABLE_FILE))
		schemaMapfile := len(utils.MustGetFlagString(options.SCHEMA_MAPPING_FILE))
		if schema == 0 && inclTabFile == 0 && schemaMapfile == 0 {
			gplog.Fatal(errors.Errorf("Option[s] \"--owner-mapping-file\" only supports with option \"--schema or --schema-mapping-file or --include-table-file\""), "")
		}
	}
}

func validateDataPortRange(flags *pflag.FlagSet) {
	dataPortRange := utils.MustGetFlagString(options.DATA_PORT_RANGE)

	sl := strings.Split(dataPortRange, "-")
	if len(sl) != 2 {
		gplog.Fatal(errors.Errorf("invalid dash format"), "")
	}

	first, err := strconv.Atoi(sl[0])
	if err != nil {
		gplog.Fatal(errors.Errorf("invalid integer format, %v", first), "")
	}

	second, err := strconv.Atoi(sl[1])
	if err != nil {
		gplog.Fatal(errors.Errorf("invalid integer format, %v", second), "")
	}
}
