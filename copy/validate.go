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

		if utils.Exists(excludedDestDb, v) && !utils.MustGetFlagBool(option.GLOBAL_METADATA_ONLY) {
			gplog.Fatal(errors.Errorf("Cannot copy to \"%v\" database", v), "")
		}
	}
}

func validateCopyMode(flags *pflag.FlagSet) {
	if !utils.MustGetFlagBool(option.FULL) &&
		!utils.MustGetFlagBool(option.GLOBAL_METADATA_ONLY) &&
		len(utils.MustGetFlagStringSlice(option.SCHEMA)) == 0 &&
		len(utils.MustGetFlagStringSlice(option.DBNAME)) == 0 &&
		len(utils.MustGetFlagStringSlice(option.INCLUDE_TABLE)) == 0 &&
		len(utils.MustGetFlagString(option.INCLUDE_TABLE_FILE)) == 0 &&
		len(utils.MustGetFlagString(option.SCHEMA_MAPPING_FILE)) == 0 {
		gplog.Fatal(errors.Errorf("One and only one of the following flags must be specified: full, dbname, schema, include-table, include-table-file, global-metadata-only, schema-mapping-file"), "")
	}
}

func validateTableMode(flags *pflag.FlagSet) {
	if !utils.MustGetFlagBool(option.METADATA_ONLY) &&
		!utils.MustGetFlagBool(option.GLOBAL_METADATA_ONLY) &&
		!utils.MustGetFlagBool(option.TRUNCATE) &&
		!utils.MustGetFlagBool(option.APPEND) {
		gplog.Fatal(errors.Errorf("One and only one of the following flags must be specified: truncate, append"), "")
	}
}

func validateDatabase(flags *pflag.FlagSet) {
	srcNum := len(utils.MustGetFlagStringSlice(option.DBNAME))
	destNum := len(utils.MustGetFlagStringSlice(option.DEST_DBNAME))
	if destNum > 0 && (srcNum == 0 && len(utils.MustGetFlagString(option.INCLUDE_TABLE_FILE)) == 0) {
		gplog.Fatal(errors.Errorf("Option[s] \"--dest-dbname\" only supports with option \"--dbname or --include-table-file\""), "")
	}

	if destNum > 0 && srcNum > 0 && destNum != srcNum {
		gplog.Fatal(errors.Errorf("The number of databases specified by \"--dbname\" should be equal to that specified by \"--dest-dbname\""), "")
	}

	if utils.ArrayIsDuplicated(utils.MustGetFlagStringSlice(option.DBNAME)) {
		gplog.Fatal(errors.Errorf("Option \"dbname\" has duplicated items"), "")
	}
	if utils.ArrayIsDuplicated(utils.MustGetFlagStringSlice(option.DEST_DBNAME)) {
		gplog.Fatal(errors.Errorf("Option \"dest-dbname\" has duplicated items"), "")
	}

	if len(utils.MustGetFlagString(option.INCLUDE_TABLE_FILE)) > 0 && destNum > 1 {
		gplog.Fatal(errors.Errorf("Only support one database in \"dest-dbname\" option when include-table-file is enable"), "")
	}
}

func validateSchema(flags *pflag.FlagSet) {
	srcNum := len(utils.MustGetFlagStringSlice(option.SCHEMA))
	destNum := len(utils.MustGetFlagStringSlice(option.DEST_SCHEMA))
	if destNum > 0 && (srcNum == 0 && len(utils.MustGetFlagString(option.INCLUDE_TABLE_FILE)) == 0) {
		gplog.Fatal(errors.Errorf("Option[s] \"--dest-schema\" only supports with option \"--schema or --include-table-file\""), "")
	}

	if destNum > 0 && srcNum > 0 && destNum != srcNum {
		gplog.Fatal(errors.Errorf("The number of databases specified by \"--schema\" should be equal to that specified by \"--dest-schema\""), "")
	}

	if utils.ArrayIsDuplicated(utils.MustGetFlagStringSlice(option.SCHEMA)) {
		gplog.Fatal(errors.Errorf("Option \"schema\" has duplicated items"), "")
	}

	if utils.ArrayIsDuplicated(utils.MustGetFlagStringSlice(option.DEST_SCHEMA)) {
		gplog.Fatal(errors.Errorf("Option \"dest-schema\" has duplicated items"), "")
	}

	if len(utils.MustGetFlagString(option.INCLUDE_TABLE_FILE)) > 0 && destNum > 1 {
		gplog.Fatal(errors.Errorf("Only support one schema in \"dest-schema\" option when include-table-file is enable"), "")
	}
}

func validateTable(flags *pflag.FlagSet) {
	validateTableCombination(option.INCLUDE_TABLE, option.DEST_TABLE, utils.MustGetFlagStringSlice(option.INCLUDE_TABLE), utils.MustGetFlagStringSlice(option.DEST_TABLE))
	if len(utils.MustGetFlagString(option.DEST_TABLE_FILE)) > 0 {
		if len(utils.MustGetFlagString(option.INCLUDE_TABLE_FILE)) == 0 {
			gplog.Fatal(errors.Errorf("Option[s] \"--dest-table-file\" only supports with option \"--include-table-file\""), "")
		}

		srcTables, err := utils.ReadTableFile(utils.MustGetFlagString(option.INCLUDE_TABLE_FILE))
		if err != nil {
			gplog.Fatal(errors.Errorf("failed to read file \"%v\": %v ", utils.MustGetFlagString(option.INCLUDE_TABLE_FILE), err), "")
		}

		destTables, err := utils.ReadTableFile(utils.MustGetFlagString(option.DEST_TABLE_FILE))
		if err != nil {
			gplog.Fatal(errors.Errorf("failed to read file \"%v\": %v ", utils.MustGetFlagString(option.DEST_TABLE_FILE), err), "")
		}

		validateTableCombination(option.INCLUDE_TABLE_FILE, option.DEST_TABLE_FILE, srcTables, destTables)
	}
}

func validateDestHost(flags *pflag.FlagSet) {
	if len(utils.MustGetFlagString(option.DEST_HOST)) == 0 {
		gplog.Fatal(errors.Errorf("missing option \"--dest-host\""), "")
	}
}

func validateFlagCombinations(flags *pflag.FlagSet) {
	option.CheckExclusiveFlags(flags, option.DEBUG, option.QUIET)
	option.CheckExclusiveFlags(flags, option.FULL, option.DBNAME, option.SCHEMA, option.INCLUDE_TABLE, option.INCLUDE_TABLE_FILE, option.GLOBAL_METADATA_ONLY, option.SCHEMA_MAPPING_FILE)
	option.CheckExclusiveFlags(flags, option.METADATA_ONLY, option.GLOBAL_METADATA_ONLY, option.TRUNCATE, option.APPEND)
	option.CheckExclusiveFlags(flags, option.COPY_JOBS, option.METADATA_ONLY, option.GLOBAL_METADATA_ONLY)
	option.CheckExclusiveFlags(flags, option.METADATA_ONLY, option.GLOBAL_METADATA_ONLY, option.DEST_TABLE, option.DEST_TABLE_FILE)
	option.CheckExclusiveFlags(flags, option.EXCLUDE_TABLE, option.EXCLUDE_TABLE_FILE)
	option.CheckExclusiveFlags(flags, option.DEST_TABLE, option.DEST_TABLE_FILE)
	option.CheckExclusiveFlags(flags, option.INCLUDE_TABLE, option.INCLUDE_TABLE_FILE)
	option.CheckExclusiveFlags(flags, option.FULL, option.WITH_GLOBALMETA)
	option.CheckExclusiveFlags(flags, option.METADATA_ONLY, option.GLOBAL_METADATA_ONLY, option.DATA_ONLY)
	option.CheckExclusiveFlags(flags, option.DATA_ONLY, option.WITH_GLOBALMETA, option.GLOBAL_METADATA_ONLY)
	option.CheckExclusiveFlags(flags, option.DATA_ONLY, option.METADATA_JOBS, option.GLOBAL_METADATA_ONLY)
	option.CheckExclusiveFlags(flags, option.DEST_TABLE, option.DEST_TABLE_FILE, option.METADATA_JOBS, option.GLOBAL_METADATA_ONLY)
	option.CheckExclusiveFlags(flags, option.DEST_TABLE, option.DEST_TABLE_FILE, option.WITH_GLOBALMETA, option.GLOBAL_METADATA_ONLY)
	option.CheckExclusiveFlags(flags, option.METADATA_ONLY, option.GLOBAL_METADATA_ONLY)
	option.CheckExclusiveFlags(flags, option.INCLUDE_TABLE_FILE, option.DEST_TABLE)
	option.CheckExclusiveFlags(flags, option.DEST_DBNAME, option.DEST_SCHEMA)
	option.CheckExclusiveFlags(flags, option.METADATA_ONLY, option.GLOBAL_METADATA_ONLY, option.DATA_ONLY)
	option.CheckExclusiveFlags(flags, option.TRUNCATE, option.APPEND)
	option.CheckExclusiveFlags(flags, option.METADATA_ONLY, option.GLOBAL_METADATA_ONLY)
	option.CheckExclusiveFlags(flags, option.METADATA_ONLY, option.GLOBAL_METADATA_ONLY)
	option.CheckExclusiveFlags(flags, option.GLOBAL_METADATA_ONLY, option.EXCLUDE_TABLE_FILE)
	option.CheckExclusiveFlags(flags, option.GLOBAL_METADATA_ONLY, option.ON_SEGMENT_THRESHOLD, option.METADATA_ONLY)
	option.CheckExclusiveFlags(flags, option.DEST_TABLE_FILE, option.DEST_DBNAME, option.DEST_SCHEMA, option.SCHEMA_MAPPING_FILE)
	option.CheckExclusiveFlags(flags, option.OWNER_MAPPING_FILE, option.DATA_ONLY)
	option.CheckExclusiveFlags(flags, option.TABLESPACE, option.DATA_ONLY, option.GLOBAL_METADATA_ONLY)
	option.CheckExclusiveFlags(flags, option.TABLESPACE, option.DEST_TABLE, option.DEST_TABLE_FILE)

	validateCopyMode(flags)
	validateTableMode(flags)
	validateDatabase(flags)
	validateSchema(flags)
	validateTable(flags)
	validateDestHost(flags)
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
	if len(utils.MustGetFlagString(option.OWNER_MAPPING_FILE)) > 0 {
		schema := len(utils.MustGetFlagStringSlice(option.SCHEMA))
		inclTabFile := len(utils.MustGetFlagString(option.INCLUDE_TABLE_FILE))
		schemaMapfile := len(utils.MustGetFlagString(option.SCHEMA_MAPPING_FILE))
		if schema == 0 && inclTabFile == 0 && schemaMapfile == 0 {
			gplog.Fatal(errors.Errorf("Option[s] \"--owner-mapping-file\" only supports with option \"--schema or --schema-mapping-file or --include-table-file\""), "")
		}
	}
}

func validateDataPortRange(flags *pflag.FlagSet) {
	dataPortRange := utils.MustGetFlagString(option.DATA_PORT_RANGE)

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
