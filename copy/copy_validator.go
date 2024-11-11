package copy

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cloudberrydb/cbcopy/option"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/spf13/pflag"
)

// ValidationError defines a custom error type for validation failures
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}

// Validator interface defines the contract for all validators
type Validator interface {
	Validate() error
}

// BaseValidator provides common validation functionality
type BaseValidator struct {
	flags *pflag.FlagSet
}

// NewBaseValidator creates a new base validator instance
func NewBaseValidator(flags *pflag.FlagSet) *BaseValidator {
	return &BaseValidator{flags: flags}
}

// DatabaseValidator handles database-related validations
type DatabaseValidator struct {
	*BaseValidator
	excludedDb []string
}

// NewDatabaseValidator creates a new database validator with default excluded databases
func NewDatabaseValidator(flags *pflag.FlagSet) *DatabaseValidator {
	return &DatabaseValidator{
		BaseValidator: NewBaseValidator(flags),
		excludedDb:    excludedDb,
	}
}

// Validate implements the Validator interface
func (v *DatabaseValidator) Validate() error {
	return v.validateDatabase()
}

// validateDatabase validates database-related flags and their combinations
func (v *DatabaseValidator) validateDatabase() error {
	srcDbs := utils.MustGetFlagStringSlice(option.DBNAME)
	destDbs := utils.MustGetFlagStringSlice(option.DEST_DBNAME)
	srcNum := len(srcDbs)
	destNum := len(destDbs)

	// Validate destination database requirements
	if destNum > 0 && (srcNum == 0 && len(utils.MustGetFlagString(option.INCLUDE_TABLE_FILE)) == 0) {
		return &ValidationError{"Option[s] \"--dest-dbname\" only supports with option \"--dbname or --include-table-file\""}
	}

	// Validate source and destination database count match
	if destNum > 0 && srcNum > 0 && destNum != srcNum {
		return &ValidationError{"The number of databases specified by \"--dbname\" should be equal to that specified by \"--dest-dbname\""}
	}

	// Check for duplicates in source databases
	if utils.ArrayIsDuplicated(srcDbs) {
		return &ValidationError{"Option \"dbname\" has duplicated items"}
	}

	// Check for duplicates in destination databases
	if utils.ArrayIsDuplicated(destDbs) {
		return &ValidationError{"Option \"dest-dbname\" has duplicated items"}
	}

	// Validate table file constraints
	if len(utils.MustGetFlagString(option.INCLUDE_TABLE_FILE)) > 0 && destNum > 1 {
		return &ValidationError{"Only support one database in \"dest-dbname\" option when include-table-file is enable"}
	}

	// Validate excluded databases
	for _, db := range srcDbs {
		if utils.Exists(v.excludedDb, db) {
			return &ValidationError{fmt.Sprintf("Cannot copy from \"%v\" database", db)}
		}
	}

	for _, db := range destDbs {
		if utils.Exists(v.excludedDb, db) {
			return &ValidationError{fmt.Sprintf("Cannot copy to \"%v\" database", db)}
		}
	}

	return nil
}

// SchemaValidator handles schema-related validations
type SchemaValidator struct {
	*BaseValidator
}

// NewSchemaValidator creates a new schema validator instance
func NewSchemaValidator(flags *pflag.FlagSet) *SchemaValidator {
	return &SchemaValidator{NewBaseValidator(flags)}
}

// Validate implements the Validator interface
func (v *SchemaValidator) Validate() error {
	return v.validateSchema()
}

// validateSchema validates schema-related flags and their combinations
func (v *SchemaValidator) validateSchema() error {
	srcNum := len(utils.MustGetFlagStringSlice(option.SCHEMA))
	destNum := len(utils.MustGetFlagStringSlice(option.DEST_SCHEMA))

	// Validate schema requirements
	if destNum > 0 && (srcNum == 0 && len(utils.MustGetFlagString(option.INCLUDE_TABLE_FILE)) == 0) {
		return &ValidationError{"Option[s] \"--dest-schema\" only supports with option \"--schema or --include-table-file\""}
	}

	// Validate source and destination schema count match
	if destNum > 0 && srcNum > 0 && destNum != srcNum {
		return &ValidationError{"The number of schemas specified by \"--schema\" should be equal to that specified by \"--dest-schema\""}
	}

	// Check for duplicates
	if utils.ArrayIsDuplicated(utils.MustGetFlagStringSlice(option.SCHEMA)) {
		return &ValidationError{"Option \"schema\" has duplicated items"}
	}
	if utils.ArrayIsDuplicated(utils.MustGetFlagStringSlice(option.DEST_SCHEMA)) {
		return &ValidationError{"Option \"dest-schema\" has duplicated items"}
	}

	// Validate table file constraints
	if len(utils.MustGetFlagString(option.INCLUDE_TABLE_FILE)) > 0 && destNum > 1 {
		return &ValidationError{"Only support one schema in \"dest-schema\" option when include-table-file is enable"}
	}

	return nil
}

// TableValidator handles table-related validations
type TableValidator struct {
	*BaseValidator
}

// NewTableValidator creates a new table validator instance
func NewTableValidator(flags *pflag.FlagSet) *TableValidator {
	return &TableValidator{NewBaseValidator(flags)}
}

// Validate implements the Validator interface
func (v *TableValidator) Validate() error {
	return v.validateTable()
}

// validateTable validates table-related flags and their combinations
func (v *TableValidator) validateTable() error {
	// Validate table combinations for direct table specifications
	if err := v.validateTableCombination(option.INCLUDE_TABLE, option.DEST_TABLE,
		utils.MustGetFlagStringSlice(option.INCLUDE_TABLE),
		utils.MustGetFlagStringSlice(option.DEST_TABLE)); err != nil {
		return err
	}

	// Validate table file specifications
	if len(utils.MustGetFlagString(option.DEST_TABLE_FILE)) > 0 {
		if len(utils.MustGetFlagString(option.INCLUDE_TABLE_FILE)) == 0 {
			return &ValidationError{"Option[s] \"--dest-table-file\" only supports with option \"--include-table-file\""}
		}

		// Read and validate source tables file
		srcTables, err := utils.ReadTableFile(utils.MustGetFlagString(option.INCLUDE_TABLE_FILE))
		if err != nil {
			return &ValidationError{fmt.Sprintf("failed to read file \"%v\": %v",
				utils.MustGetFlagString(option.INCLUDE_TABLE_FILE), err)}
		}

		// Read and validate destination tables file
		destTables, err := utils.ReadTableFile(utils.MustGetFlagString(option.DEST_TABLE_FILE))
		if err != nil {
			return &ValidationError{fmt.Sprintf("failed to read file \"%v\": %v",
				utils.MustGetFlagString(option.DEST_TABLE_FILE), err)}
		}

		return v.validateTableCombination(option.INCLUDE_TABLE_FILE, option.DEST_TABLE_FILE,
			srcTables, destTables)
	}

	return nil
}

// validateTableCombination validates the combination of source and destination tables
func (v *TableValidator) validateTableCombination(optSrcName, optDestName string,
	srcTables, destTables []string) error {
	srcNum := len(srcTables)
	destNum := len(destTables)

	// Validate destination tables requirements
	if destNum > 0 && srcNum == 0 {
		return &ValidationError{fmt.Sprintf("Option[s] \"--%v\" option only supports with option \"--%v\"",
			optDestName, optSrcName)}
	}

	// Validate source and destination table count match
	if destNum > 0 && srcNum > 0 && destNum != srcNum {
		return &ValidationError{fmt.Sprintf("The number of table specified by \"--%v\" should be equal to that specified by \"--%v\"",
			optSrcName, optDestName)}
	}

	// Check for duplicates
	if utils.ArrayIsDuplicated(destTables) {
		return &ValidationError{fmt.Sprintf("Option \"%v\" has duplicated items", optDestName)}
	}
	if utils.ArrayIsDuplicated(srcTables) {
		return &ValidationError{fmt.Sprintf("Option \"%v\" has duplicated items", optSrcName)}
	}

	return nil
}

// ModeValidator handles mode-related validations
type ModeValidator struct {
	*BaseValidator
}

// NewModeValidator creates a new mode validator instance
func NewModeValidator(flags *pflag.FlagSet) *ModeValidator {
	return &ModeValidator{NewBaseValidator(flags)}
}

// Validate implements the Validator interface
func (v *ModeValidator) Validate() error {
	if err := v.validateCopyMode(); err != nil {
		return err
	}
	return v.validateTableMode()
}

// validateCopyMode validates the copy mode flags
func (v *ModeValidator) validateCopyMode() error {
	// Check if at least one copy mode is specified
	if !utils.MustGetFlagBool(option.FULL) &&
		!utils.MustGetFlagBool(option.GLOBAL_METADATA_ONLY) &&
		len(utils.MustGetFlagStringSlice(option.SCHEMA)) == 0 &&
		len(utils.MustGetFlagStringSlice(option.DBNAME)) == 0 &&
		len(utils.MustGetFlagStringSlice(option.INCLUDE_TABLE)) == 0 &&
		len(utils.MustGetFlagString(option.INCLUDE_TABLE_FILE)) == 0 &&
		len(utils.MustGetFlagString(option.SCHEMA_MAPPING_FILE)) == 0 {
		return &ValidationError{"One and only one of the following flags must be specified: full, dbname, schema, include-table, include-table-file, global-metadata-only, schema-mapping-file"}
	}
	return nil
}

// validateTableMode validates the table mode flags
func (v *ModeValidator) validateTableMode() error {
	if !utils.MustGetFlagBool(option.METADATA_ONLY) &&
		!utils.MustGetFlagBool(option.GLOBAL_METADATA_ONLY) &&
		!utils.MustGetFlagBool(option.TRUNCATE) &&
		!utils.MustGetFlagBool(option.APPEND) {
		return &ValidationError{"One and only one of the following flags must be specified: truncate, append"}
	}
	return nil
}

// OwnerMappingValidator handles owner mapping file validations
type OwnerMappingValidator struct {
	*BaseValidator
}

// NewOwnerMappingValidator creates a new owner mapping validator instance
func NewOwnerMappingValidator(flags *pflag.FlagSet) *OwnerMappingValidator {
	return &OwnerMappingValidator{NewBaseValidator(flags)}
}

// Validate implements the Validator interface
func (v *OwnerMappingValidator) Validate() error {
	return v.validateOwnerMappingFile()
}

// validateOwnerMappingFile validates owner mapping file and its related flags
func (v *OwnerMappingValidator) validateOwnerMappingFile() error {
	ownerMappingFile := utils.MustGetFlagString(option.OWNER_MAPPING_FILE)
	if len(ownerMappingFile) > 0 {
		schema := len(utils.MustGetFlagStringSlice(option.SCHEMA))
		inclTabFile := len(utils.MustGetFlagString(option.INCLUDE_TABLE_FILE))
		schemaMapfile := len(utils.MustGetFlagString(option.SCHEMA_MAPPING_FILE))

		// Owner mapping file can only be used with specific options
		if schema == 0 && inclTabFile == 0 && schemaMapfile == 0 {
			return &ValidationError{
				"Option[s] \"--owner-mapping-file\" only supports with option \"--schema or --include-table-file or --schema-mapping-file\"",
			}
		}
	}

	return nil
}

// PortValidator handles port-related validations
type PortValidator struct {
	*BaseValidator
}

// NewPortValidator creates a new port validator instance
func NewPortValidator(flags *pflag.FlagSet) *PortValidator {
	return &PortValidator{NewBaseValidator(flags)}
}

// Validate implements the Validator interface
func (v *PortValidator) Validate() error {
	return v.validateDataPortRange()
}

// validateDataPortRange validates the data port range format and values
func (v *PortValidator) validateDataPortRange() error {
	dataPortRange := utils.MustGetFlagString(option.DATA_PORT_RANGE)

	// Validate port range format
	sl := strings.Split(dataPortRange, "-")
	if len(sl) != 2 {
		return &ValidationError{"invalid dash format"}
	}

	// Validate first port number
	first, err := strconv.Atoi(sl[0])
	if err != nil {
		return &ValidationError{fmt.Sprintf("invalid integer format, %v", first)}
	}

	// Validate second port number
	second, err := strconv.Atoi(sl[1])
	if err != nil {
		return &ValidationError{fmt.Sprintf("invalid integer format, %v", second)}
	}

	return nil
}

// FlagCombinationValidator handles validation of flag combinations
type FlagCombinationValidator struct {
	*BaseValidator
	// Define exclusive flag groups
	exclusiveGroups [][]string
}

// NewFlagCombinationValidator creates a new flag combination validator
func NewFlagCombinationValidator(flags *pflag.FlagSet) *FlagCombinationValidator {
	return &FlagCombinationValidator{
		BaseValidator: NewBaseValidator(flags),
		exclusiveGroups: [][]string{
			{option.DEBUG, option.QUIET},
			{option.FULL, option.DBNAME, option.SCHEMA, option.INCLUDE_TABLE,
				option.INCLUDE_TABLE_FILE, option.GLOBAL_METADATA_ONLY, option.SCHEMA_MAPPING_FILE},
			{option.METADATA_ONLY, option.GLOBAL_METADATA_ONLY, option.TRUNCATE, option.APPEND},
			{option.COPY_JOBS, option.METADATA_ONLY, option.GLOBAL_METADATA_ONLY},
			{option.METADATA_ONLY, option.GLOBAL_METADATA_ONLY, option.DEST_TABLE, option.DEST_TABLE_FILE},
			{option.EXCLUDE_TABLE, option.EXCLUDE_TABLE_FILE},
			{option.DEST_TABLE, option.DEST_TABLE_FILE},
			{option.INCLUDE_TABLE, option.INCLUDE_TABLE_FILE},
			{option.FULL, option.WITH_GLOBALMETA},
			{option.METADATA_ONLY, option.GLOBAL_METADATA_ONLY, option.DATA_ONLY},
			{option.DATA_ONLY, option.WITH_GLOBALMETA, option.GLOBAL_METADATA_ONLY},
			{option.DATA_ONLY, option.METADATA_JOBS, option.GLOBAL_METADATA_ONLY},
			{option.DEST_TABLE, option.DEST_TABLE_FILE, option.METADATA_JOBS, option.GLOBAL_METADATA_ONLY},
			{option.DEST_TABLE, option.DEST_TABLE_FILE, option.WITH_GLOBALMETA, option.GLOBAL_METADATA_ONLY},
			{option.METADATA_ONLY, option.GLOBAL_METADATA_ONLY},
			{option.INCLUDE_TABLE_FILE, option.DEST_TABLE},
			{option.DEST_DBNAME, option.DEST_SCHEMA},
			{option.METADATA_ONLY, option.GLOBAL_METADATA_ONLY, option.DATA_ONLY},
			{option.TRUNCATE, option.APPEND},
			{option.METADATA_ONLY, option.GLOBAL_METADATA_ONLY},
			{option.GLOBAL_METADATA_ONLY, option.EXCLUDE_TABLE_FILE},
			{option.GLOBAL_METADATA_ONLY, option.ON_SEGMENT_THRESHOLD, option.METADATA_ONLY},
			{option.DEST_TABLE_FILE, option.DEST_DBNAME, option.DEST_SCHEMA, option.SCHEMA_MAPPING_FILE},
			{option.OWNER_MAPPING_FILE, option.DATA_ONLY},
			{option.TABLESPACE, option.DATA_ONLY, option.GLOBAL_METADATA_ONLY},
			{option.TABLESPACE, option.DEST_TABLE, option.DEST_TABLE_FILE},
		},
	}
}

// Validate implements the Validator interface
func (v *FlagCombinationValidator) Validate() error {
	// Validate each exclusive group
	for _, group := range v.exclusiveGroups {
		if err := v.validateExclusiveFlags(group); err != nil {
			return err
		}
	}
	return nil
}

// validateExclusiveFlags ensures only one flag from the group is set
func (v *FlagCombinationValidator) validateExclusiveFlags(flagNames []string) error {
	numSet := 0
	var setFlags []string

	// Count how many flags are set and collect their names
	for _, name := range flagNames {
		if v.flags.Changed(name) {
			numSet++
			setFlags = append(setFlags, name)
		}
	}

	// If more than one flag is set, return error
	if numSet > 1 {
		return &ValidationError{
			Message: fmt.Sprintf("The following flags may not be specified together: %s",
				strings.Join(setFlags, ", ")),
		}
	}

	return nil
}

// ValidatorManager manages all validators
type ValidatorManager struct {
	validators []Validator
}

// NewValidatorManager creates a new validator manager with all required validators
func NewValidatorManager(flags *pflag.FlagSet) *ValidatorManager {
	return &ValidatorManager{
		validators: []Validator{
			NewDatabaseValidator(flags),
			NewSchemaValidator(flags),
			NewTableValidator(flags),
			NewModeValidator(flags),
			NewPortValidator(flags),
			NewFlagCombinationValidator(flags),
			NewOwnerMappingValidator(flags),
		},
	}
}

// ValidateAll runs all validators
func (vm *ValidatorManager) ValidateAll() error {
	for _, v := range vm.validators {
		if err := v.Validate(); err != nil {
			return err
		}
	}
	return nil
}
