package toc

import (
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strings"

	"github.com/cloudberry-contrib/cbcopy/utils"
	"github.com/apache/cloudberry-go-libs/gplog"
	"gopkg.in/yaml.v2"
)

type TOC struct {
	metadataEntryMap    map[string]*[]MetadataEntry
	GlobalEntries       []MetadataEntry
	PredataEntries      []MetadataEntry
	PostdataEntries     []MetadataEntry
	StatisticsEntries   []MetadataEntry
	DataEntries         []MasterDataEntry
	IncrementalMetadata IncrementalEntries
}

type SegmentTOC struct {
	DataEntries map[uint]SegmentDataEntry
}

type MetadataEntry struct {
	Schema          string
	Name            string
	ObjectType      string
	ReferenceObject string
	StartByte       uint64
	EndByte         uint64
	// gpbackup does not have this now, (todo) which commit did that?
	//ClassOID        uint32
	//Oid             uint32
}

// todo, "GPDB 7 is renaming the GPDB "master" to the "coordinator"", https://github.com/greenplum-db/gpbackup/commit/c2a8a3fefe579b8ac574bd952b2e012f9c43b9f8
type MasterDataEntry struct {
	Schema          string
	Name            string
	Oid             uint32
	AttributeString string
	RowsCopied      int64
	PartitionRoot   string
	IsReplicated    bool // 	https://github.com/greenplum-db/gpbackup/commit/3bd62da18ecaa3c4c49dbddba1fcfcbbe41c6f51
}

type SegmentDataEntry struct {
	StartByte uint64
	EndByte   uint64
}

type IncrementalEntries struct {
	AO map[string]AOEntry
}

type AOEntry struct {
	Modcount         int64
	LastDDLTimestamp string
}

func NewTOC(filename string) *TOC {
	toc := &TOC{}
	contents, err := ioutil.ReadFile(filename)
	gplog.FatalOnError(err)
	err = yaml.Unmarshal(contents, toc)
	gplog.FatalOnError(err)
	return toc
}

func NewSegmentTOC(filename string) *SegmentTOC {
	toc := &SegmentTOC{}
	contents, err := ioutil.ReadFile(filename)
	gplog.FatalOnError(err)
	err = yaml.Unmarshal(contents, toc)
	gplog.FatalOnError(err)
	return toc
}

func (toc *TOC) WriteToFileAndMakeReadOnly(filename string) {
	contents, err := yaml.Marshal(toc)
	gplog.FatalOnError(err)
	err = utils.WriteToFileAndMakeReadOnly(filename, contents)
	gplog.FatalOnError(err)
}

func (toc *SegmentTOC) WriteToFileAndMakeReadOnly(filename string) error {
	contents, err := yaml.Marshal(toc)
	if err != nil {
		return err
	}
	return utils.WriteToFileAndMakeReadOnly(filename, contents)
}

type StatementWithType struct {
	Schema          string
	Name            string
	ObjectType      string
	ReferenceObject string
	Statement       string
	ClassOID        uint32
	Oid             uint32
}

func GetIncludedPartitionRoots(tocDataEntries []MasterDataEntry, includeRelations []string) []string {
	if len(includeRelations) == 0 {
		return []string{}
	}
	rootPartitions := make([]string, 0)

	FQNToPartitionRoot := make(map[string]string)
	for _, entry := range tocDataEntries {
		if entry.PartitionRoot != "" {
			FQNToPartitionRoot[utils.MakeFQN(entry.Schema, entry.Name)] = utils.MakeFQN(entry.Schema, entry.PartitionRoot)
		}
	}

	for _, relation := range includeRelations {
		if rootPartition, ok := FQNToPartitionRoot[relation]; ok {
			rootPartitions = append(rootPartitions, rootPartition)
		}
	}

	return rootPartitions
}

func (toc *TOC) GetSQLStatementForObjectTypes(section string, metadataFile io.ReaderAt, includeObjectTypes []string, excludeObjectTypes []string, includeSchemas []string, excludeSchemas []string, includeRelations []string, excludeRelations []string) []StatementWithType {
	entries := *toc.metadataEntryMap[section]

	objectSet, schemaSet, relationSet := constructFilterSets(includeObjectTypes, excludeObjectTypes, includeSchemas, excludeSchemas, includeRelations, excludeRelations)
	statements := make([]StatementWithType, 0)
	for _, entry := range entries {
		if shouldIncludeStatement(entry, objectSet, schemaSet, relationSet) {
			contents := make([]byte, entry.EndByte-entry.StartByte)
			_, err := metadataFile.ReadAt(contents, int64(entry.StartByte))
			gplog.FatalOnError(err)
			//statements = append(statements, StatementWithType{Schema: entry.Schema, Name: entry.Name, ObjectType: entry.ObjectType, ReferenceObject: entry.ReferenceObject, Statement: string(contents), Oid: entry.Oid, ClassOID: entry.ClassOID})
			statements = append(statements, StatementWithType{Schema: entry.Schema, Name: entry.Name, ObjectType: entry.ObjectType, ReferenceObject: entry.ReferenceObject, Statement: string(contents)})
		}
	}
	return statements
}

func constructFilterSets(includeObjectTypes []string, excludeObjectTypes []string, includeSchemas []string, excludeSchemas []string, includeRelations []string, excludeRelations []string) (*utils.FilterSet, *utils.FilterSet, *utils.FilterSet) {
	var objectSet, schemaSet, relationSet *utils.FilterSet
	if len(includeObjectTypes) > 0 {
		objectSet = utils.NewIncludeSet(includeObjectTypes)
	} else {
		objectSet = utils.NewExcludeSet(excludeObjectTypes)
	}
	if len(includeSchemas) > 0 {
		schemaSet = utils.NewIncludeSet(includeSchemas)
	} else {
		schemaSet = utils.NewExcludeSet(excludeSchemas)
	}
	if len(includeRelations) > 0 {
		relationSet = utils.NewIncludeSet(includeRelations)
	} else {
		relationSet = utils.NewExcludeSet(excludeRelations)
	}
	return objectSet, schemaSet, relationSet
}

func shouldIncludeStatement(entry MetadataEntry, objectSet *utils.FilterSet, schemaSet *utils.FilterSet, relationSet *utils.FilterSet) bool {
	shouldIncludeObject := objectSet.MatchesFilter(entry.ObjectType)
	shouldIncludeSchema := schemaSet.MatchesFilter(entry.Schema)
	relationFQN := utils.MakeFQN(entry.Schema, entry.Name)

	// https://github.com/greenplum-db/gpbackup/commit/526b859245dcba8ecc61ea6d305330048b0ddba2
	// todo: understand this logic,
	// In GPDB 7+, leaf partitions have the reference object set to their
	// upper-most root. If that root is in the exclude set, then we need
	// to prevent the leaf partition from being included.
	includeLeafPartition := true
	if relationSet.IsExclude && entry.ReferenceObject != "" && !relationSet.MatchesFilter(entry.ReferenceObject) {
		includeLeafPartition = false
	}

	shouldIncludeRelation := (relationSet.IsExclude && entry.ObjectType != "TABLE" && entry.ObjectType != "VIEW" && entry.ObjectType != "MATERIALIZED VIEW" && entry.ObjectType != "SEQUENCE" && entry.ReferenceObject == "") ||
		((entry.ObjectType == "TABLE" || entry.ObjectType == "VIEW" || entry.ObjectType == "MATERIALIZED VIEW" || entry.ObjectType == "SEQUENCE") && relationSet.MatchesFilter(relationFQN) && includeLeafPartition) || // Relations should match the filter
		(entry.ObjectType != "SEQUENCE OWNER" && entry.ReferenceObject != "" && relationSet.MatchesFilter(entry.ReferenceObject)) || // Include relations that filtered tables depend on
		(entry.ObjectType == "SEQUENCE OWNER" && relationSet.MatchesFilter(relationFQN) && relationSet.MatchesFilter(entry.ReferenceObject)) //Include sequence owners if both table and sequence are being restored

	return shouldIncludeObject && shouldIncludeSchema && shouldIncludeRelation
}

func getLeafPartitions(tableFQNs []string, tocDataEntries []MasterDataEntry) (leafPartitions []string) {
	tableSet := utils.NewSet(tableFQNs)

	for _, entry := range tocDataEntries {
		if entry.PartitionRoot == "" {
			continue
		}

		parentFQN := utils.MakeFQN(entry.Schema, entry.PartitionRoot)
		if tableSet.MatchesFilter(parentFQN) {
			leafPartitions = append(leafPartitions, utils.MakeFQN(entry.Schema, entry.Name))
		}
	}

	return leafPartitions
}

func (toc *TOC) GetDataEntriesMatching(includeSchemas []string, excludeSchemas []string,
	includeTableFQNs []string, excludeTableFQNs []string, restorePlanTableFQNs []string) []MasterDataEntry {

	schemaSet := utils.NewIncludeSet([]string{})
	if len(includeSchemas) > 0 {
		schemaSet = utils.NewIncludeSet(includeSchemas)
	} else if len(excludeSchemas) > 0 {
		schemaSet = utils.NewExcludeSet(excludeSchemas)
	}

	tableSet := utils.NewIncludeSet([]string{})
	if len(includeTableFQNs) > 0 {
		includeTableFQNs = append(includeTableFQNs, getLeafPartitions(includeTableFQNs, toc.DataEntries)...)
		tableSet = utils.NewIncludeSet(includeTableFQNs)
	} else if len(excludeTableFQNs) > 0 {
		excludeTableFQNs = append(excludeTableFQNs, getLeafPartitions(excludeTableFQNs, toc.DataEntries)...)
		tableSet = utils.NewExcludeSet(excludeTableFQNs)
	}

	restorePlanTableSet := utils.NewSet(restorePlanTableFQNs)

	matchingEntries := make([]MasterDataEntry, 0)
	for _, entry := range toc.DataEntries {
		tableFQN := utils.MakeFQN(entry.Schema, entry.Name)

		validSchema := schemaSet.MatchesFilter(entry.Schema)
		validRestorePlan := restorePlanTableSet.MatchesFilter(tableFQN)
		validTable := tableSet.MatchesFilter(tableFQN)
		if validRestorePlan && validSchema && validTable {
			matchingEntries = append(matchingEntries, entry)
		}
	}
	return matchingEntries
}

func SubstituteRedirectDatabaseInStatements(statements []StatementWithType, oldQuotedName string, newQuotedName string) []StatementWithType {
	shouldReplace := map[string]bool{"DATABASE GUC": true, "DATABASE": true, "DATABASE METADATA": true}
	pattern := regexp.MustCompile(fmt.Sprintf("DATABASE %s(;| OWNER| SET| TO| FROM| IS| TEMPLATE)", regexp.QuoteMeta(oldQuotedName)))
	for i := range statements {
		if shouldReplace[statements[i].ObjectType] {
			statements[i].Statement = pattern.ReplaceAllString(statements[i].Statement, fmt.Sprintf("DATABASE %s$1", newQuotedName))
		}
	}
	return statements
}

func RemoveActiveRole(activeUser string, statements []StatementWithType) []StatementWithType {
	newStatements := make([]StatementWithType, 0)
	for _, statement := range statements {
		if statement.ObjectType == "ROLE" && statement.Name == activeUser {
			continue
		}
		newStatements = append(newStatements, statement)
	}
	return newStatements
}

func (toc *TOC) InitializeMetadataEntryMap() {
	toc.metadataEntryMap = make(map[string]*[]MetadataEntry, 4)
	toc.metadataEntryMap["global"] = &toc.GlobalEntries
	toc.metadataEntryMap["predata"] = &toc.PredataEntries
	toc.metadataEntryMap["postdata"] = &toc.PostdataEntries
	toc.metadataEntryMap["statistics"] = &toc.StatisticsEntries
}

type TOCObject interface {
	GetMetadataEntry() (string, MetadataEntry)
}

type TOCObjectWithMetadata interface {
	GetMetadataEntry() (string, MetadataEntry)
	FQN() string
}

func (toc *TOC) AddMetadataEntry(section string, entry MetadataEntry, start, end uint64) {
	entry.StartByte = start
	entry.EndByte = end
	*toc.metadataEntryMap[section] = append(*toc.metadataEntryMap[section], entry)
}

func (toc *TOC) AddMasterDataEntry(schema string, name string, oid uint32, attributeString string, rowsCopied int64, PartitionRoot string, distPolicy string) {
	isReplicated := strings.Contains(distPolicy, "REPLICATED")
	toc.DataEntries = append(toc.DataEntries, MasterDataEntry{schema, name, oid, attributeString, rowsCopied, PartitionRoot, isReplicated})
}

func (toc *SegmentTOC) AddSegmentDataEntry(oid uint, startByte uint64, endByte uint64) {
	// We use uint for oid since the flags package does not have a uint32 flag
	toc.DataEntries[oid] = SegmentDataEntry{startByte, endByte}
}
