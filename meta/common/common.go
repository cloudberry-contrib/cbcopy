package common

type MetaCommon struct {
	Timestamp     string
	WithGlobal    bool
	MetaOnly      bool
	PartNameMap   map[string][]string
	TableMap      map[string]string
	OwnerMap      map[string]string
	TablespaceMap map[string]string
}
