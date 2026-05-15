package builtin

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/apache/cloudberry-go-libs/operating"
	"github.com/cloudberry-contrib/cbcopy/option"
	"github.com/cloudberry-contrib/cbcopy/utils"
	"github.com/apache/cloudberry-go-libs/gplog"
)

// SkipReason categorizes why FilterTablesByDestExisting elected to bypass a
// given table. Recorded on each SkippedTable entry for downstream reporting.
type SkipReason int

const (
	// SkipReasonExists: the table itself (non-partition, or a partition root)
	// is already present on the destination.
	SkipReasonExists SkipReason = iota

	// SkipReasonRootExists: the table is a partition leaf or intermediate
	// whose root is present on the destination, so the entire partition tree
	// is skipped as a unit.
	SkipReasonRootExists

	// SkipReasonHalfBuiltLeaf: CB→CB declarative-partition path only. The
	// partition root is *absent* from the destination but this specific leaf
	// *is* present — a half-built state the user must resolve manually. We
	// emit a WARN and keep the leaf in the plan so the existing pipeline's
	// "already exists" swallow path handles the conflict during DDL execution.
	// (On the GP6 path the single inline CREATE will hard-error before this
	// state can be observed at filter time, so no warning is needed there.)
	SkipReasonHalfBuiltLeaf
)

// SkippedTable records one decision made by the --skip-existing filter.
// Both source-side and destination-side schema/name pairs are kept because
// they may differ under --schema-mapping. SourceDbName and DestDbName
// matter when cbcopy processes more than one db pair in a single run
// (--full or --dbname db1,db2 mode) so an audit of skip_existing.list can
// tell entries from different db pairs apart.
type SkippedTable struct {
	SourceDbName string
	SourceSchema string
	SourceName   string
	DestDbName   string
	DestSchema   string
	DestName     string
	Reason       SkipReason
}

var (
	skipExistingMu     sync.Mutex
	skipExistingTables []SkippedTable
)

// NumSkipExisting reports how many tables have been bypassed by
// --skip-existing so far. Used by the summary printer.
func NumSkipExisting() int {
	skipExistingMu.Lock()
	defer skipExistingMu.Unlock()
	return len(skipExistingTables)
}

// SkipExistingTables returns a copy of the recorded skip entries. Used by
// the list-file writer and the summary section.
func SkipExistingTables() []SkippedTable {
	skipExistingMu.Lock()
	defer skipExistingMu.Unlock()
	out := make([]SkippedTable, len(skipExistingTables))
	copy(out, skipExistingTables)
	return out
}

// ResetSkipExistingState clears the recorder. Test-only.
func ResetSkipExistingState() {
	skipExistingMu.Lock()
	defer skipExistingMu.Unlock()
	skipExistingTables = nil
}

func recordSkip(s SkippedTable) {
	skipExistingMu.Lock()
	defer skipExistingMu.Unlock()
	skipExistingTables = append(skipExistingTables, s)
}

// RecordPairSkip is the cross-package entry point used by the data-channel
// filter (filterTablePairsByDestExisting in copy/copy_metadata.go), which
// has the source and destination FQNs already paired up. Records the
// skip with reason SkipReasonExists; if the same destination FQN is also
// recorded by the DDL filter with a more specific reason (e.g.
// SkipReasonRootExists), dedupByDestFQN at write time will keep the more
// informative one.
func RecordPairSkip(srcDbName_ string, srcSchema, srcName string, destDbName_ string, destSchema, destName string) {
	recordSkip(SkippedTable{
		SourceDbName: srcDbName_,
		SourceSchema: srcSchema,
		SourceName:   srcName,
		DestDbName:   destDbName_,
		DestSchema:   destSchema,
		DestName:     destName,
		Reason:       SkipReasonExists,
	})
}

// dedupByDestFQN collapses entries that point to the same destination
// table (DestDbName + DestSchema + DestName). When the same destination
// is recorded by both the DDL filter (FilterTablesByDestExisting) and
// the data-pair filter (filterTablePairsByDestExisting) the entry with
// the more informative reason wins, scored
// HalfBuiltLeaf > RootExists > Exists. First-occurrence order is
// preserved so the file remains readable as a chronological audit.
func dedupByDestFQN(snapshot []SkippedTable) []SkippedTable {
	type key struct {
		dbName, schema, name string
	}
	score := func(r SkipReason) int {
		switch r {
		case SkipReasonHalfBuiltLeaf:
			return 3
		case SkipReasonRootExists:
			return 2
		case SkipReasonExists:
			return 1
		default:
			return 0
		}
	}

	indexByKey := make(map[key]int, len(snapshot))
	out := make([]SkippedTable, 0, len(snapshot))
	for _, s := range snapshot {
		k := key{s.DestDbName, s.DestSchema, s.DestName}
		if idx, ok := indexByKey[k]; ok {
			if score(s.Reason) > score(out[idx].Reason) {
				out[idx].Reason = s.Reason
			}
			continue
		}
		indexByKey[k] = len(out)
		out = append(out, s)
	}
	return out
}

// WriteSkipExistingList persists the full set of tables that were bypassed
// because of --skip-existing into ~/gpAdminLogs/<timestamp>_skip_existing.list.
// Layout: one tab-separated line per table —
//
//	<reason>	<src_db>.<src_schema>.<src_name>	<dest_db>.<dest_schema>.<dest_name>
//
// The db name is embedded into the FQN (matching cbcopy's existing
// <db>.<schema>.<table> convention used by cbcopy_skipped / cbcopy_failed)
// so that multi-database copies (--full or --dbname db1,db2) can be
// audited unambiguously. The file is written once, after all databases
// have been processed. Returns nil and writes nothing if no tables were
// skipped.
func WriteSkipExistingList(timestamp string) error {
	snapshot := SkipExistingTables()
	if len(snapshot) == 0 {
		return nil
	}
	snapshot = dedupByDestFQN(snapshot)

	homeDir, err := homeDirectory()
	if err != nil {
		return fmt.Errorf("resolve user home: %w", err)
	}
	logDir := filepath.Join(homeDir, "gpAdminLogs")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return fmt.Errorf("create %s: %w", logDir, err)
	}
	path := filepath.Join(logDir, fmt.Sprintf("%s_skip_existing.list", timestamp))

	var b strings.Builder
	for _, s := range snapshot {
		b.WriteString(s.Reason.String())
		b.WriteByte('\t')
		if s.SourceDbName != "" {
			b.WriteString(s.SourceDbName)
			b.WriteByte('.')
		}
		b.WriteString(s.SourceSchema)
		b.WriteByte('.')
		b.WriteString(s.SourceName)
		b.WriteByte('\t')
		if s.DestDbName != "" {
			b.WriteString(s.DestDbName)
			b.WriteByte('.')
		}
		b.WriteString(s.DestSchema)
		b.WriteByte('.')
		b.WriteString(s.DestName)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	gplog.Info("[skip-existing] wrote %d entries to %s", len(snapshot), path)
	return nil
}

// LogSkipExistingSummary writes a single Info-level line summarizing how
// many tables --skip-existing bypassed during this run. Intended to be
// called once near the end of cbcopy execution; complements (does not
// replace) the per-database summary line emitted by CopyManager.
func LogSkipExistingSummary() {
	snapshot := SkipExistingTables()
	if len(snapshot) == 0 {
		return
	}
	snapshot = dedupByDestFQN(snapshot)

	var (
		existsN     int
		rootExistsN int
		halfBuiltN  int
	)
	for _, s := range snapshot {
		switch s.Reason {
		case SkipReasonExists:
			existsN++
		case SkipReasonRootExists:
			rootExistsN++
		case SkipReasonHalfBuiltLeaf:
			halfBuiltN++
		}
	}
	gplog.Info("[skip-existing] bypassed %d table(s): %d already-exist, %d partition-tree (root present), %d half-built leaf warnings",
		len(snapshot), existsN, rootExistsN, halfBuiltN)
}

// String returns a short label suitable for the skip_existing.list file.
func (r SkipReason) String() string {
	switch r {
	case SkipReasonExists:
		return "exists"
	case SkipReasonRootExists:
		return "root_exists"
	case SkipReasonHalfBuiltLeaf:
		return "half_built_leaf"
	default:
		return "unknown"
	}
}

func homeDirectory() (string, error) {
	user, err := operating.System.CurrentUser()
	if err != nil {
		return "", err
	}
	return user.HomeDir, nil
}

// FilterTablesByDestExisting removes tables that already exist on the
// destination from the input slice and returns the survivors. Existence is
// queried on the *translated* destination-side FQN (so --schema-mapping is
// applied). For partition trees the root's existence is authoritative: if
// the root exists on the destination, the entire tree is skipped; if the
// root is absent, the children are kept — except for the CB→CB half-built
// case (see SkipReasonHalfBuiltLeaf).
//
// No-op unless --skip-existing is set and runtimeOption has been installed.
func FilterTablesByDestExisting(tables []Table) []Table {
	if !utils.MustGetFlagBool(option.SKIP_EXISTING) {
		return tables
	}
	if runtimeOption == nil {
		gplog.Warn("[skip-existing] runtimeOption is nil; skipping filter (this indicates a wiring bug)")
		return tables
	}

	// Index by src FQN so walkToRoot can navigate the inheritance chain.
	byFQN := make(map[string]Table, len(tables))
	for _, t := range tables {
		byFQN[t.Schema+"."+t.Name] = t
	}

	// Pass 1: decide which roots are bypassed. A "root" is either an explicit
	// partition root (level "p") or a non-partition regular table (level "").
	rootSkip := make(map[string]bool) // src root FQN -> true if root exists on dest
	for _, t := range tables {
		level := t.PartitionLevelInfo.Level
		if level != "p" && level != "" {
			continue
		}
		destSchema, destName := runtimeOption.TranslateToDestFQN(t.Schema, t.Name)
		if runtimeOption.IsDestTableExisting(destDbName, destSchema, destName) {
			rootSkip[t.Schema+"."+t.Name] = true
		}
	}

	// Pass 2: for each table, apply the root decision and handle the
	// per-table edge cases (external-partition leaf suffix, half-built leaves).
	kept := make([]Table, 0, len(tables))
	for _, t := range tables {
		srcFQN := t.Schema + "." + t.Name
		level := t.PartitionLevelInfo.Level

		switch level {
		case "p", "":
			if rootSkip[srcFQN] {
				ds, dn := runtimeOption.TranslateToDestFQN(t.Schema, t.Name)
				recordSkip(SkippedTable{
					SourceDbName: srcDbName, SourceSchema: t.Schema, SourceName: t.Name,
					DestDbName: destDbName, DestSchema: ds, DestName: dn,
					Reason: SkipReasonExists,
				})
				continue
			}
		case "l", "i":
			rootFQN := walkToRoot(t, byFQN)
			if rootSkip[rootFQN] {
				ds, dn := runtimeOption.TranslateToDestFQN(t.Schema, t.Name)
				recordSkip(SkippedTable{
					SourceDbName: srcDbName, SourceSchema: t.Schema, SourceName: t.Name,
					DestDbName: destDbName, DestSchema: ds, DestName: dn,
					Reason: SkipReasonRootExists,
				})
				continue
			}
			if level == "l" {
				// GP6 external-partition leaves are emitted on the destination
				// with an "_ext_part_" suffix (see AppendExtPartSuffix). When
				// the existence query targets the suffixed name and hits, the
				// leaf is treated like a regular skipped table (the root is
				// absent, but this leaf already has a destination presence).
				if t.IsExternal && srcDBVersion.IsGPDB() && srcDBVersion.Before("7") {
					suffixed := AppendExtPartSuffix(t.Name)
					ds, dn := runtimeOption.TranslateToDestFQN(t.Schema, suffixed)
					if runtimeOption.IsDestTableExisting(destDbName, ds, dn) {
						recordSkip(SkippedTable{
							SourceDbName: srcDbName, SourceSchema: t.Schema, SourceName: t.Name,
							DestDbName: destDbName, DestSchema: ds, DestName: dn,
							Reason: SkipReasonExists,
						})
						continue
					}
				}
				// CB→CB half-built check: root absent on dest, but the leaf
				// itself is present. Warn loudly so the user can decide; keep
				// the leaf in the plan because the GP6 single-CREATE path
				// hard-errors and the CB→CB attach path will swallow the
				// already-exists CREATE and attach the stale leaf onto a
				// freshly created root — a footgun, but matches gpcopy.
				if isCBDBFamilyPath() {
					ds, dn := runtimeOption.TranslateToDestFQN(t.Schema, t.Name)
					if runtimeOption.IsDestTableExisting(destDbName, ds, dn) {
						gplog.Warn("[skip-existing] partition leaf %s.%s exists on destination but its root does not; existing leaf will be attached to a freshly created root and may diverge from the source. Inspect the destination cluster before relying on the result.",
							ds, dn)
						recordSkip(SkippedTable{
							SourceDbName: srcDbName, SourceSchema: t.Schema, SourceName: t.Name,
							DestDbName: destDbName, DestSchema: ds, DestName: dn,
							Reason: SkipReasonHalfBuiltLeaf,
						})
						// Intentionally do not "continue" — the leaf stays in
						// the plan so the existing DDL pipeline observes the
						// conflict and handles it consistently with the
						// non-skip-existing flow.
					}
				}
			}
		}
		kept = append(kept, t)
	}

	if got := len(tables) - len(kept); got > 0 {
		gplog.Info("[skip-existing] %d table(s) bypassed because they already exist on destination database %q.",
			got, destDbName)
	}

	return kept
}

// walkToRoot follows Table.Inherits up the chain until it reaches a row
// whose PartitionLevelInfo.Level is "p" or "" (non-partition / root), and
// returns that row's "schema.name" FQN. For tables that aren't part of any
// partition tree this trivially returns the table's own FQN.
//
// Bounded by maxDepth so a pathological catalog can never lock the filter.
func walkToRoot(t Table, byFQN map[string]Table) string {
	const maxDepth = 16
	cur := t
	for i := 0; i < maxDepth; i++ {
		if cur.PartitionLevelInfo.Level == "p" || cur.PartitionLevelInfo.Level == "" {
			return cur.Schema + "." + cur.Name
		}
		if len(cur.Inherits) == 0 {
			return cur.Schema + "." + cur.Name
		}
		parent, ok := byFQN[cur.Inherits[0]]
		if !ok {
			// Parent was filtered out upstream (e.g., --exclude-table). Fall
			// back to the immediate-parent FQN as the root-equivalent.
			return cur.Inherits[0]
		}
		cur = parent
	}
	return cur.Schema + "." + cur.Name
}

// isCBDBFamilyPath reports whether the source-side path is the modern
// declarative partition path (GPDB 7+ / CBDB family), where the half-built
// leaf case can produce silently-divergent ATTACH behavior. On the GP6
// classical-partition path the inline root CREATE will hard-error before
// any half-built scenario can take hold, so the check doesn't apply.
func isCBDBFamilyPath() bool {
	return (srcDBVersion.IsGPDB() && srcDBVersion.AtLeast("7")) || srcDBVersion.IsCBDBFamily()
}
