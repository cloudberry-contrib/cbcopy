package builtin

import (
	"sync"

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
// they may differ under --schema-mapping.
type SkippedTable struct {
	SourceSchema string
	SourceName   string
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
// filter (CopyModeTable + --dest-table flow in copy/copy_metadata.go), which
// has the source and destination FQNs already paired up and so doesn't need
// the partition/inheritance reasoning that FilterTablesByDestExisting
// performs. The reason is fixed to SkipReasonExists because the caller has
// already established that the destination row is present.
func RecordPairSkip(srcSchema, srcName, destSchema, destName string) {
	recordSkip(SkippedTable{
		SourceSchema: srcSchema,
		SourceName:   srcName,
		DestSchema:   destSchema,
		DestName:     destName,
		Reason:       SkipReasonExists,
	})
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
					SourceSchema: t.Schema, SourceName: t.Name,
					DestSchema: ds, DestName: dn,
					Reason: SkipReasonExists,
				})
				continue
			}
		case "l", "i":
			rootFQN := walkToRoot(t, byFQN)
			if rootSkip[rootFQN] {
				ds, dn := runtimeOption.TranslateToDestFQN(t.Schema, t.Name)
				recordSkip(SkippedTable{
					SourceSchema: t.Schema, SourceName: t.Name,
					DestSchema: ds, DestName: dn,
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
							SourceSchema: t.Schema, SourceName: t.Name,
							DestSchema: ds, DestName: dn,
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
							SourceSchema: t.Schema, SourceName: t.Name,
							DestSchema: ds, DestName: dn,
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
