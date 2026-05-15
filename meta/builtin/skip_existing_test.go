package builtin

import (
	"os"
	"os/user"
	"strings"
	"sync"
	"testing"

	"github.com/apache/cloudberry-go-libs/operating"
	"github.com/cloudberry-contrib/cbcopy/internal/dbconn"
	"github.com/cloudberry-contrib/cbcopy/internal/testhelper"
	"github.com/cloudberry-contrib/cbcopy/option"
	"github.com/cloudberry-contrib/cbcopy/utils"
	"github.com/spf13/pflag"
)

// testLoggerOnce ensures gplog has in-memory writers installed exactly once
// across all tests in this file, without contending with the Ginkgo
// BeforeEach in backup_suite_test.go that also touches the gplog singleton.
var testLoggerOnce sync.Once

func ensureTestLogger() {
	testLoggerOnce.Do(func() {
		testhelper.SetupTestLogger()
	})
}

// installSkipExistingTestFixtures wires up the package-level state that
// FilterTablesByDestExisting reads from. Callers pass the destination
// inventory + root-partition inventory as plain maps; the helper builds
// an *option.Option around them and installs srcDBVersion accordingly.
// The source db name is fixed to "src" — tests that don't care about
// the source db column don't need to look at it.
func installSkipExistingTestFixtures(t *testing.T, srcVer dbconn.GPDBVersion, destDb string, inv, parts map[string]struct{}) {
	t.Helper()
	ensureTestLogger()

	srcDBVersion = srcVer
	srcDbName = "src"
	destDbName = destDb

	o := &option.Option{}
	// Populate using the public MarkDestTables path so we exercise the same
	// code that production does. Convert the test fixture maps to the shapes
	// MarkDestTables expects.
	userTables := make(map[string]option.TableStatistics, len(inv))
	for k := range inv {
		userTables[k] = option.TableStatistics{}
	}
	partTables := make(map[string]bool, len(parts))
	for k := range parts {
		partTables[k] = true
	}
	// MarkDestTables requires destDbInventory/destDbRootParts to be initialized.
	// option.NewOption does this for real callers; the test sets it directly.
	setInventories(o, destDb, userTables, partTables)
	runtimeOption = o

	// Install a pflag.FlagSet that has SKIP_EXISTING registered and set to
	// true (so MustGetFlagBool returns true).
	fs := pflag.NewFlagSet("skip-existing-test", pflag.ContinueOnError)
	fs.Bool(option.SKIP_EXISTING, true, "")
	utils.SetCmdFlags(fs)

	ResetSkipExistingState()
}

// setInventories bypasses MarkDestTables (which requires the maps to already
// exist) so tests can directly seed an Option fixture.
func setInventories(o *option.Option, destDb string, userTables map[string]option.TableStatistics, partTables map[string]bool) {
	// Use the exported MarkDestTables after seeding the maps via NewOption
	// would normally be the path, but for tests we shortcut by ensuring the
	// maps are non-nil first. Since fields are unexported, do this via
	// option.NewOption indirectly: build an Option that has empty maps, then
	// call MarkDestTables.
	o2 := newOptionWithEmptyInventories()
	*o = *o2
	o.MarkDestTables(destDb, userTables, partTables)
}

// newOptionWithEmptyInventories returns a fresh *Option whose inventory maps
// are initialized but empty. Constructed via NewOption with a minimal flag
// set to avoid touching real config paths.
func newOptionWithEmptyInventories() *option.Option {
	fs := pflag.NewFlagSet("init", pflag.ContinueOnError)
	// NewOption reads several flags; register the ones it touches.
	fs.StringSlice(option.DBNAME, []string{}, "")
	fs.StringSlice(option.DEST_DBNAME, []string{}, "")
	fs.StringSlice(option.EXCLUDE_TABLE, []string{}, "")
	fs.String(option.EXCLUDE_TABLE_FILE, "", "")
	fs.StringSlice(option.INCLUDE_TABLE, []string{}, "")
	fs.String(option.INCLUDE_TABLE_FILE, "", "")
	fs.StringSlice(option.DEST_TABLE, []string{}, "")
	fs.String(option.DEST_TABLE_FILE, "", "")
	fs.Bool(option.APPEND, false, "")
	fs.Bool(option.SKIP_EXISTING, false, "")
	fs.StringSlice(option.SCHEMA, []string{}, "")
	fs.StringSlice(option.DEST_SCHEMA, []string{}, "")
	fs.String(option.SCHEMA_MAPPING_FILE, "", "")
	fs.String(option.OWNER_MAPPING_FILE, "", "")
	fs.String(option.DEST_TABLESPACE, "", "")
	fs.String(option.TABLESPACE_MAPPING_FILE, "", "")
	fs.String(option.CONNECTION_MODE, "push", "")
	o, err := option.NewOption(fs)
	if err != nil {
		panic(err)
	}
	return o
}

func mkTable(schema, name, level string, inherits []string, isExternal bool) Table {
	return Table{
		Relation: Relation{Schema: schema, Name: name},
		TableDefinition: TableDefinition{
			PartitionLevelInfo: PartitionLevelInfo{Level: level},
			Inherits:           inherits,
			IsExternal:         isExternal,
		},
	}
}

func TestFilter_FlagOff_NoOp(t *testing.T) {
	ensureTestLogger()
	srcDBVersion = dbconn.NewVersion("7.0.0")
	destDbName = "dst"
	runtimeOption = newOptionWithEmptyInventories()
	fs := pflag.NewFlagSet("off", pflag.ContinueOnError)
	fs.Bool(option.SKIP_EXISTING, false, "")
	utils.SetCmdFlags(fs)
	ResetSkipExistingState()

	tables := []Table{mkTable("public", "t1", "", nil, false)}
	got := FilterTablesByDestExisting(tables)
	if len(got) != 1 || got[0].Name != "t1" {
		t.Fatalf("expected input passthrough when --skip-existing is off, got %+v", got)
	}
	if NumSkipExisting() != 0 {
		t.Fatalf("expected 0 recorded skips, got %d", NumSkipExisting())
	}
}

func TestFilter_RegularTableExists_Skipped(t *testing.T) {
	cbdb := dbconn.GPDBVersion{Type: dbconn.CBDB}
	installSkipExistingTestFixtures(t, cbdb, "dst",
		map[string]struct{}{"public.t1": {}},
		map[string]struct{}{},
	)

	tables := []Table{
		mkTable("public", "t1", "", nil, false),
		mkTable("public", "t2", "", nil, false),
	}
	got := FilterTablesByDestExisting(tables)
	if len(got) != 1 || got[0].Name != "t2" {
		t.Fatalf("expected only t2 kept, got %+v", got)
	}
	if NumSkipExisting() != 1 {
		t.Fatalf("expected 1 recorded skip, got %d", NumSkipExisting())
	}
	if SkipExistingTables()[0].Reason != SkipReasonExists {
		t.Fatalf("expected SkipReasonExists, got %v", SkipExistingTables()[0].Reason)
	}
}

func TestFilter_PartitionRootExists_TreeSkipped(t *testing.T) {
	cbdb := dbconn.GPDBVersion{Type: dbconn.CBDB}
	installSkipExistingTestFixtures(t, cbdb, "dst",
		map[string]struct{}{},
		map[string]struct{}{"public.root1": {}},
	)

	tables := []Table{
		mkTable("public", "root1", "p", nil, false),
		mkTable("public", "root1_1_prt_a", "l", []string{"public.root1"}, false),
		mkTable("public", "root1_1_prt_b", "l", []string{"public.root1"}, false),
		mkTable("public", "t_other", "", nil, false),
	}
	got := FilterTablesByDestExisting(tables)
	if len(got) != 1 || got[0].Name != "t_other" {
		t.Fatalf("expected only t_other kept, got %+v", got)
	}
	if NumSkipExisting() != 3 {
		t.Fatalf("expected 3 recorded skips (root + 2 leaves), got %d", NumSkipExisting())
	}
}

func TestFilter_PartitionRootMissing_AllKept(t *testing.T) {
	cbdb := dbconn.GPDBVersion{Type: dbconn.CBDB}
	installSkipExistingTestFixtures(t, cbdb, "dst",
		map[string]struct{}{},
		map[string]struct{}{},
	)

	tables := []Table{
		mkTable("public", "root1", "p", nil, false),
		mkTable("public", "root1_1_prt_a", "l", []string{"public.root1"}, false),
	}
	got := FilterTablesByDestExisting(tables)
	if len(got) != 2 {
		t.Fatalf("expected all 2 kept, got %d", len(got))
	}
	if NumSkipExisting() != 0 {
		t.Fatalf("expected 0 recorded skips, got %d", NumSkipExisting())
	}
}

func TestFilter_CBHalfBuiltLeaf_WarnAndKeep(t *testing.T) {
	// CB-family source, root absent on dest, but leaf is present.
	cbdb := dbconn.GPDBVersion{Type: dbconn.CBDB}
	installSkipExistingTestFixtures(t, cbdb, "dst",
		map[string]struct{}{"public.root1_1_prt_a": {}}, // leaf exists
		map[string]struct{}{},                            // root does NOT
	)

	tables := []Table{
		mkTable("public", "root1", "p", nil, false),
		mkTable("public", "root1_1_prt_a", "l", []string{"public.root1"}, false),
	}
	got := FilterTablesByDestExisting(tables)
	// Both should be kept: the existing pipeline handles the conflict at DDL
	// execution time. Filter records a half-built skip for reporting.
	if len(got) != 2 {
		t.Fatalf("expected 2 kept (half-built case keeps leaf in plan), got %d", len(got))
	}
	if NumSkipExisting() != 1 {
		t.Fatalf("expected 1 recorded half-built skip, got %d", NumSkipExisting())
	}
	if SkipExistingTables()[0].Reason != SkipReasonHalfBuiltLeaf {
		t.Fatalf("expected SkipReasonHalfBuiltLeaf, got %v", SkipExistingTables()[0].Reason)
	}
}

func TestFilter_GP6ExternalLeafSuffix_SkippedBySuffixedName(t *testing.T) {
	// GP6 source: external partition leaf is emitted with "_ext_part_" suffix
	// on the destination. Existence check must use the suffixed name.
	gp6 := dbconn.NewVersion("6.20.0") // Type = GPDB by NewVersion
	installSkipExistingTestFixtures(t, gp6, "dst",
		map[string]struct{}{"public.ext_leaf_ext_part_": {}},
		map[string]struct{}{},
	)

	tables := []Table{
		mkTable("public", "root1", "p", nil, false),
		mkTable("public", "ext_leaf", "l", []string{"public.root1"}, true),
	}
	got := FilterTablesByDestExisting(tables)
	// Root is absent on dest, so root + non-external leaves would be kept;
	// the external leaf should be filtered out because its suffixed name
	// exists on the destination.
	if len(got) != 1 || got[0].Name != "root1" {
		t.Fatalf("expected only root1 kept (external leaf filtered by suffix), got %+v", got)
	}
	if NumSkipExisting() != 1 {
		t.Fatalf("expected 1 recorded skip, got %d", NumSkipExisting())
	}
}

func TestWalkToRoot_NonPartitionReturnsSelf(t *testing.T) {
	byFQN := map[string]Table{}
	t1 := mkTable("public", "t1", "", nil, false)
	if got := walkToRoot(t1, byFQN); got != "public.t1" {
		t.Fatalf("expected public.t1, got %s", got)
	}
}

func TestWalkToRoot_PartitionRootReturnsSelf(t *testing.T) {
	byFQN := map[string]Table{}
	r := mkTable("public", "root1", "p", nil, false)
	if got := walkToRoot(r, byFQN); got != "public.root1" {
		t.Fatalf("expected public.root1, got %s", got)
	}
}

func TestWalkToRoot_LeafFollowsInherits(t *testing.T) {
	root := mkTable("public", "root1", "p", nil, false)
	leaf := mkTable("public", "root1_1_prt_a", "l", []string{"public.root1"}, false)
	byFQN := map[string]Table{
		"public.root1":           root,
		"public.root1_1_prt_a":   leaf,
	}
	if got := walkToRoot(leaf, byFQN); got != "public.root1" {
		t.Fatalf("expected public.root1, got %s", got)
	}
}

func TestWalkToRoot_LeafThroughIntermediate(t *testing.T) {
	root := mkTable("public", "root1", "p", nil, false)
	mid := mkTable("public", "root1_1_prt_q1", "i", []string{"public.root1"}, false)
	leaf := mkTable("public", "root1_1_prt_q1_2_prt_jan", "l", []string{"public.root1_1_prt_q1"}, false)
	byFQN := map[string]Table{
		"public.root1":                          root,
		"public.root1_1_prt_q1":                 mid,
		"public.root1_1_prt_q1_2_prt_jan":       leaf,
	}
	if got := walkToRoot(leaf, byFQN); got != "public.root1" {
		t.Fatalf("expected public.root1 (through intermediate), got %s", got)
	}
}

func TestWalkToRoot_ParentMissingFallsBackToImmediate(t *testing.T) {
	leaf := mkTable("public", "orphan_leaf", "l", []string{"public.gone"}, false)
	byFQN := map[string]Table{} // parent not present
	if got := walkToRoot(leaf, byFQN); got != "public.gone" {
		t.Fatalf("expected fallback to immediate parent FQN, got %s", got)
	}
}

func TestSkipReasonString(t *testing.T) {
	cases := []struct {
		r    SkipReason
		want string
	}{
		{SkipReasonExists, "exists"},
		{SkipReasonRootExists, "root_exists"},
		{SkipReasonHalfBuiltLeaf, "half_built_leaf"},
		{SkipReason(99), "unknown"},
	}
	for _, tc := range cases {
		if got := tc.r.String(); got != tc.want {
			t.Errorf("SkipReason(%d).String() = %q, want %q", tc.r, got, tc.want)
		}
	}
}

func TestDedupByDestFQN(t *testing.T) {
	// Same destination FQN recorded twice (once with weaker reason, once
	// with stronger) collapses to a single entry whose reason is the
	// stronger one. Entry order is the first-occurrence order so the
	// audit file reads chronologically.
	in := []SkippedTable{
		{SourceDbName: "s", SourceSchema: "public", SourceName: "p_1_prt_a",
			DestDbName: "d", DestSchema: "public", DestName: "p_1_prt_a",
			Reason: SkipReasonExists},
		{SourceDbName: "s", SourceSchema: "public", SourceName: "regular",
			DestDbName: "d", DestSchema: "public", DestName: "regular",
			Reason: SkipReasonExists},
		{SourceDbName: "s", SourceSchema: "public", SourceName: "p_1_prt_a",
			DestDbName: "d", DestSchema: "public", DestName: "p_1_prt_a",
			Reason: SkipReasonRootExists}, // upgrade for p_1_prt_a
		{SourceDbName: "s", SourceSchema: "public", SourceName: "halfbuilt",
			DestDbName: "d", DestSchema: "public", DestName: "halfbuilt",
			Reason: SkipReasonExists},
		{SourceDbName: "s", SourceSchema: "public", SourceName: "halfbuilt",
			DestDbName: "d", DestSchema: "public", DestName: "halfbuilt",
			Reason: SkipReasonHalfBuiltLeaf}, // upgrade for halfbuilt
	}

	out := dedupByDestFQN(in)

	if len(out) != 3 {
		t.Fatalf("len(out) = %d, want 3 (one per distinct dest FQN). out=%+v", len(out), out)
	}

	// Order: p_1_prt_a (first seen), regular, halfbuilt.
	expectedOrder := []struct {
		name   string
		reason SkipReason
	}{
		{"p_1_prt_a", SkipReasonRootExists},
		{"regular", SkipReasonExists},
		{"halfbuilt", SkipReasonHalfBuiltLeaf},
	}
	for i, want := range expectedOrder {
		if out[i].DestName != want.name {
			t.Errorf("out[%d].DestName = %q, want %q", i, out[i].DestName, want.name)
		}
		if out[i].Reason != want.reason {
			t.Errorf("out[%d].Reason = %v, want %v", i, out[i].Reason, want.reason)
		}
	}
}

// Weaker reason coming AFTER a stronger one must not downgrade.
func TestDedupByDestFQN_DoesNotDowngrade(t *testing.T) {
	in := []SkippedTable{
		{DestDbName: "d", DestSchema: "public", DestName: "t",
			Reason: SkipReasonRootExists},
		{DestDbName: "d", DestSchema: "public", DestName: "t",
			Reason: SkipReasonExists},
	}
	out := dedupByDestFQN(in)
	if len(out) != 1 {
		t.Fatalf("len(out) = %d, want 1", len(out))
	}
	if out[0].Reason != SkipReasonRootExists {
		t.Errorf("Reason = %v, want SkipReasonRootExists (must not be downgraded by a later weaker record)", out[0].Reason)
	}
}

func TestWriteSkipExistingList_EmptyIsNoOp(t *testing.T) {
	ensureTestLogger()
	ResetSkipExistingState()
	tmpHome := t.TempDir()
	t.Setenv("HOME", tmpHome)

	if err := WriteSkipExistingList("ts"); err != nil {
		t.Fatalf("expected no error on empty input, got %v", err)
	}
	// Should not create the file at all.
	path := tmpHome + "/gpAdminLogs/ts_skip_existing.list"
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected no file to be created, got err=%v", err)
	}
}

func TestWriteSkipExistingList_PersistsAllEntries(t *testing.T) {
	ensureTestLogger()
	ResetSkipExistingState()
	recordSkip(SkippedTable{
		SourceDbName: "prod", SourceSchema: "src_s", SourceName: "t1",
		DestDbName: "prod_new", DestSchema: "dst_s", DestName: "t1",
		Reason: SkipReasonExists,
	})
	recordSkip(SkippedTable{
		SourceDbName: "staging", SourceSchema: "public", SourceName: "rootA",
		DestDbName: "staging_new", DestSchema: "public", DestName: "rootA",
		Reason: SkipReasonRootExists,
	})
	recordSkip(SkippedTable{
		SourceDbName: "prod", SourceSchema: "public", SourceName: "rootA_1_prt_a",
		DestDbName: "prod_new", DestSchema: "public", DestName: "rootA_1_prt_a",
		Reason: SkipReasonHalfBuiltLeaf,
	})

	tmpHome := t.TempDir()
	t.Setenv("HOME", tmpHome)
	// operating.System is a *SystemFunctions, so we must swap the field
	// directly (not the whole pointer) and restore the same field on exit;
	// otherwise the override leaks into other tests in the binary and breaks
	// dbconn.NewDBConnFromEnvironment in the Ginkgo BeforeEach.
	prevCurrentUser := operating.System.CurrentUser
	operating.System.CurrentUser = func() (*user.User, error) {
		return &user.User{HomeDir: tmpHome}, nil
	}
	defer func() { operating.System.CurrentUser = prevCurrentUser }()

	if err := WriteSkipExistingList("20260511_1830"); err != nil {
		t.Fatalf("WriteSkipExistingList: %v", err)
	}
	path := tmpHome + "/gpAdminLogs/20260511_1830_skip_existing.list"
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read produced file: %v", err)
	}
	body := string(raw)
	wants := []string{
		"exists\tprod.src_s.t1\tprod_new.dst_s.t1",
		"root_exists\tstaging.public.rootA\tstaging_new.public.rootA",
		"half_built_leaf\tprod.public.rootA_1_prt_a\tprod_new.public.rootA_1_prt_a",
	}
	for _, w := range wants {
		if !strings.Contains(body, w) {
			t.Errorf("expected file to contain %q, got:\n%s", w, body)
		}
	}
}

func TestWriteSkipExistingList_OmitsEmptyDbName(t *testing.T) {
	// Defensive: if a record was produced without a db name (e.g., a code
	// path that doesn't have it), the writer should still produce a valid
	// FQN, falling back to "<schema>.<name>" instead of ".schema.name".
	ensureTestLogger()
	ResetSkipExistingState()
	recordSkip(SkippedTable{
		SourceSchema: "public", SourceName: "t1",
		DestSchema: "public", DestName: "t1",
		Reason: SkipReasonExists,
	})

	tmpHome := t.TempDir()
	t.Setenv("HOME", tmpHome)
	prevCurrentUser := operating.System.CurrentUser
	operating.System.CurrentUser = func() (*user.User, error) {
		return &user.User{HomeDir: tmpHome}, nil
	}
	defer func() { operating.System.CurrentUser = prevCurrentUser }()

	if err := WriteSkipExistingList("no-db"); err != nil {
		t.Fatalf("WriteSkipExistingList: %v", err)
	}
	raw, err := os.ReadFile(tmpHome + "/gpAdminLogs/no-db_skip_existing.list")
	if err != nil {
		t.Fatalf("read produced file: %v", err)
	}
	want := "exists\tpublic.t1\tpublic.t1"
	if !strings.Contains(string(raw), want) {
		t.Errorf("expected file to contain %q, got:\n%s", want, string(raw))
	}
}
