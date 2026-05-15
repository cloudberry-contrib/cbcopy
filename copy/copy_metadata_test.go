package copy

import (
	"reflect"
	"testing"

	"github.com/cloudberry-contrib/cbcopy/meta/builtin"
	"github.com/cloudberry-contrib/cbcopy/option"
	"github.com/cloudberry-contrib/cbcopy/utils"
	"github.com/spf13/pflag"
)

// setupTablePairFilterFixtures installs the global config that
// filterTablePairsByDestExisting reads from. The destination inventory is
// seeded via Option.MarkDestTables so the helper exercises the same path
// production uses.
func setupTablePairFilterFixtures(t *testing.T, destDb string, present []string) {
	t.Helper()

	fs := pflag.NewFlagSet("table-pair-filter-test", pflag.ContinueOnError)
	fs.StringSlice(option.DBNAME, []string{}, "")
	fs.StringSlice(option.DEST_DBNAME, []string{}, "")
	fs.StringSlice(option.EXCLUDE_TABLE, []string{}, "")
	fs.String(option.EXCLUDE_TABLE_FILE, "", "")
	fs.StringSlice(option.INCLUDE_TABLE, []string{}, "")
	fs.String(option.INCLUDE_TABLE_FILE, "", "")
	fs.StringSlice(option.DEST_TABLE, []string{}, "")
	fs.String(option.DEST_TABLE_FILE, "", "")
	fs.Bool(option.APPEND, false, "")
	fs.Bool(option.SKIP_EXISTING, true, "")
	fs.StringSlice(option.SCHEMA, []string{}, "")
	fs.StringSlice(option.DEST_SCHEMA, []string{}, "")
	fs.String(option.SCHEMA_MAPPING_FILE, "", "")
	fs.String(option.OWNER_MAPPING_FILE, "", "")
	fs.String(option.DEST_TABLESPACE, "", "")
	fs.String(option.TABLESPACE_MAPPING_FILE, "", "")
	fs.String(option.CONNECTION_MODE, "push", "")
	utils.SetCmdFlags(fs)

	o, err := option.NewOption(fs)
	if err != nil {
		t.Fatalf("NewOption: %v", err)
	}
	userTables := make(map[string]option.TableStatistics, len(present))
	for _, fqn := range present {
		userTables[fqn] = option.TableStatistics{}
	}
	o.MarkDestTables(destDb, userTables, map[string]bool{})
	config = o

	builtin.ResetSkipExistingState()
}

func TestFilterTablePairs_NoneOnDest_AllKept(t *testing.T) {
	setupTablePairFilterFixtures(t, "dst", nil)

	src := []option.Table{
		{Schema: "src_s", Name: "t1"},
		{Schema: "src_s", Name: "t2"},
	}
	dst := []option.Table{
		{Schema: "dst_s", Name: "t1"},
		{Schema: "dst_s", Name: "t2"},
	}
	keptSrc, keptDst := filterTablePairsByDestExisting("src", "dst", src, dst, nil)
	if !reflect.DeepEqual(keptSrc, src) || !reflect.DeepEqual(keptDst, dst) {
		t.Fatalf("expected all pairs kept, got src=%v dst=%v", keptSrc, keptDst)
	}
	if builtin.NumSkipExisting() != 0 {
		t.Fatalf("expected 0 skips, got %d", builtin.NumSkipExisting())
	}
}

func TestFilterTablePairs_SomeOnDest_FilteredAndRecorded(t *testing.T) {
	// dst_s.t1 exists on dest; dst_s.t2 does not.
	setupTablePairFilterFixtures(t, "dst", []string{"dst_s.t1"})

	src := []option.Table{
		{Schema: "src_s", Name: "src_t1"},
		{Schema: "src_s", Name: "src_t2"},
	}
	dst := []option.Table{
		{Schema: "dst_s", Name: "t1"},
		{Schema: "dst_s", Name: "t2"},
	}
	keptSrc, keptDst := filterTablePairsByDestExisting("src", "dst", src, dst, nil)

	if len(keptSrc) != 1 || keptSrc[0].Name != "src_t2" {
		t.Fatalf("expected only src_t2 kept, got %v", keptSrc)
	}
	if len(keptDst) != 1 || keptDst[0].Name != "t2" {
		t.Fatalf("expected only dst_s.t2 kept, got %v", keptDst)
	}
	if builtin.NumSkipExisting() != 1 {
		t.Fatalf("expected 1 recorded skip, got %d", builtin.NumSkipExisting())
	}
	rec := builtin.SkipExistingTables()[0]
	if rec.SourceDbName != "src" || rec.SourceSchema != "src_s" || rec.SourceName != "src_t1" ||
		rec.DestDbName != "dst" || rec.DestSchema != "dst_s" || rec.DestName != "t1" {
		t.Fatalf("unexpected skip record: %+v", rec)
	}
	if rec.Reason != builtin.SkipReasonExists {
		t.Fatalf("expected SkipReasonExists, got %v", rec.Reason)
	}
}

func TestFilterTablePairs_AllOnDest_NoneKept(t *testing.T) {
	setupTablePairFilterFixtures(t, "dst", []string{"dst_s.t1", "dst_s.t2"})

	src := []option.Table{
		{Schema: "src_s", Name: "t1"},
		{Schema: "src_s", Name: "t2"},
	}
	dst := []option.Table{
		{Schema: "dst_s", Name: "t1"},
		{Schema: "dst_s", Name: "t2"},
	}
	keptSrc, keptDst := filterTablePairsByDestExisting("src", "dst", src, dst, nil)
	if len(keptSrc) != 0 || len(keptDst) != 0 {
		t.Fatalf("expected no pairs kept, got src=%v dst=%v", keptSrc, keptDst)
	}
	if builtin.NumSkipExisting() != 2 {
		t.Fatalf("expected 2 recorded skips, got %d", builtin.NumSkipExisting())
	}
}

func TestFilterTablePairs_EmptyInput_Passthrough(t *testing.T) {
	setupTablePairFilterFixtures(t, "dst", []string{"dst_s.anything"})

	keptSrc, keptDst := filterTablePairsByDestExisting("src", "dst", nil, nil, nil)
	if keptSrc != nil || keptDst != nil {
		t.Fatalf("expected nil passthrough on empty input, got src=%v dst=%v", keptSrc, keptDst)
	}
	if builtin.NumSkipExisting() != 0 {
		t.Fatalf("expected 0 skips, got %d", builtin.NumSkipExisting())
	}
}
