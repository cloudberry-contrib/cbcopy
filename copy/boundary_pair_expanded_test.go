package copy

import (
	"testing"

	"github.com/cloudberry-contrib/cbcopy/option"
	"github.com/cloudberry-contrib/cbcopy/utils"
	"github.com/spf13/pflag"
)

func initTestConfig(t *testing.T, schemaArgs, destSchemaArgs []string) {
	t.Helper()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	fs.StringSlice(option.DBNAME, []string{}, "")
	fs.StringSlice(option.DEST_DBNAME, []string{}, "")
	fs.StringSlice(option.SCHEMA, schemaArgs, "")
	fs.StringSlice(option.DEST_SCHEMA, destSchemaArgs, "")
	fs.StringSlice(option.INCLUDE_TABLE, []string{}, "")
	fs.String(option.INCLUDE_TABLE_FILE, "", "")
	fs.StringSlice(option.DEST_TABLE, []string{}, "")
	fs.String(option.DEST_TABLE_FILE, "", "")
	fs.StringSlice(option.EXCLUDE_TABLE, []string{}, "")
	fs.String(option.EXCLUDE_TABLE_FILE, "", "")
	fs.String(option.SCHEMA_MAPPING_FILE, "", "")
	fs.String(option.OWNER_MAPPING_FILE, "", "")
	fs.String(option.DEST_TABLESPACE, "", "")
	fs.String(option.TABLESPACE_MAPPING_FILE, "", "")
	fs.String(option.CONNECTION_MODE, "push", "")
	fs.Bool(option.APPEND, false, "")
	fs.Bool(option.SKIP_EXISTING, false, "")
	utils.SetCmdFlags(fs)

	o, err := option.NewOption(fs)
	if err != nil {
		t.Fatalf("NewOption: %v", err)
	}
	config = o
}

func TestResolveDestRootFqn_NoMapping(t *testing.T) {
	initTestConfig(t, nil, nil)
	qw := &QueryWrapper{}
	got := qw.resolveDestRootFqn("public.t1")
	if got != "public.t1" {
		t.Errorf("no mapping: got %q, want %q", got, "public.t1")
	}
}

func TestResolveDestRootFqn_WithSchemaMapping(t *testing.T) {
	initTestConfig(t, []string{"db.src_schema"}, []string{"db.dst_schema"})
	qw := &QueryWrapper{}
	got := qw.resolveDestRootFqn("src_schema.t1")
	if got != "dst_schema.t1" {
		t.Errorf("with mapping: got %q, want %q", got, "dst_schema.t1")
	}
}

func TestResolveDestRootFqn_UnmappedSchema_FallsBackToDestSchema(t *testing.T) {
	// When dest-schema is set, unmapped schemas still get redirected to dest-schema
	initTestConfig(t, []string{"db.src_schema"}, []string{"db.dst_schema"})
	qw := &QueryWrapper{}
	got := qw.resolveDestRootFqn("other_schema.t1")
	// dest-schema is set, so GetDestSchemas() returns dst_schema → used as fallback
	if got != "dst_schema.t1" {
		t.Errorf("unmapped with dest-schema: got %q, want %q", got, "dst_schema.t1")
	}
}

func TestResolveDestRootFqn_UnmappedSchema_NoDestSchema(t *testing.T) {
	// No dest-schema set → unmapped schema stays unchanged
	initTestConfig(t, nil, nil)
	qw := &QueryWrapper{}
	got := qw.resolveDestRootFqn("other_schema.t1")
	if got != "other_schema.t1" {
		t.Errorf("unmapped no dest-schema: got %q, want %q", got, "other_schema.t1")
	}
}

func TestResolveDestRootFqn_InvalidFqn(t *testing.T) {
	initTestConfig(t, nil, nil)
	qw := &QueryWrapper{}
	got := qw.resolveDestRootFqn("nope")
	if got != "nope" {
		t.Errorf("invalid fqn: got %q, want %q", got, "nope")
	}
}

func TestBoundaryPairExpandedTables_NilDestConn(t *testing.T) {
	initTestConfig(t, nil, nil)
	qw := &QueryWrapper{}
	src := []option.Table{
		{Schema: "s", Name: "t1", RelTuples: 100},
		{Schema: "s", Name: "t2", RelTuples: 200},
	}
	dst := []option.Table{
		{Schema: "d", Name: "t1", RelTuples: 100},
		{Schema: "d", Name: "t2", RelTuples: 200},
	}
	gotSrc, gotDst := qw.boundaryPairExpandedTables(nil, nil, src, dst)
	if len(gotSrc) != 2 || len(gotDst) != 2 {
		t.Errorf("nil destConn: expected 2 pairs, got src=%d dst=%d", len(gotSrc), len(gotDst))
	}
	if gotSrc[0].Name != "t1" || gotDst[0].Name != "t1" {
		t.Errorf("nil destConn: pairs should be unchanged")
	}
}
