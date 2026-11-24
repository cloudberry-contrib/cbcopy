package copy

import (
	"testing"

	"github.com/cloudberry-contrib/cbcopy/option"
)

func TestExpandIntermediateLeaves_WithLeaves(t *testing.T) {
	expandMap := make(map[string]option.TableStatistics)
	leaves := []option.Table{
		{Schema: "s", Name: "leaf_east", RelTuples: 50},
		{Schema: "s", Name: "leaf_west", RelTuples: 50},
	}
	ok := expandIntermediateLeaves(expandMap, leaves)
	if !ok {
		t.Error("expected true, got false")
	}
	if len(expandMap) != 2 {
		t.Errorf("expected 2 entries, got %d", len(expandMap))
	}
	if _, exists := expandMap["s.leaf_east"]; !exists {
		t.Error("missing s.leaf_east")
	}
	if _, exists := expandMap["s.leaf_west"]; !exists {
		t.Error("missing s.leaf_west")
	}
	if expandMap["s.leaf_east"].RelTuples != 50 {
		t.Errorf("expected RelTuples=50, got %d", expandMap["s.leaf_east"].RelTuples)
	}
}

func TestExpandIntermediateLeaves_EmptyLeaves(t *testing.T) {
	expandMap := make(map[string]option.TableStatistics)
	ok := expandIntermediateLeaves(expandMap, nil)
	if ok {
		t.Error("expected false for nil leaves, got true")
	}
	if len(expandMap) != 0 {
		t.Errorf("expected empty map, got %d entries", len(expandMap))
	}

	ok = expandIntermediateLeaves(expandMap, []option.Table{})
	if ok {
		t.Error("expected false for empty leaves, got true")
	}
}

func TestExpandIntermediateLeaves_PreservesExisting(t *testing.T) {
	expandMap := make(map[string]option.TableStatistics)
	expandMap["s.existing"] = option.TableStatistics{Partition: 0, RelTuples: 999}

	leaves := []option.Table{
		{Schema: "s", Name: "new_leaf", RelTuples: 100},
	}
	ok := expandIntermediateLeaves(expandMap, leaves)
	if !ok {
		t.Error("expected true")
	}
	if len(expandMap) != 2 {
		t.Errorf("expected 2 entries, got %d", len(expandMap))
	}
	if expandMap["s.existing"].RelTuples != 999 {
		t.Error("existing entry should be preserved")
	}
	if expandMap["s.new_leaf"].RelTuples != 100 {
		t.Error("new leaf should be added")
	}
}

func TestExpandIntermediateLeaves_MultipleSchemas(t *testing.T) {
	expandMap := make(map[string]option.TableStatistics)
	leaves := []option.Table{
		{Schema: "schema_a", Name: "leaf1", RelTuples: 10},
		{Schema: "schema_b", Name: "leaf2", RelTuples: 20},
	}
	ok := expandIntermediateLeaves(expandMap, leaves)
	if !ok {
		t.Error("expected true")
	}
	if _, exists := expandMap["schema_a.leaf1"]; !exists {
		t.Error("missing schema_a.leaf1")
	}
	if _, exists := expandMap["schema_b.leaf2"]; !exists {
		t.Error("missing schema_b.leaf2")
	}
}
