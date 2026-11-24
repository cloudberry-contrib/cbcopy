package copy

import (
	"testing"
)

// ---- leavesByRoot ----

func TestLeavesByRoot_ReturnsMatchingLeaves(t *testing.T) {
	leaves := []PartLeafTable{
		{RootName: "public.sales", LeafName: "public.sales_1_prt_q1", Boundary: "b1"},
		{RootName: "public.sales", LeafName: "public.sales_1_prt_q2", Boundary: "b2"},
		{RootName: "public.orders", LeafName: "public.orders_1_prt_q1", Boundary: "b1"},
	}
	got := leavesByRoot(leaves, "public.sales")
	if len(got) != 2 {
		t.Fatalf("expected 2 leaves, got %d", len(got))
	}
	for _, l := range got {
		if l.RootName != "public.sales" {
			t.Errorf("unexpected root %q", l.RootName)
		}
	}
}

func TestLeavesByRoot_NoMatch(t *testing.T) {
	leaves := []PartLeafTable{
		{RootName: "public.orders", LeafName: "public.orders_1_prt_q1", Boundary: "b1"},
	}
	got := leavesByRoot(leaves, "public.sales")
	if len(got) != 0 {
		t.Fatalf("expected 0 leaves, got %d", len(got))
	}
}

func TestLeavesByRoot_EmptyInput(t *testing.T) {
	got := leavesByRoot(nil, "public.sales")
	if len(got) != 0 {
		t.Fatalf("expected 0 leaves, got %d", len(got))
	}
}

// ---- helpers ----

func makeLeaves(root string, specs [][2]string) []PartLeafTable {
	out := make([]PartLeafTable, len(specs))
	for i, s := range specs {
		out[i] = PartLeafTable{RootName: root, LeafName: s[0], Boundary: s[1]}
	}
	return out
}

// ---- matchLeavesByBoundary: same-version matching ----

func TestMatchLeaves_SuccessExactMatch(t *testing.T) {
	src := makeLeaves("public.sales", [][2]string{
		{"public.sales_q1", "FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')"},
		{"public.sales_q2", "FOR VALUES FROM ('2024-04-01') TO ('2024-07-01')"},
	})
	dst := makeLeaves("public.sales", [][2]string{
		{"public.sales_h2", "FOR VALUES FROM ('2024-04-01') TO ('2024-07-01')"},
		{"public.sales_h1", "FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')"},
	})

	matchedSrc, matchedDst, unmatched := matchLeavesByBoundary(src, dst)
	if len(unmatched) != 0 {
		t.Fatalf("expected 0 unmatched, got %d", len(unmatched))
	}
	if len(matchedSrc) != 2 || len(matchedDst) != 2 {
		t.Fatalf("expected 2 pairs, got src=%d dst=%d", len(matchedSrc), len(matchedDst))
	}
	if matchedSrc[0].Name != "sales_q1" {
		t.Errorf("unexpected src[0] name %q", matchedSrc[0].Name)
	}
	if matchedDst[0].Name != "sales_h1" {
		t.Errorf("unexpected dst[0] name %q (want sales_h1, boundary-matched)", matchedDst[0].Name)
	}
}

// ---- matchLeavesByBoundary: partial match cases ----

func TestMatchLeaves_LeafCountMismatch_PartialMatch(t *testing.T) {
	src := makeLeaves("public.sales", [][2]string{
		{"public.sales_q1", "b1"},
		{"public.sales_q2", "b2"},
		{"public.sales_q3", "b3"},
	})
	dst := makeLeaves("public.sales", [][2]string{
		{"public.sales_h1", "b1"},
		{"public.sales_h2", "b2"},
	})

	matchedSrc, matchedDst, unmatched := matchLeavesByBoundary(src, dst)
	if len(matchedSrc) != 2 || len(matchedDst) != 2 {
		t.Fatalf("expected 2 matched pairs, got %d", len(matchedSrc))
	}
	if len(unmatched) != 1 {
		t.Fatalf("expected 1 unmatched leaf, got %d", len(unmatched))
	}
	if unmatched[0].LeafName != "public.sales_q3" {
		t.Errorf("expected unmatched leaf sales_q3, got %q", unmatched[0].LeafName)
	}
}

func TestMatchLeaves_NoBoundaryMatchForSrcLeaf_PartialMatch(t *testing.T) {
	src := makeLeaves("public.sales", [][2]string{
		{"public.sales_q1", "b1"},
		{"public.sales_q2", "b_no_match"},
	})
	dst := makeLeaves("public.sales", [][2]string{
		{"public.sales_h1", "b1"},
		{"public.sales_h2", "b2"},
	})

	matchedSrc, _, unmatched := matchLeavesByBoundary(src, dst)
	if len(matchedSrc) != 1 {
		t.Fatalf("expected 1 matched pair, got %d", len(matchedSrc))
	}
	if len(unmatched) != 1 {
		t.Fatalf("expected 1 unmatched, got %d", len(unmatched))
	}
	if unmatched[0].LeafName != "public.sales_q2" {
		t.Errorf("unexpected unmatched leaf %q", unmatched[0].LeafName)
	}
}

func TestMatchLeaves_EmptySrcBoundary_TreatedAsUnmatched(t *testing.T) {
	src := makeLeaves("public.sales", [][2]string{
		{"public.sales_q1", ""},
		{"public.sales_q2", "b2"},
	})
	dst := makeLeaves("public.sales", [][2]string{
		{"public.sales_h1", "b1"},
		{"public.sales_h2", "b2"},
	})

	matchedSrc, _, unmatched := matchLeavesByBoundary(src, dst)
	if len(matchedSrc) != 1 {
		t.Fatalf("expected 1 matched pair, got %d", len(matchedSrc))
	}
	if len(unmatched) != 1 || unmatched[0].LeafName != "public.sales_q1" {
		t.Fatalf("expected sales_q1 as unmatched, got %v", unmatched)
	}
}

func TestMatchLeaves_EmptyDestBoundary_SrcBecomesUnmatched(t *testing.T) {
	src := makeLeaves("public.sales", [][2]string{
		{"public.sales_q1", "b1"},
	})
	dst := makeLeaves("public.sales", [][2]string{
		{"public.sales_h1", ""},
	})

	matchedSrc, _, unmatched := matchLeavesByBoundary(src, dst)
	if len(matchedSrc) != 0 {
		t.Fatalf("expected 0 matched pairs, got %d", len(matchedSrc))
	}
	if len(unmatched) != 1 {
		t.Fatalf("expected 1 unmatched leaf, got %d", len(unmatched))
	}
}

func TestMatchLeaves_DuplicateSrcBoundary_BothUnmatched(t *testing.T) {
	src := makeLeaves("public.sales", [][2]string{
		{"public.sales_q1", "b1"},
		{"public.sales_q2", "b1"}, // duplicate
	})
	dst := makeLeaves("public.sales", [][2]string{
		{"public.sales_h1", "b1"},
		{"public.sales_h2", "b2"},
	})

	matchedSrc, _, unmatched := matchLeavesByBoundary(src, dst)
	if len(matchedSrc) != 0 {
		t.Fatalf("expected 0 matched pairs (duplicate src boundary), got %d", len(matchedSrc))
	}
	if len(unmatched) != 2 {
		t.Fatalf("expected 2 unmatched leaves, got %d", len(unmatched))
	}
}

func TestMatchLeaves_DuplicateDestBoundary_SrcUnmatched(t *testing.T) {
	src := makeLeaves("public.sales", [][2]string{
		{"public.sales_q1", "b1"},
		{"public.sales_q2", "b2"},
	})
	dst := makeLeaves("public.sales", [][2]string{
		{"public.sales_h1", "b1"},
		{"public.sales_h2", "b1"}, // duplicate
	})

	matchedSrc, _, unmatched := matchLeavesByBoundary(src, dst)
	if len(matchedSrc) != 0 {
		t.Fatalf("expected 0 matched pairs, got %d", len(matchedSrc))
	}
	if len(unmatched) != 2 {
		t.Fatalf("expected 2 unmatched, got %d", len(unmatched))
	}
}

func TestMatchLeaves_DestIsPlainTable_ReturnsNil(t *testing.T) {
	src := makeLeaves("public.sales", [][2]string{
		{"public.sales_q1", "b1"},
	})
	matchedSrc, matchedDst, unmatched := matchLeavesByBoundary(src, nil)
	if matchedSrc != nil || matchedDst != nil || unmatched != nil {
		t.Fatal("expected all nil when dest has no leaves")
	}
}

func TestMatchLeaves_SrcHasNoLeaves_ReturnsNil(t *testing.T) {
	dst := makeLeaves("public.sales", [][2]string{
		{"public.sales_h1", "b1"},
	})
	matchedSrc, matchedDst, unmatched := matchLeavesByBoundary(nil, dst)
	if matchedSrc != nil || matchedDst != nil || unmatched != nil {
		t.Fatal("expected all nil when src has no leaves")
	}
}

func TestMatchLeaves_RelTuplesPreserved(t *testing.T) {
	src := makeLeaves("public.sales", [][2]string{
		{"public.sales_q1", "b1"},
	})
	src[0].RelTuples = 500000
	dst := makeLeaves("public.sales", [][2]string{
		{"public.sales_h1", "b1"},
	})

	matchedSrc, _, unmatched := matchLeavesByBoundary(src, dst)
	if len(unmatched) != 0 {
		t.Fatalf("expected 0 unmatched, got %d", len(unmatched))
	}
	if matchedSrc[0].RelTuples != 500000 {
		t.Errorf("RelTuples not preserved: got %d", matchedSrc[0].RelTuples)
	}
}

// ---- matchLeavesByBoundary: cross-version boundary normalisation ----

func TestMatchLeaves_CrossVersion_GP6toGP7_Range(t *testing.T) {
	src := makeLeaves("public.sales", [][2]string{
		{"public.sales_q1", "PARTITION p1 START ('2024-01-01'::date) END ('2024-02-01'::date) WITH (appendonly='true')"},
		{"public.sales_q2", "PARTITION p2 START ('2024-02-01'::date) END ('2024-03-01'::date) WITH (appendonly='true')"},
	})
	dst := makeLeaves("public.sales", [][2]string{
		{"public.sales_h2", "FOR VALUES FROM ('2024-02-01') TO ('2024-03-01')"},
		{"public.sales_h1", "FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')"},
	})

	matchedSrc, matchedDst, unmatched := matchLeavesByBoundary(src, dst)
	if len(unmatched) != 0 {
		t.Fatalf("expected 0 unmatched in cross-version match, got %d", len(unmatched))
	}
	if len(matchedSrc) != 2 || len(matchedDst) != 2 {
		t.Fatalf("expected 2 pairs, got src=%d dst=%d", len(matchedSrc), len(matchedDst))
	}
}

func TestMatchLeaves_CrossVersion_GP6toGP7_Default(t *testing.T) {
	src := makeLeaves("public.sales", [][2]string{
		{"public.sales_def", "DEFAULT PARTITION pother  WITH (appendonly='true')"},
	})
	dst := makeLeaves("public.sales", [][2]string{
		{"public.sales_default", "DEFAULT"},
	})

	matchedSrc, _, unmatched := matchLeavesByBoundary(src, dst)
	if len(unmatched) != 0 {
		t.Fatalf("expected DEFAULT to match cross-version, got %d unmatched", len(unmatched))
	}
	if len(matchedSrc) != 1 {
		t.Fatalf("expected 1 matched pair, got %d", len(matchedSrc))
	}
}

func TestMatchLeaves_CrossVersion_GP6toGP7_List(t *testing.T) {
	src := makeLeaves("public.sales", [][2]string{
		{"public.sales_l1", "PARTITION pa VALUES('apple', 'banana') WITH (appendonly='true')"},
	})
	dst := makeLeaves("public.sales", [][2]string{
		{"public.sales_list1", "FOR VALUES IN ('apple', 'banana')"},
	})

	matchedSrc, _, unmatched := matchLeavesByBoundary(src, dst)
	if len(unmatched) != 0 {
		t.Fatalf("expected LIST to match cross-version, got %d unmatched", len(unmatched))
	}
	if len(matchedSrc) != 1 {
		t.Fatalf("expected 1 matched pair, got %d", len(matchedSrc))
	}
}

// ---- decidePairAction ----

func TestDecidePairAction_FullMatch(t *testing.T) {
	if got := decidePairAction(4, 0, false); got != PairFull {
		t.Errorf("full match: expected %q, got %q", PairFull, got)
	}
	if got := decidePairAction(4, 0, true); got != PairFull {
		t.Errorf("full match with flag: expected %q, got %q", PairFull, got)
	}
}

func TestDecidePairAction_PartialMatch_WithFlag(t *testing.T) {
	if got := decidePairAction(2, 2, true); got != PairPartial {
		t.Errorf("partial+flag: expected %q, got %q", PairPartial, got)
	}
}

func TestDecidePairAction_PartialMatch_NoFlag(t *testing.T) {
	if got := decidePairAction(2, 2, false); got != PairFatal {
		t.Errorf("partial+no flag: expected %q, got %q", PairFatal, got)
	}
}

func TestDecidePairAction_ZeroMatch_WithFlag(t *testing.T) {
	if got := decidePairAction(0, 4, true); got != PairRootFallback {
		t.Errorf("zero+flag: expected %q, got %q", PairRootFallback, got)
	}
}

func TestDecidePairAction_ZeroMatch_NoFlag(t *testing.T) {
	if got := decidePairAction(0, 4, false); got != PairFatal {
		t.Errorf("zero+no flag: expected %q, got %q", PairFatal, got)
	}
}

func TestDecidePairAction_ZeroMatchZeroUnmatched_WithFlag(t *testing.T) {
	// Edge case: both sides empty after filtering (e.g. all boundaries duplicated)
	if got := decidePairAction(0, 0, true); got != PairRootFallback {
		t.Errorf("zero/zero+flag: expected %q, got %q", PairRootFallback, got)
	}
}

func TestDecidePairAction_ZeroMatchZeroUnmatched_NoFlag(t *testing.T) {
	if got := decidePairAction(0, 0, false); got != PairFatal {
		t.Errorf("zero/zero+no flag: expected %q, got %q", PairFatal, got)
	}
}
