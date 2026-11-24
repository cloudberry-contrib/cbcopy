package copy

import (
	"fmt"
	"testing"
)

func TestSplitListValues_Normal(t *testing.T) {
	got := splitListValues("'beijing', 'shanghai'")
	want := []string{"beijing", "shanghai"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("normal: got %v, want %v", got, want)
	}
}

func TestSplitListValues_CommaInQuotes(t *testing.T) {
	got := splitListValues("'a,b', 'c'")
	want := []string{"a,b", "c"}
	if len(got) != len(want) {
		t.Errorf("comma in quotes: got %v (len=%d), want %v (len=%d)", got, len(got), want, len(want))
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("comma in quotes[%d]: got %q, want %q", i, got[i], want[i])
		}
	}
	fmt.Printf("splitListValues(\"'a,b', 'c'\") = %v ✓\n", got)
}

func TestSplitListValues_MultipleCommasInQuotes(t *testing.T) {
	got := splitListValues("'a,b,c', 'd', 'e,f'")
	want := []string{"a,b,c", "d", "e,f"}
	if len(got) != len(want) {
		t.Errorf("multi comma: got %v (len=%d), want %v (len=%d)", got, len(got), want, len(want))
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("multi comma[%d]: got %q, want %q", i, got[i], want[i])
		}
	}
	fmt.Printf("splitListValues(\"'a,b,c', 'd', 'e,f'\") = %v ✓\n", got)
}

func TestSplitListValues_NoQuotes(t *testing.T) {
	got := splitListValues("1, 2, 3")
	want := []string{"1", "2", "3"}
	if len(got) != len(want) {
		t.Errorf("no quotes: got %v, want %v", got, want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("no quotes[%d]: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestSplitListValues_EscapedQuote(t *testing.T) {
	// SQL escaped quote: 'it''s' → it's
	got := splitListValues("'it''s', 'fine'")
	want := []string{"it''s", "fine"}
	if len(got) != len(want) {
		t.Errorf("escaped: got %v, want %v", got, want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("escaped[%d]: got %q, want %q", i, got[i], want[i])
		}
	}
	fmt.Printf("splitListValues(\"'it''s', 'fine'\") = %v ✓\n", got)
}

func TestSplitListValues_BoundaryNormalize_ListWithComma(t *testing.T) {
	// 完整的 normalizeBoundary 流程：带逗号的 LIST 值
	gp6 := normalizeBoundary("PARTITION pa VALUES('a,b', 'c') WITH (tablename='t')")
	gp7 := normalizeBoundary("FOR VALUES IN ('a,b', 'c')")
	fmt.Printf("GP6: %s\nGP7: %s\nmatch: %v\n", gp6, gp7, gp6 == gp7)
	if gp6 != gp7 {
		t.Errorf("cross-version LIST with comma: GP6=%q GP7=%q", gp6, gp7)
	}
}
