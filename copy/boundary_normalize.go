package copy

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// normalizeBoundary converts a partition boundary string from either the GP6
// pg_get_partition_rule_def format or the GP7/CBDB pg_get_expr format into a
// canonical string that compares equal across database versions when the
// underlying partition bounds are semantically identical.
//
// GP6  "START ('2024-01-01'::date) END ('2024-02-01'::date) EVERY ('1 mon'::interval) WITH (...)"
// GP6  "PARTITION jan START ('2024-01-01'::date) END ('2024-02-01'::date) WITH (...)"
// GP7  "FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')"
// all  → "RANGE:[2024-01-01,2024-02-01)"
//
// Returns "" for empty input. Unrecognised input is returned unchanged so
// that it can never accidentally match a known-format boundary.
func normalizeBoundary(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if s == "DEFAULT" {
		return "DEFAULT"
	}
	upper := strings.ToUpper(s)
	if strings.HasPrefix(upper, "FOR VALUES") {
		return normalizeGP7Boundary(s)
	}
	// GP6 pg_get_partition_rule_def output comes in several forms:
	//   "PARTITION <name> START ..."       (named partitions)
	//   "START ... END ... EVERY ..."      (EVERY-generated, no PARTITION prefix)
	//   "DEFAULT PARTITION <name> ..."     (default partitions)
	//   "SUBPARTITION <name> VALUES ..."   (sub-partitions)
	//   "DEFAULT SUBPARTITION <name> ..."  (default sub-partitions)
	//   "PARTITION <name> VALUES ..."      (list partitions)
	// Dispatch broadly — normalizeGP6RuleDef returns s unchanged if no regex matches.
	if strings.HasPrefix(upper, "PARTITION ") || strings.HasPrefix(upper, "DEFAULT") ||
		strings.HasPrefix(upper, "START") || strings.HasPrefix(upper, "SUBPARTITION ") ||
		strings.Contains(upper, " START (") || strings.Contains(upper, "VALUES") {
		return normalizeGP6RuleDef(s)
	}
	return s
}

var (
	// GP7/CBDB: pg_get_expr output.
	// NOTE: currently handles single-column partition bounds only.
	// Multi-column bounds (e.g. FROM (1, 'a') TO (2, 'b')) will not
	// normalise and will be returned as-is (fails safe — no silent mismatch).
	reGP7Range = regexp.MustCompile(`(?i)^FOR\s+VALUES\s+FROM\s+\((.+?)\)\s+TO\s+\((.+?)\)$`)
	reGP7List  = regexp.MustCompile(`(?i)^FOR\s+VALUES\s+IN\s+\((.+)\)$`)
	reGP7Hash  = regexp.MustCompile(`(?i)^FOR\s+VALUES\s+WITH\s+\(modulus\s+(\d+),\s*remainder\s+(\d+)\)$`)

	// GP6: pg_get_partition_rule_def output.
	// Applied after preprocessing strips EVERY (...), WITH (...), and type casts.
	// After preprocessing, bounds are clean: START ('val') or START (val)
	reGP6RuleRange = regexp.MustCompile(`(?i)START\s*\(\s*'?(\(?[^')]+?\)?)'?\s*\)\s*(INCLUSIVE|EXCLUSIVE)?\s*END\s*\(\s*'?(\(?[^')]+?\)?)'?\s*\)\s*(INCLUSIVE|EXCLUSIVE)?`)
	reGP6RuleList  = regexp.MustCompile(`(?i)\bVALUES\s*\(([^)]+)\)`)

	// Preprocess helpers for GP6 boundary strings.
	reStripEvery = regexp.MustCompile(`(?i)\s+EVERY\s*\([^)]*\)`)
	reStripWith  = regexp.MustCompile(`(?i)\s+WITH\s*\(.*$`)
	// Strip type casts: ::date, ::bigint, ::timestamp without time zone, ::character(3), etc.
	reStripCast = regexp.MustCompile(`::[\w][\w ]*(?:\(\d+\))?`)
)

// normalizeGP6RuleDef parses the output of pg_get_partition_rule_def(oid, true)
// used as the boundary field for GP6 (Greenplum 6 / HashData 3.x) partitions.
//
// Recognised formats (after preprocessing strips EVERY/WITH suffixes):
//
//	RANGE:   "[PARTITION <n>] START (<val>[::type]) [INCLUSIVE|EXCLUSIVE] END (<val>[::type]) [INCLUSIVE|EXCLUSIVE]"
//	LIST:    "[PARTITION|SUBPARTITION <n>] VALUES(<v1>, <v2>)"
//	DEFAULT: "DEFAULT [SUB]PARTITION <n> ..."
func normalizeGP6RuleDef(s string) string {
	upper := strings.ToUpper(s)

	// DEFAULT / DEFAULT SUBPARTITION
	if strings.HasPrefix(upper, "DEFAULT ") {
		return "DEFAULT"
	}

	// Preprocess: strip trailing EVERY (...), WITH (...), and type casts to simplify regex.
	cleaned := reStripEvery.ReplaceAllString(s, "")
	cleaned = reStripWith.ReplaceAllString(cleaned, "")
	cleaned = reStripCast.ReplaceAllString(cleaned, "")

	// RANGE: START ... END ...
	if m := reGP6RuleRange.FindStringSubmatch(cleaned); len(m) >= 4 {
		start := stripBoundaryValue(m[1])
		end := stripBoundaryValue(m[3])
		startMod := strings.ToUpper(strings.TrimSpace(m[2]))
		endMod := strings.ToUpper(strings.TrimSpace(m[4]))
		// GP6 default: START inclusive, END exclusive (matches GP7 FROM..TO semantics)
		lb, rb := "[", ")"
		if startMod == "EXCLUSIVE" {
			lb = "("
		}
		if endMod == "INCLUSIVE" {
			rb = "]"
		}
		return fmt.Sprintf("RANGE:%s%s,%s%s", lb, start, end, rb)
	}

	// LIST: VALUES(...)
	if m := reGP6RuleList.FindStringSubmatch(cleaned); len(m) == 2 {
		vals := splitListValues(m[1])
		sort.Strings(vals)
		return fmt.Sprintf("LIST:(%s)", strings.Join(vals, ","))
	}

	return s
}

func normalizeGP7Boundary(s string) string {
	if m := reGP7Range.FindStringSubmatch(s); len(m) == 3 {
		start := stripSingleQuotes(strings.TrimSpace(m[1]))
		end := stripSingleQuotes(strings.TrimSpace(m[2]))
		// GP7 FROM..TO is always inclusive-start, exclusive-end
		return fmt.Sprintf("RANGE:[%s,%s)", start, end)
	}
	if m := reGP7List.FindStringSubmatch(s); len(m) == 2 {
		vals := splitListValues(m[1])
		sort.Strings(vals)
		return fmt.Sprintf("LIST:(%s)", strings.Join(vals, ","))
	}
	if m := reGP7Hash.FindStringSubmatch(s); len(m) == 3 {
		return fmt.Sprintf("HASH:%s:%s", m[1], m[2])
	}
	return s
}

// stripBoundaryValue cleans a captured boundary value:
//   - strips outer parentheses for negative numbers: (-100) → -100
//   - strips single quotes: '2024-01-01' → 2024-01-01
func stripBoundaryValue(s string) string {
	s = strings.TrimSpace(s)
	// Strip outer parens (GP6 wraps negative numbers: ((-100)) → captured as (-100))
	for len(s) >= 2 && s[0] == '(' && s[len(s)-1] == ')' {
		s = s[1 : len(s)-1]
	}
	return stripSingleQuotes(s)
}

// splitListValues splits "val1, val2, ..." and strips surrounding single quotes.
func splitListValues(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		v := stripSingleQuotes(strings.TrimSpace(p))
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func stripSingleQuotes(s string) string {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	return s
}
