package copy

import "testing"

func TestNormalizeBoundary_Empty(t *testing.T) {
	if got := normalizeBoundary(""); got != "" {
		t.Errorf("expected empty, got %q", got)
	}
	if got := normalizeBoundary("   "); got != "" {
		t.Errorf("expected empty for whitespace, got %q", got)
	}
}

// ---- GP7/CBDB: pg_get_expr ----

func TestNormalizeBoundary_GP7_Range_Date(t *testing.T) {
	in := "FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')"
	want := "RANGE:[2024-01-01,2024-02-01)"
	if got := normalizeBoundary(in); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNormalizeBoundary_GP7_Range_Int(t *testing.T) {
	in := "FOR VALUES FROM (1) TO (1000)"
	want := "RANGE:[1,1000)"
	if got := normalizeBoundary(in); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNormalizeBoundary_GP7_List(t *testing.T) {
	in := "FOR VALUES IN ('apple', 'banana')"
	want := "LIST:(apple,banana)"
	if got := normalizeBoundary(in); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNormalizeBoundary_GP7_List_Sorted(t *testing.T) {
	in := "FOR VALUES IN ('zebra', 'apple')"
	want := "LIST:(apple,zebra)"
	if got := normalizeBoundary(in); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNormalizeBoundary_GP7_Hash(t *testing.T) {
	in := "FOR VALUES WITH (modulus 4, remainder 0)"
	want := "HASH:4:0"
	if got := normalizeBoundary(in); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNormalizeBoundary_GP7_Default(t *testing.T) {
	if got := normalizeBoundary("DEFAULT"); got != "DEFAULT" {
		t.Errorf("got %q, want DEFAULT", got)
	}
}

// ---- GP6: pg_get_partition_rule_def (named partitions) ----

func TestNormalizeBoundary_GP6_Range_Date(t *testing.T) {
	in := "PARTITION p1 START ('2024-01-01'::date) END ('2024-02-01'::date) WITH (appendonly='true')"
	want := "RANGE:[2024-01-01,2024-02-01)"
	if got := normalizeBoundary(in); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNormalizeBoundary_GP6_Range_Int(t *testing.T) {
	in := "PARTITION p1 START ('1'::integer) END ('1000'::integer) WITH (appendonly='true')"
	want := "RANGE:[1,1000)"
	if got := normalizeBoundary(in); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNormalizeBoundary_GP6_Range_ExclusiveStart(t *testing.T) {
	in := "PARTITION p1 START ('100') EXCLUSIVE END ('200') WITH (appendonly='true')"
	want := "RANGE:(100,200)"
	if got := normalizeBoundary(in); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNormalizeBoundary_GP6_Range_InclusiveEnd(t *testing.T) {
	in := "PARTITION p1 START ('1') END ('10') INCLUSIVE WITH (appendonly='true')"
	want := "RANGE:[1,10]"
	if got := normalizeBoundary(in); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNormalizeBoundary_GP6_List(t *testing.T) {
	in := "PARTITION pa VALUES('apple', 'banana') WITH (appendonly='true')"
	want := "LIST:(apple,banana)"
	if got := normalizeBoundary(in); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNormalizeBoundary_GP6_List_Sorted(t *testing.T) {
	in := "PARTITION pa VALUES('zebra', 'apple') WITH (appendonly='true')"
	want := "LIST:(apple,zebra)"
	if got := normalizeBoundary(in); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNormalizeBoundary_GP6_Default(t *testing.T) {
	in := "DEFAULT PARTITION pother  WITH (appendonly='true')"
	want := "DEFAULT"
	if got := normalizeBoundary(in); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// ---- Real GP6 boundary strings (verbatim from pg_get_partition_rule_def on GP6 6.27.1 / HashData 3.13.33) ----

func TestNormalizeBoundary_RealGP6_AllTypes(t *testing.T) {
	cases := []struct {
		label string
		input string
		want  string
	}{
		// date + EVERY
		{"date EVERY", "START ('2024-01-01'::date) END ('2024-02-01'::date) EVERY ('1 mon'::interval) WITH (appendonly='true', blocksize='1048576')", "RANGE:[2024-01-01,2024-02-01)"},
		// date + named
		{"date named", "PARTITION jan START ('2024-01-01'::date) END ('2024-02-01'::date) WITH (appendonly='true', blocksize='1048576')", "RANGE:[2024-01-01,2024-02-01)"},
		// int4 + EVERY (unquoted)
		{"int4 EVERY", "START (0) END (100) EVERY (100) WITH (appendonly='true', blocksize='1048576')", "RANGE:[0,100)"},
		// int4 + named (unquoted)
		{"int4 named", "PARTITION p1 START (0) END (100) WITH (appendonly='true', blocksize='1048576')", "RANGE:[0,100)"},
		// int4 + INCLUSIVE
		{"int4 INCLUSIVE", "PARTITION p2 START (100) END (200) INCLUSIVE WITH (appendonly='true', blocksize='1048576')", "RANGE:[100,200]"},
		// int4 negative (double parens)
		{"int4 negative", "PARTITION pn START ((-100)) END (0) WITH (appendonly='true', blocksize='1048576')", "RANGE:[-100,0)"},
		// bigint + EVERY (unquoted with cast)
		{"bigint EVERY", "START (0::bigint) END (100::bigint) EVERY (100::bigint) WITH (appendonly='true', blocksize='1048576')", "RANGE:[0,100)"},
		// bigint + named
		{"bigint named", "PARTITION p1 START (0::bigint) END (100::bigint) WITH (appendonly='true', blocksize='1048576')", "RANGE:[0,100)"},
		// bigint negative
		{"bigint negative", "PARTITION pn START ((-9999999999)::bigint) END (0::bigint) WITH (appendonly='true', blocksize='1048576')", "RANGE:[-9999999999,0)"},
		// smallint + EVERY
		{"smallint EVERY", "START (0::smallint) END (100::smallint) EVERY (100::smallint) WITH (appendonly='true', blocksize='1048576')", "RANGE:[0,100)"},
		// numeric + EVERY (unquoted)
		{"numeric EVERY", "START (0.0) END (10.0) EVERY (10.0) WITH (appendonly='true', blocksize='1048576')", "RANGE:[0.0,10.0)"},
		// numeric + named
		{"numeric named", "PARTITION p1 START (0.0) END (10.0) WITH (appendonly='true', blocksize='1048576')", "RANGE:[0.0,10.0)"},
		// real (float4) + named (unquoted with cast)
		{"real named", "PARTITION p1 START (0::real) END (10::real) WITH (appendonly='true', blocksize='1048576')", "RANGE:[0,10)"},
		// double precision + named (multi-word cast)
		{"double named", "PARTITION p1 START (0::double precision) END (100::double precision) WITH (appendonly='true', blocksize='1048576')", "RANGE:[0,100)"},
		// text + named (quoted with cast)
		{"text named", "PARTITION pa START ('A'::text) END ('M'::text) WITH (appendonly='true', blocksize='1048576')", "RANGE:[A,M)"},
		// char(3) + named (multi-char cast with parens)
		{"char(3) named", "PARTITION pa START ('AAA'::character(3)) END ('MMM'::character(3)) WITH (appendonly='true', blocksize='1048576')", "RANGE:[AAA,MMM)"},
		// timestamp without time zone + EVERY (multi-word cast)
		{"timestamp EVERY", "START ('2024-01-01 00:00:00'::timestamp without time zone) END ('2024-02-01 00:00:00'::timestamp without time zone) EVERY ('1 mon'::interval) WITH (appendonly='true', blocksize='1048576')", "RANGE:[2024-01-01 00:00:00,2024-02-01 00:00:00)"},
		// timestamp without time zone + named
		{"timestamp named", "PARTITION jan START ('2024-01-01 00:00:00'::timestamp without time zone) END ('2024-02-01 00:00:00'::timestamp without time zone) WITH (appendonly='true', blocksize='1048576')", "RANGE:[2024-01-01 00:00:00,2024-02-01 00:00:00)"},
		// timestamp with time zone + named
		{"timestamptz named", "PARTITION jan START ('2024-01-01 00:00:00+08'::timestamp with time zone) END ('2024-02-01 00:00:00+08'::timestamp with time zone) WITH (appendonly='true', blocksize='1048576')", "RANGE:[2024-01-01 00:00:00+08,2024-02-01 00:00:00+08)"},
		// LIST text
		{"list text", "PARTITION pa VALUES('beijing', 'shanghai') WITH (appendonly='true', blocksize='1048576')", "LIST:(beijing,shanghai)"},
		// LIST text with spaces
		{"list text spaces", "PARTITION pa VALUES('New York', 'Los Angeles') WITH (appendonly='true', blocksize='1048576')", "LIST:(Los Angeles,New York)"},
		// LIST int (unquoted)
		{"list int", "PARTITION active VALUES(1, 2, 3) WITH (appendonly='true', blocksize='1048576')", "LIST:(1,2,3)"},
		// LIST char(1)
		{"list char", "PARTITION py VALUES('Y') WITH (appendonly='true', blocksize='1048576')", "LIST:(Y)"},
		// DEFAULT
		{"default", "DEFAULT PARTITION pother  WITH (appendonly='true', blocksize='1048576')", "DEFAULT"},
		// DEFAULT SUBPARTITION
		{"default subpartition", "DEFAULT SUBPARTITION other  WITH (appendonly='true', blocksize='1048576')", "DEFAULT"},
		// SUBPARTITION LIST
		{"subpartition list", "SUBPARTITION east VALUES('east') WITH (appendonly='true', blocksize='1048576')", "LIST:(east)"},
		// ALTER TABLE ADD PARTITION (same format as named)
		{"alter add", "PARTITION p2 START (100) END (200) WITH (appendonly='true', blocksize='1048576')", "RANGE:[100,200)"},
		// SPLIT PARTITION (same format as named)
		{"split", "PARTITION p2a START (100) END (200) WITH (appendonly='true', blocksize='1048576')", "RANGE:[100,200)"},
	}
	for _, c := range cases {
		if got := normalizeBoundary(c.input); got != c.want {
			t.Errorf("[%s] got %q, want %q\n  input: %s", c.label, got, c.want, c.input)
		}
	}
}

// ---- Real GP7 boundary strings (verbatim from pg_get_expr on GP7 7.1.0) ----

func TestNormalizeBoundary_RealGP7_AllTypes(t *testing.T) {
	cases := []struct {
		label string
		input string
		want  string
	}{
		{"date", "FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')", "RANGE:[2024-01-01,2024-02-01)"},
		{"int4", "FOR VALUES FROM (0) TO (100)", "RANGE:[0,100)"},
		{"int4 negative", "FOR VALUES FROM ('-100') TO (0)", "RANGE:[-100,0)"},
		{"int4 neg large", "FOR VALUES FROM ('-9999') TO (0)", "RANGE:[-9999,0)"},
		{"bigint", "FOR VALUES FROM ('0') TO ('100')", "RANGE:[0,100)"},
		{"bigint negative", "FOR VALUES FROM ('-9999999999') TO ('0')", "RANGE:[-9999999999,0)"},
		{"smallint", "FOR VALUES FROM ('0') TO ('100')", "RANGE:[0,100)"},
		{"numeric", "FOR VALUES FROM (0.0) TO (10.0)", "RANGE:[0.0,10.0)"},
		{"real", "FOR VALUES FROM ('0') TO ('10')", "RANGE:[0,10)"},
		{"double", "FOR VALUES FROM ('0') TO ('100')", "RANGE:[0,100)"},
		{"text", "FOR VALUES FROM ('A') TO ('M')", "RANGE:[A,M)"},
		{"char(3)", "FOR VALUES FROM ('AAA') TO ('MMM')", "RANGE:[AAA,MMM)"},
		{"timestamp", "FOR VALUES FROM ('2024-01-01 00:00:00') TO ('2024-02-01 00:00:00')", "RANGE:[2024-01-01 00:00:00,2024-02-01 00:00:00)"},
		{"timestamptz", "FOR VALUES FROM ('2024-01-01 00:00:00+08') TO ('2024-02-01 00:00:00+08')", "RANGE:[2024-01-01 00:00:00+08,2024-02-01 00:00:00+08)"},
		{"list text", "FOR VALUES IN ('beijing', 'shanghai')", "LIST:(beijing,shanghai)"},
		{"list text spaces", "FOR VALUES IN ('New York', 'Los Angeles')", "LIST:(Los Angeles,New York)"},
		{"list int", "FOR VALUES IN (1, 2, 3)", "LIST:(1,2,3)"},
		{"list boolean", "FOR VALUES IN (true)", "LIST:(true)"},
		{"list NULL", "FOR VALUES IN (NULL)", "LIST:(NULL)"},
		{"default", "DEFAULT", "DEFAULT"},
		{"MINVALUE", "FOR VALUES FROM (MINVALUE) TO (0)", "RANGE:[MINVALUE,0)"},
		{"MAXVALUE", "FOR VALUES FROM (100) TO (MAXVALUE)", "RANGE:[100,MAXVALUE)"},
		{"hash", "FOR VALUES WITH (modulus 3, remainder 0)", "HASH:3:0"},
	}
	for _, c := range cases {
		if got := normalizeBoundary(c.input); got != c.want {
			t.Errorf("[%s] got %q, want %q\n  input: %s", c.label, got, c.want, c.input)
		}
	}
}

// ---- Real CBDB boundary strings (verbatim from pg_get_expr on CBDB 2.4.0) ----
// Verified identical to GP7 output for all tested types.

func TestNormalizeBoundary_RealCBDB_AllTypes(t *testing.T) {
	cases := []struct {
		label string
		input string
		want  string
	}{
		{"date", "FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')", "RANGE:[2024-01-01,2024-02-01)"},
		{"int4", "FOR VALUES FROM (0) TO (100)", "RANGE:[0,100)"},
		{"bigint", "FOR VALUES FROM ('0') TO ('100')", "RANGE:[0,100)"},
		{"numeric", "FOR VALUES FROM (0.0) TO (10.0)", "RANGE:[0.0,10.0)"},
		{"timestamp", "FOR VALUES FROM ('2024-01-01 00:00:00') TO ('2024-02-01 00:00:00')", "RANGE:[2024-01-01 00:00:00,2024-02-01 00:00:00)"},
		{"timestamptz", "FOR VALUES FROM ('2024-01-01 00:00:00+08') TO ('2024-02-01 00:00:00+08')", "RANGE:[2024-01-01 00:00:00+08,2024-02-01 00:00:00+08)"},
		{"list text", "FOR VALUES IN ('beijing', 'shanghai')", "LIST:(beijing,shanghai)"},
		{"list int", "FOR VALUES IN (1, 2, 3)", "LIST:(1,2,3)"},
		{"default", "DEFAULT", "DEFAULT"},
		{"multi-col range", "FOR VALUES FROM (0, 0) TO (10, 100)", "RANGE:[0, 0,10, 100)"},
	}
	for _, c := range cases {
		if got := normalizeBoundary(c.input); got != c.want {
			t.Errorf("[%s] got %q, want %q\n  input: %s", c.label, got, c.want, c.input)
		}
	}
}

// ---- Cross-version pairing: GP6 ↔ GP7/CBDB ----

func TestNormalizeBoundary_CrossVersion_AllTypes(t *testing.T) {
	cases := []struct {
		label string
		gp6   string
		gp7   string // GP7 = CBDB (verified identical)
	}{
		{"date EVERY", "START ('2024-01-01'::date) END ('2024-02-01'::date) EVERY ('1 mon'::interval) WITH (appendonly='true', blocksize='1048576')", "FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')"},
		{"date named", "PARTITION jan START ('2024-01-01'::date) END ('2024-02-01'::date) WITH (appendonly='true', blocksize='1048576')", "FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')"},
		{"int4 EVERY", "START (0) END (100) EVERY (100) WITH (appendonly='true', blocksize='1048576')", "FOR VALUES FROM (0) TO (100)"},
		{"int4 named", "PARTITION p1 START (0) END (100) WITH (appendonly='true', blocksize='1048576')", "FOR VALUES FROM (0) TO (100)"},
		{"int4 negative", "PARTITION pn START ((-100)) END (0) WITH (appendonly='true', blocksize='1048576')", "FOR VALUES FROM ('-100') TO (0)"},
		{"bigint EVERY", "START (0::bigint) END (100::bigint) EVERY (100::bigint) WITH (appendonly='true', blocksize='1048576')", "FOR VALUES FROM ('0') TO ('100')"},
		{"bigint named", "PARTITION p1 START (0::bigint) END (100::bigint) WITH (appendonly='true', blocksize='1048576')", "FOR VALUES FROM ('0') TO ('100')"},
		{"bigint negative", "PARTITION pn START ((-9999999999)::bigint) END (0::bigint) WITH (appendonly='true', blocksize='1048576')", "FOR VALUES FROM ('-9999999999') TO ('0')"},
		{"smallint EVERY", "START (0::smallint) END (100::smallint) EVERY (100::smallint) WITH (appendonly='true', blocksize='1048576')", "FOR VALUES FROM ('0') TO ('100')"},
		{"numeric EVERY", "START (0.0) END (10.0) EVERY (10.0) WITH (appendonly='true', blocksize='1048576')", "FOR VALUES FROM (0.0) TO (10.0)"},
		{"numeric named", "PARTITION p1 START (0.0) END (10.0) WITH (appendonly='true', blocksize='1048576')", "FOR VALUES FROM (0.0) TO (10.0)"},
		{"real named", "PARTITION p1 START (0::real) END (10::real) WITH (appendonly='true', blocksize='1048576')", "FOR VALUES FROM ('0') TO ('10')"},
		{"double named", "PARTITION p1 START (0::double precision) END (100::double precision) WITH (appendonly='true', blocksize='1048576')", "FOR VALUES FROM ('0') TO ('100')"},
		{"text named", "PARTITION pa START ('A'::text) END ('M'::text) WITH (appendonly='true', blocksize='1048576')", "FOR VALUES FROM ('A') TO ('M')"},
		{"char(3) named", "PARTITION pa START ('AAA'::character(3)) END ('MMM'::character(3)) WITH (appendonly='true', blocksize='1048576')", "FOR VALUES FROM ('AAA') TO ('MMM')"},
		{"timestamp EVERY", "START ('2024-01-01 00:00:00'::timestamp without time zone) END ('2024-02-01 00:00:00'::timestamp without time zone) EVERY ('1 mon'::interval) WITH (appendonly='true', blocksize='1048576')", "FOR VALUES FROM ('2024-01-01 00:00:00') TO ('2024-02-01 00:00:00')"},
		{"timestamp named", "PARTITION jan START ('2024-01-01 00:00:00'::timestamp without time zone) END ('2024-02-01 00:00:00'::timestamp without time zone) WITH (appendonly='true', blocksize='1048576')", "FOR VALUES FROM ('2024-01-01 00:00:00') TO ('2024-02-01 00:00:00')"},
		{"timestamptz named", "PARTITION jan START ('2024-01-01 00:00:00+08'::timestamp with time zone) END ('2024-02-01 00:00:00+08'::timestamp with time zone) WITH (appendonly='true', blocksize='1048576')", "FOR VALUES FROM ('2024-01-01 00:00:00+08') TO ('2024-02-01 00:00:00+08')"},
		{"list text", "PARTITION pa VALUES('beijing', 'shanghai') WITH (appendonly='true', blocksize='1048576')", "FOR VALUES IN ('beijing', 'shanghai')"},
		{"list text spaces", "PARTITION pa VALUES('New York', 'Los Angeles') WITH (appendonly='true', blocksize='1048576')", "FOR VALUES IN ('New York', 'Los Angeles')"},
		{"list int", "PARTITION active VALUES(1, 2, 3) WITH (appendonly='true', blocksize='1048576')", "FOR VALUES IN (1, 2, 3)"},
		{"list char", "PARTITION py VALUES('Y') WITH (appendonly='true', blocksize='1048576')", "FOR VALUES IN ('Y')"},
		{"default", "DEFAULT PARTITION pother  WITH (appendonly='true', blocksize='1048576')", "DEFAULT"},
	}
	for _, c := range cases {
		g6 := normalizeBoundary(c.gp6)
		g7 := normalizeBoundary(c.gp7)
		if g6 != g7 {
			t.Errorf("[%s] cross-version mismatch:\n  GP6  %q -> %q\n  GP7  %q -> %q", c.label, c.gp6, g6, c.gp7, g7)
		}
	}
}
