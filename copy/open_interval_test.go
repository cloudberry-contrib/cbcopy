package copy

import (
	"fmt"
	"testing"
)

func TestNormalizeBoundary_GP6_EndOnly(t *testing.T) {
	// GP6 首分区：只有 END
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"END only date", `PARTITION p_first  END ('2024-04-01'::date) WITH (appendonly='true', blocksize='1048576')`, `RANGE:[MINVALUE,2024-04-01)`},
		{"END only int", `PARTITION p1 END (100) WITH (appendonly='true')`, `RANGE:[MINVALUE,100)`},
		{"END only timestamp", `PARTITION p_first END ('2024-01-01 00:00:00'::timestamp without time zone) WITH (appendonly='true')`, `RANGE:[MINVALUE,2024-01-01 00:00:00)`},
		{"END only INCLUSIVE", `PARTITION p1 END (100) INCLUSIVE WITH (appendonly='true')`, `RANGE:[MINVALUE,100]`},
	}
	for _, c := range cases {
		got := normalizeBoundary(c.input)
		if got != c.want {
			t.Errorf("%s:\n  input: %s\n  got:   %s\n  want:  %s", c.name, c.input, got, c.want)
		}
	}
}

func TestNormalizeBoundary_GP6_StartOnly(t *testing.T) {
	// GP6 尾分区：只有 START
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"START only date", `PARTITION p_last START ('2024-07-01'::date) WITH (appendonly='true', blocksize='1048576')`, `RANGE:[2024-07-01,MAXVALUE)`},
		{"START only int", `PARTITION pn START (900) WITH (appendonly='true')`, `RANGE:[900,MAXVALUE)`},
		{"START only timestamp", `PARTITION p_last START ('2024-12-01 00:00:00'::timestamp without time zone) WITH (appendonly='true')`, `RANGE:[2024-12-01 00:00:00,MAXVALUE)`},
		{"START only EXCLUSIVE", `PARTITION p1 START (100) EXCLUSIVE WITH (appendonly='true')`, `RANGE:(100,MAXVALUE)`},
	}
	for _, c := range cases {
		got := normalizeBoundary(c.input)
		if got != c.want {
			t.Errorf("%s:\n  input: %s\n  got:   %s\n  want:  %s", c.name, c.input, got, c.want)
		}
	}
}

func TestNormalizeBoundary_CrossVersion_OpenInterval(t *testing.T) {
	// 跨版本配对：GP6 开区间 ↔ CBDB MINVALUE/MAXVALUE
	pairs := []struct {
		name string
		gp6  string
		cbdb string
	}{
		{"首分区", `PARTITION p_first  END ('2024-04-01'::date) WITH (appendonly='true', blocksize='1048576')`, `FOR VALUES FROM (MINVALUE) TO ('2024-04-01')`},
		{"尾分区", `PARTITION p_last START ('2024-07-01'::date) WITH (appendonly='true', blocksize='1048576')`, `FOR VALUES FROM ('2024-07-01') TO (MAXVALUE)`},
		{"中间分区", `PARTITION p_mid START ('2024-04-01'::date) END ('2024-07-01'::date) WITH (appendonly='true', blocksize='1048576')`, `FOR VALUES FROM ('2024-04-01') TO ('2024-07-01')`},
	}
	for _, p := range pairs {
		gp6Norm := normalizeBoundary(p.gp6)
		cbdbNorm := normalizeBoundary(p.cbdb)
		match := gp6Norm == cbdbNorm && gp6Norm != "" && gp6Norm != p.gp6
		fmt.Printf("%-10s GP6=%q  CBDB=%q  match=%v\n", p.name, gp6Norm, cbdbNorm, match)
		if !match {
			t.Errorf("%s: cross-version mismatch\n  GP6:  %s\n  CBDB: %s", p.name, gp6Norm, cbdbNorm)
		}
	}
}

func TestNormalizeBoundary_GP6_ClosedRange_StillWorks(t *testing.T) {
	// 确认闭区间没被影响
	cases := []struct {
		input string
		want  string
	}{
		{`START ('2024-01-01'::date) END ('2024-04-01'::date) EVERY ('1 mon'::interval) WITH (appendonly='true')`, `RANGE:[2024-01-01,2024-04-01)`},
		{`PARTITION p2 START ('2024-04-01'::date) END ('2024-07-01'::date) WITH (appendonly='true')`, `RANGE:[2024-04-01,2024-07-01)`},
	}
	for _, c := range cases {
		got := normalizeBoundary(c.input)
		if got != c.want {
			t.Errorf("closed range regression:\n  input: %s\n  got:   %s\n  want:  %s", c.input, got, c.want)
		}
	}
}
