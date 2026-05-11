package option

import "testing"

func newOptionForTest() *Option {
	return &Option{
		destDbInventory: make(map[string]map[string]struct{}),
		destDbRootParts: make(map[string]map[string]struct{}),
	}
}

func TestMarkDestTablesPersistsInventory(t *testing.T) {
	o := newOptionForTest()

	userTables := map[string]TableStatistics{
		"public.t1": {RelTuples: 100},
		"sales.q1":  {RelTuples: 200},
	}
	partTables := map[string]bool{
		"public.partroot": true,
	}

	o.MarkDestTables("dst", userTables, partTables)

	if !o.IsDestTableExisting("dst", "public", "t1") {
		t.Fatalf("expected public.t1 to be reported as existing")
	}
	if !o.IsDestTableExisting("dst", "sales", "q1") {
		t.Fatalf("expected sales.q1 to be reported as existing")
	}
	if !o.IsDestTableExisting("dst", "public", "partroot") {
		t.Fatalf("expected root partition public.partroot to be reported as existing")
	}
}

func TestIsDestTableExistingMisses(t *testing.T) {
	o := newOptionForTest()
	o.MarkDestTables("dst", map[string]TableStatistics{
		"public.t1": {},
	}, map[string]bool{})

	cases := []struct {
		name           string
		db, schema, tn string
	}{
		{"unknown table", "dst", "public", "missing"},
		{"unknown schema", "dst", "other", "t1"},
		{"unknown db", "other_dst", "public", "t1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if o.IsDestTableExisting(tc.db, tc.schema, tc.tn) {
				t.Fatalf("expected %s.%s on db %s to be reported as not existing", tc.schema, tc.tn, tc.db)
			}
		})
	}
}

func TestIsDestTableExistingNoInventoryYet(t *testing.T) {
	// Brand-new Option, MarkDestTables not yet called: every query should miss.
	o := newOptionForTest()
	if o.IsDestTableExisting("any", "any", "any") {
		t.Fatalf("expected no hits on an Option whose inventory was never populated")
	}
}

func TestTranslateToDestFQNNoMapping(t *testing.T) {
	o := newOptionForTest()
	// sourceSchemas/destSchemas are nil → GetSchemaMap returns empty map → identity.
	schema, name := o.TranslateToDestFQN("public", "t1")
	if schema != "public" || name != "t1" {
		t.Fatalf("expected (public, t1), got (%s, %s)", schema, name)
	}
}

func TestTranslateToDestFQNWithMapping(t *testing.T) {
	o := newOptionForTest()
	o.sourceSchemas = []*DbSchema{
		{Database: "srcdb", Schema: "src_a"},
		{Database: "srcdb", Schema: "src_b"},
	}
	o.destSchemas = []*DbSchema{
		{Database: "dstdb", Schema: "dst_a"},
		{Database: "dstdb", Schema: "dst_b"},
	}

	cases := []struct {
		srcSchema, srcName     string
		wantSchema, wantName   string
	}{
		{"src_a", "t1", "dst_a", "t1"},
		{"src_b", "q2", "dst_b", "q2"},
		{"unmapped", "t3", "unmapped", "t3"},
	}
	for _, tc := range cases {
		t.Run(tc.srcSchema, func(t *testing.T) {
			gotSchema, gotName := o.TranslateToDestFQN(tc.srcSchema, tc.srcName)
			if gotSchema != tc.wantSchema || gotName != tc.wantName {
				t.Fatalf("expected (%s, %s), got (%s, %s)", tc.wantSchema, tc.wantName, gotSchema, gotName)
			}
		})
	}
}

func TestIsDestTableExistingPerDbIsolation(t *testing.T) {
	o := newOptionForTest()
	o.MarkDestTables("db_a", map[string]TableStatistics{
		"public.only_in_a": {},
	}, map[string]bool{})
	o.MarkDestTables("db_b", map[string]TableStatistics{
		"public.only_in_b": {},
	}, map[string]bool{})

	if !o.IsDestTableExisting("db_a", "public", "only_in_a") {
		t.Fatalf("expected db_a.public.only_in_a to exist")
	}
	if o.IsDestTableExisting("db_a", "public", "only_in_b") {
		t.Fatalf("expected db_a.public.only_in_b to NOT exist (it is on db_b)")
	}
	if !o.IsDestTableExisting("db_b", "public", "only_in_b") {
		t.Fatalf("expected db_b.public.only_in_b to exist")
	}
}
