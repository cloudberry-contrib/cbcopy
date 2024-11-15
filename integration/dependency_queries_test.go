package integration

import (
	// "github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/cloudberrydb/cbcopy/internal/testhelper"

	// "github.com/greenplum-db/gpbackup/backup"
	"github.com/cloudberrydb/cbcopy/meta/builtin"

	// "github.com/greenplum-db/gpbackup/testutils"
	"github.com/cloudberrydb/cbcopy/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("cbcopy integration tests", func() {
	Describe("GetDependencies", func() {
		It("correctly constructs table inheritance dependencies", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foo(i int, j text, k bool)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.foo")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.bar(m int) inherits (public.foo)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.bar")

			oidFoo := testutils.OidFromObjectName(connectionPool, "public", "foo", builtin.TYPE_RELATION)
			oidBar := testutils.OidFromObjectName(connectionPool, "public", "bar", builtin.TYPE_RELATION)
			fooEntry := builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: oidFoo}
			barEntry := builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: oidBar}
			backupSet := map[builtin.UniqueID]bool{fooEntry: true, barEntry: true}
			tables := make([]builtin.Table, 0)

			deps := builtin.GetDependencies(connectionPool, backupSet, tables)

			Expect(deps).To(HaveLen(1))
			Expect(deps[barEntry]).To(HaveLen(1))
			Expect(deps[barEntry]).To(HaveKey(fooEntry))
		})
		It("constructs dependencies correctly for a table dependent on a protocol", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION public.read_from_s3() RETURNS integer
		AS '$libdir/gps3ext.so', 's3_import'
		LANGUAGE c STABLE NO SQL;`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.read_from_s3()")
			testhelper.AssertQueryRuns(connectionPool, `CREATE PROTOCOL s3 (readfunc = public.read_from_s3);`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP PROTOCOL s3")
			testhelper.AssertQueryRuns(connectionPool, `CREATE EXTERNAL TABLE public.ext_tbl (
		i int
		) LOCATION (
		's3://192.168.0.1'
		)
		FORMAT 'csv' (delimiter E',' null E'' escape E'"' quote E'"')
		ENCODING 'UTF8';`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EXTERNAL TABLE public.ext_tbl")

			tableOid := testutils.OidFromObjectName(connectionPool, "public", "ext_tbl", builtin.TYPE_RELATION)
			protocolOid := testutils.OidFromObjectName(connectionPool, "", "s3", builtin.TYPE_PROTOCOL)
			functionOid := testutils.OidFromObjectName(connectionPool, "public", "read_from_s3", builtin.TYPE_FUNCTION)

			tableEntry := builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: tableOid}
			protocolEntry := builtin.UniqueID{ClassID: builtin.PG_EXTPROTOCOL_OID, Oid: protocolOid}
			functionEntry := builtin.UniqueID{ClassID: builtin.PG_PROC_OID, Oid: functionOid}
			backupSet := map[builtin.UniqueID]bool{tableEntry: true, protocolEntry: true, functionEntry: true}
			tables := make([]builtin.Table, 0)

			deps := builtin.GetDependencies(connectionPool, backupSet, tables)
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Is("4") {
				tableRelations := builtin.GetIncludedUserTableRelations(connectionPool, []string{})
				tables := builtin.ConstructDefinitionsForTables(connectionPool, tableRelations)
				protocols := builtin.GetExternalProtocols(connectionPool)
				builtin.AddProtocolDependenciesForGPDB4(deps, tables, protocols)
			}

			Expect(deps).To(HaveLen(2))
			Expect(deps[tableEntry]).To(HaveLen(1))
			Expect(deps[tableEntry]).To(HaveKey(protocolEntry))
			Expect(deps[protocolEntry]).To(HaveLen(1))
			Expect(deps[protocolEntry]).To(HaveKey(functionEntry))
		})
		It("constructs dependencies correctly for a view that depends on two other views", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE VIEW public.parent1 AS SELECT relname FROM pg_class")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP VIEW public.parent1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE VIEW public.parent2 AS SELECT relname FROM pg_class")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP VIEW public.parent2")
			testhelper.AssertQueryRuns(connectionPool, "CREATE VIEW public.child AS (SELECT * FROM public.parent1 UNION SELECT * FROM public.parent2)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP VIEW public.child")

			parent1Oid := testutils.OidFromObjectName(connectionPool, "public", "parent1", builtin.TYPE_RELATION)
			parent2Oid := testutils.OidFromObjectName(connectionPool, "public", "parent2", builtin.TYPE_RELATION)
			childOid := testutils.OidFromObjectName(connectionPool, "public", "child", builtin.TYPE_RELATION)

			parent1Entry := builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: parent1Oid}
			parent2Entry := builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: parent2Oid}
			childEntry := builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: childOid}
			backupSet := map[builtin.UniqueID]bool{parent1Entry: true, parent2Entry: true, childEntry: true}
			tables := make([]builtin.Table, 0)

			deps := builtin.GetDependencies(connectionPool, backupSet, tables)

			Expect(deps).To(HaveLen(1))
			Expect(deps[childEntry]).To(HaveLen(2))
			Expect(deps[childEntry]).To(HaveKey(parent1Entry))
			Expect(deps[childEntry]).To(HaveKey(parent2Entry))
		})
		It("constructs dependencies correctly for a materialized view that depends on two other materialized views", func() {
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("6.2") {
				Skip("Test only applicable to GPDB 6.2 and above")
			}
			testhelper.AssertQueryRuns(connectionPool, "CREATE MATERIALIZED VIEW public.parent1 AS SELECT relname FROM pg_class")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP MATERIALIZED VIEW public.parent1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE MATERIALIZED VIEW public.parent2 AS SELECT relname FROM pg_class")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP MATERIALIZED VIEW public.parent2")
			testhelper.AssertQueryRuns(connectionPool, "CREATE MATERIALIZED VIEW public.child AS (SELECT * FROM public.parent1 UNION SELECT * FROM public.parent2)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP MATERIALIZED VIEW public.child")

			parent1Oid := testutils.OidFromObjectName(connectionPool, "public", "parent1", builtin.TYPE_RELATION)
			parent2Oid := testutils.OidFromObjectName(connectionPool, "public", "parent2", builtin.TYPE_RELATION)
			childOid := testutils.OidFromObjectName(connectionPool, "public", "child", builtin.TYPE_RELATION)

			parent1Entry := builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: parent1Oid}
			parent2Entry := builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: parent2Oid}
			childEntry := builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: childOid}
			backupSet := map[builtin.UniqueID]bool{parent1Entry: true, parent2Entry: true, childEntry: true}
			tables := make([]builtin.Table, 0)

			deps := builtin.GetDependencies(connectionPool, backupSet, tables)

			Expect(deps).To(HaveLen(1))
			Expect(deps[childEntry]).To(HaveLen(2))
			Expect(deps[childEntry]).To(HaveKey(parent1Entry))
			Expect(deps[childEntry]).To(HaveKey(parent2Entry))
		})
		It("constructs dependencies correctly for a view dependent on text search objects", func() {
			testutils.SkipIfBefore5(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH PARSER public.testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH PARSER public.testparser;")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH CONFIGURATION public.testconfig(PARSER = public.testparser);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH CONFIGURATION public.testconfig;")
			testhelper.AssertQueryRuns(connectionPool, `CREATE VIEW public.ts_config_view AS SELECT * FROM ts_debug('public.testconfig',
'PostgreSQL, the highly scalable, SQL compliant, open source
object-relational database management system');`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP VIEW public.ts_config_view;")

			parserID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testparser", builtin.TYPE_TSPARSER)
			configID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testconfig", builtin.TYPE_TSCONFIGURATION)
			viewID := testutils.UniqueIDFromObjectName(connectionPool, "public", "ts_config_view", builtin.TYPE_RELATION)
			backupSet := map[builtin.UniqueID]bool{parserID: true, configID: true, viewID: true}
			tables := make([]builtin.Table, 0)

			deps := builtin.GetDependencies(connectionPool, backupSet, tables)
			Expect(deps).To(HaveLen(2))
			Expect(deps[configID]).To(HaveLen(1))
			Expect(deps[configID]).To(HaveKey(parserID))
			Expect(deps[viewID]).To(HaveLen(1))
			Expect(deps[viewID]).To(HaveKey(configID))
		})
		Describe("function dependencies", func() {
			var compositeEntry builtin.UniqueID
			BeforeEach(func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.composite_ints AS (one integer, two integer)")
				compositeOid := testutils.OidFromObjectName(connectionPool, "public", "composite_ints", builtin.TYPE_TYPE)
				compositeEntry = builtin.UniqueID{ClassID: builtin.PG_TYPE_OID, Oid: compositeOid}
			})
			AfterEach(func() {
				testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.composite_ints CASCADE")
			})
			It("constructs dependencies correctly for a function dependent on a user-defined type in the arguments", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.add(public.composite_ints) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT ($1.one + $1.two);'")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.add(public.composite_ints)")

				functionOid := testutils.OidFromObjectName(connectionPool, "public", "add", builtin.TYPE_FUNCTION)
				funcEntry := builtin.UniqueID{ClassID: builtin.PG_PROC_OID, Oid: functionOid}
				backupSet := map[builtin.UniqueID]bool{funcEntry: true, compositeEntry: true}
				tables := make([]builtin.Table, 0)

				functionDeps := builtin.GetDependencies(connectionPool, backupSet, tables)

				Expect(functionDeps).To(HaveLen(1))
				Expect(functionDeps[funcEntry]).To(HaveLen(1))
				Expect(functionDeps[funcEntry]).To(HaveKey(compositeEntry))
			})
			It("constructs dependencies correctly for a function dependent on a user-defined type in the return type", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.compose(integer, integer) RETURNS public.composite_ints STRICT IMMUTABLE LANGUAGE PLPGSQL AS 'DECLARE comp public.composite_ints; BEGIN SELECT $1, $2 INTO comp; RETURN comp; END;';")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.compose(integer, integer)")

				functionOid := testutils.OidFromObjectName(connectionPool, "public", "compose", builtin.TYPE_FUNCTION)
				funcEntry := builtin.UniqueID{ClassID: builtin.PG_PROC_OID, Oid: functionOid}
				backupSet := map[builtin.UniqueID]bool{funcEntry: true, compositeEntry: true}
				tables := make([]builtin.Table, 0)

				functionDeps := builtin.GetDependencies(connectionPool, backupSet, tables)

				Expect(functionDeps).To(HaveLen(1))
				Expect(functionDeps[funcEntry]).To(HaveLen(1))
				Expect(functionDeps[funcEntry]).To(HaveKey(compositeEntry))
			})
			It("constructs dependencies correctly for a function dependent on an implicit array type", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.base_type")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.base_type CASCADE")
				testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.base_fn_in(cstring) RETURNS public.base_type AS 'boolin' LANGUAGE internal")
				testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.base_fn_out(public.base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
				testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.base_type(INPUT=public.base_fn_in, OUTPUT=public.base_fn_out)")
				testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.compose(public.base_type[], public.composite_ints) RETURNS public.composite_ints STRICT IMMUTABLE LANGUAGE PLPGSQL AS 'DECLARE comp public.composite_ints; BEGIN SELECT $1[0].one+$2.one, $1[0].two+$2.two INTO comp; RETURN comp; END;';")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.compose(public.base_type[], public.composite_ints)")

				functionOid := testutils.OidFromObjectName(connectionPool, "public", "compose", builtin.TYPE_FUNCTION)
				funcEntry := builtin.UniqueID{ClassID: builtin.PG_PROC_OID, Oid: functionOid}
				baseOid := testutils.OidFromObjectName(connectionPool, "public", "base_type", builtin.TYPE_TYPE)
				baseEntry := builtin.UniqueID{ClassID: builtin.PG_TYPE_OID, Oid: baseOid}
				backupSet := map[builtin.UniqueID]bool{funcEntry: true, compositeEntry: true, baseEntry: true}
				tables := make([]builtin.Table, 0)

				functionDeps := builtin.GetDependencies(connectionPool, backupSet, tables)

				Expect(functionDeps).To(HaveLen(1))
				Expect(functionDeps[funcEntry]).To(HaveLen(2))
				Expect(functionDeps[funcEntry]).To(HaveKey(compositeEntry))
				Expect(functionDeps[funcEntry]).To(HaveKey(baseEntry))
			})
		})
		Describe("type dependencies", func() {
			var (
				baseOid   uint32
				baseEntry builtin.UniqueID
			)
			BeforeEach(func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.base_type")
				testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.base_fn_in(cstring) RETURNS public.base_type AS 'boolin' LANGUAGE internal")
				testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.base_fn_out(public.base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
				testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.base_type(INPUT=public.base_fn_in, OUTPUT=public.base_fn_out)")

				baseOid = testutils.OidFromObjectName(connectionPool, "public", "base_type", builtin.TYPE_TYPE)
				baseEntry = builtin.UniqueID{ClassID: builtin.PG_TYPE_OID, Oid: baseOid}
			})
			AfterEach(func() {
				testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.base_type CASCADE")
			})
			It("constructs domain dependencies on user-defined types", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE DOMAIN public.parent_domain AS integer")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP DOMAIN public.parent_domain")
				testhelper.AssertQueryRuns(connectionPool, "CREATE DOMAIN public.domain_type AS public.parent_domain")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP DOMAIN public.domain_type")

				domainOid := testutils.OidFromObjectName(connectionPool, "public", "parent_domain", builtin.TYPE_TYPE)
				domain2Oid := testutils.OidFromObjectName(connectionPool, "public", "domain_type", builtin.TYPE_TYPE)

				domainEntry := builtin.UniqueID{ClassID: builtin.PG_TYPE_OID, Oid: domainOid}
				domain2Entry := builtin.UniqueID{ClassID: builtin.PG_TYPE_OID, Oid: domain2Oid}
				backupSet := map[builtin.UniqueID]bool{domainEntry: true, domain2Entry: true}
				tables := make([]builtin.Table, 0)

				deps := builtin.GetDependencies(connectionPool, backupSet, tables)

				Expect(deps).To(HaveLen(1))
				Expect(deps[domain2Entry]).To(HaveLen(1))
				Expect(deps[domain2Entry]).To(HaveKey(domainEntry))
			})

			It("constructs dependencies correctly for a function/base type dependency loop", func() {
				baseInOid := testutils.OidFromObjectName(connectionPool, "public", "base_fn_in", builtin.TYPE_FUNCTION)
				baseOutOid := testutils.OidFromObjectName(connectionPool, "public", "base_fn_out", builtin.TYPE_FUNCTION)

				baseInEntry := builtin.UniqueID{ClassID: builtin.PG_PROC_OID, Oid: baseInOid}
				baseOutEntry := builtin.UniqueID{ClassID: builtin.PG_PROC_OID, Oid: baseOutOid}
				backupSet := map[builtin.UniqueID]bool{baseEntry: true, baseInEntry: true, baseOutEntry: true}
				tables := make([]builtin.Table, 0)

				deps := builtin.GetDependencies(connectionPool, backupSet, tables)

				Expect(deps).To(HaveLen(1))
				Expect(deps[baseEntry]).To(HaveLen(2))
				Expect(deps[baseEntry]).To(HaveKey(baseInEntry))
				Expect(deps[baseEntry]).To(HaveKey(baseOutEntry))
			})
			It("constructs dependencies correctly for a composite type dependent on one user-defined type", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.comp_type AS (base public.base_type, builtin integer)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.comp_type")

				compositeOid := testutils.OidFromObjectName(connectionPool, "public", "comp_type", builtin.TYPE_TYPE)
				compositeEntry := builtin.UniqueID{ClassID: builtin.PG_TYPE_OID, Oid: compositeOid}
				backupSet := map[builtin.UniqueID]bool{baseEntry: true, compositeEntry: true}
				tables := make([]builtin.Table, 0)

				deps := builtin.GetDependencies(connectionPool, backupSet, tables)

				Expect(deps).To(HaveLen(1))
				Expect(deps[compositeEntry]).To(HaveLen(1))
				Expect(deps[compositeEntry]).To(HaveKey(baseEntry))
			})
			It("constructs dependencies correctly for a composite type dependent on multiple user-defined types", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.base_type2")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.base_type2 CASCADE")
				testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.base_fn_in2(cstring) RETURNS public.base_type2 AS 'boolin' LANGUAGE internal")
				testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.base_fn_out2(public.base_type2) RETURNS cstring AS 'boolout' LANGUAGE internal")
				testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.base_type2(INPUT=public.base_fn_in2, OUTPUT=public.base_fn_out2)")

				testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.comp_type AS (base public.base_type, base2 public.base_type2)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.comp_type")

				base2Oid := testutils.OidFromObjectName(connectionPool, "public", "base_type2", builtin.TYPE_TYPE)
				base2Entry := builtin.UniqueID{ClassID: builtin.PG_TYPE_OID, Oid: base2Oid}
				compositeOid := testutils.OidFromObjectName(connectionPool, "public", "comp_type", builtin.TYPE_TYPE)
				compositeEntry := builtin.UniqueID{ClassID: builtin.PG_TYPE_OID, Oid: compositeOid}
				backupSet := map[builtin.UniqueID]bool{baseEntry: true, base2Entry: true, compositeEntry: true}
				tables := make([]builtin.Table, 0)

				deps := builtin.GetDependencies(connectionPool, backupSet, tables)

				Expect(deps).To(HaveLen(1))
				Expect(deps[compositeEntry]).To(HaveLen(2))
				Expect(deps[compositeEntry]).To(HaveKey(baseEntry))
				Expect(deps[compositeEntry]).To(HaveKey(base2Entry))
			})
			It("constructs dependencies correctly for a composite type dependent on the same user-defined type multiple times", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.comp_type AS (base public.base_type, base2 public.base_type)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.comp_type")

				compositeOid := testutils.OidFromObjectName(connectionPool, "public", "comp_type", builtin.TYPE_TYPE)
				compositeEntry := builtin.UniqueID{ClassID: builtin.PG_TYPE_OID, Oid: compositeOid}
				backupSet := map[builtin.UniqueID]bool{baseEntry: true, compositeEntry: true}
				tables := make([]builtin.Table, 0)

				deps := builtin.GetDependencies(connectionPool, backupSet, tables)

				Expect(deps).To(HaveLen(1))
				Expect(deps[compositeEntry]).To(HaveLen(1))
				Expect(deps[compositeEntry]).To(HaveKey(baseEntry))
			})
			It("constructs dependencies correctly for a composite type dependent on a table", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.my_table(i int)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.my_table")
				testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.my_type AS (type1 public.my_table)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.my_type")

				tableOid := testutils.OidFromObjectName(connectionPool, "public", "my_table", builtin.TYPE_RELATION)
				typeOid := testutils.OidFromObjectName(connectionPool, "public", "my_type", builtin.TYPE_TYPE)

				tableEntry := builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: tableOid}
				typeEntry := builtin.UniqueID{ClassID: builtin.PG_TYPE_OID, Oid: typeOid}
				backupSet := map[builtin.UniqueID]bool{tableEntry: true, typeEntry: true}
				tables := make([]builtin.Table, 0)

				deps := builtin.GetDependencies(connectionPool, backupSet, tables)

				Expect(deps).To(HaveLen(1))
				Expect(deps[typeEntry]).To(HaveLen(1))
				Expect(deps[typeEntry]).To(HaveKey(tableEntry))
			})
		})
	})
})
