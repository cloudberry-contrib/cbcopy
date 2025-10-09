package builtin_test

import (
	"database/sql"
	"fmt"

	//"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/cloudberry-contrib/cbcopy/internal/testhelper"

	// "github.com/greenplum-db/gpbackup/backup"
	"github.com/cloudberry-contrib/cbcopy/meta/builtin"

	// "github.com/greenplum-db/gpbackup/testutils"
	"github.com/cloudberry-contrib/cbcopy/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/dependencies tests", func() {
	var (
		relation1 builtin.Relation
		relation2 builtin.Relation
		relation3 builtin.Relation
		depMap    map[builtin.UniqueID]map[builtin.UniqueID]bool
	)

	BeforeEach(func() {
		relation1 = builtin.Relation{Schema: "public", Name: "relation1", Oid: 1}
		relation2 = builtin.Relation{Schema: "public", Name: "relation2", Oid: 2}
		relation3 = builtin.Relation{Schema: "public", Name: "relation3", Oid: 3}
		depMap = make(map[builtin.UniqueID]map[builtin.UniqueID]bool)
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("TopologicalSort", func() {
		It("returns the original slice if there are no dependencies among objects", func() {
			relations := []builtin.Sortable{relation1, relation2, relation3}

			relations = builtin.TopologicalSort(relations, depMap)

			Expect(relations[0].FQN()).To(Equal("public.relation1"))
			Expect(relations[1].FQN()).To(Equal("public.relation2"))
			Expect(relations[2].FQN()).To(Equal("public.relation3"))
		})
		It("sorts the slice correctly if there is an object dependent on one other object", func() {
			depMap[builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: 1}] = map[builtin.UniqueID]bool{{ClassID: builtin.PG_CLASS_OID, Oid: 3}: true}
			relations := []builtin.Sortable{relation1, relation2, relation3}

			relations = builtin.TopologicalSort(relations, depMap)

			Expect(relations[0].FQN()).To(Equal("public.relation2"))
			Expect(relations[1].FQN()).To(Equal("public.relation3"))
			Expect(relations[2].FQN()).To(Equal("public.relation1"))
		})
		It("sorts the slice correctly if there are two objects dependent on one other object", func() {
			depMap[builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: 1}] = map[builtin.UniqueID]bool{{ClassID: builtin.PG_CLASS_OID, Oid: 2}: true}
			depMap[builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: 3}] = map[builtin.UniqueID]bool{{ClassID: builtin.PG_CLASS_OID, Oid: 2}: true}
			relations := []builtin.Sortable{relation1, relation2, relation3}

			relations = builtin.TopologicalSort(relations, depMap)

			Expect(relations[0].FQN()).To(Equal("public.relation2"))
			Expect(relations[1].FQN()).To(Equal("public.relation1"))
			Expect(relations[2].FQN()).To(Equal("public.relation3"))
		})
		It("sorts the slice correctly if there is one object dependent on two other objects", func() {
			depMap[builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: 2}] = map[builtin.UniqueID]bool{{ClassID: builtin.PG_CLASS_OID, Oid: 1}: true, {ClassID: builtin.PG_CLASS_OID, Oid: 1}: true}
			relations := []builtin.Sortable{relation1, relation2, relation3}

			relations = builtin.TopologicalSort(relations, depMap)

			Expect(relations[0].FQN()).To(Equal("public.relation1"))
			Expect(relations[1].FQN()).To(Equal("public.relation3"))
			Expect(relations[2].FQN()).To(Equal("public.relation2"))
		})
		It("aborts if dependency loop (this shouldn't be possible)", func() {
			depMap[builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: 1}] = map[builtin.UniqueID]bool{{ClassID: builtin.PG_CLASS_OID, Oid: 3}: true}
			depMap[builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: 2}] = map[builtin.UniqueID]bool{{ClassID: builtin.PG_CLASS_OID, Oid: 1}: true}
			depMap[builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: 3}] = map[builtin.UniqueID]bool{{ClassID: builtin.PG_CLASS_OID, Oid: 2}: true}

			sortable := []builtin.Sortable{relation1, relation2, relation3}
			defer func() {
				testhelper.ExpectRegexp(logfile, "Object: public.relation1 {ClassID:1259 Oid:1}")
				testhelper.ExpectRegexp(logfile, "Dependencies:")
				testhelper.ExpectRegexp(logfile, "\tpublic.relation3 {ClassID:1259 Oid:3}")
				testhelper.ExpectRegexp(logfile, "Object: public.relation2 {ClassID:1259 Oid:2}")
				testhelper.ExpectRegexp(logfile, "Dependencies:")
				testhelper.ExpectRegexp(logfile, "\tpublic.relation1 {ClassID:1259 Oid:1}")
				testhelper.ExpectRegexp(logfile, "Object: public.relation3 {ClassID:1259 Oid:3}")
				testhelper.ExpectRegexp(logfile, "Dependencies:")
				testhelper.ExpectRegexp(logfile, "\tpublic.relation2 {ClassID:1259 Oid:2}")
			}()
			defer testhelper.ShouldPanicWithMessage("Dependency resolution failed; see log file gbytes.Buffer for details. This is a bug, please report.")
			sortable = builtin.TopologicalSort(sortable, depMap)
		})
		It("aborts if dependencies are not met", func() {
			depMap[builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: 1}] = map[builtin.UniqueID]bool{{ClassID: builtin.PG_CLASS_OID, Oid: 2}: true}
			depMap[builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: 1}] = map[builtin.UniqueID]bool{{ClassID: builtin.PG_CLASS_OID, Oid: 4}: true}
			sortable := []builtin.Sortable{relation1, relation2}

			defer testhelper.ShouldPanicWithMessage("Dependency resolution failed; see log file gbytes.Buffer for details. This is a bug, please report.")
			sortable = builtin.TopologicalSort(sortable, depMap)
		})
	})
	Describe("PrintDependentObjectStatements", func() {
		var (
			objects             []builtin.Sortable
			metadataMap         builtin.MetadataMap
			funcInfoMap         map[uint32]builtin.FunctionInfo
			plannerSupportValue string
			parallelValue       string
			default_parallel    string
		)
		BeforeEach(func() {
			plannerSupportValue = ""
			parallelValue = ""
			default_parallel = ""
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDBFamily() {
				plannerSupportValue = "-"
				parallelValue = "u"
				default_parallel = " PARALLEL UNSAFE"
			}
			funcInfoMap = map[uint32]builtin.FunctionInfo{
				1: {QualifiedName: "public.write_to_s3", Arguments: sql.NullString{String: "", Valid: true}, IsInternal: false},
				2: {QualifiedName: "public.read_from_s3", Arguments: sql.NullString{String: "", Valid: true}, IsInternal: false},
			}
			objects = []builtin.Sortable{
				builtin.Function{Oid: 1, Schema: "public", Name: "function", FunctionBody: "SELECT $1 + $2",
					Arguments:  sql.NullString{String: "integer, integer", Valid: true},
					IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
					ResultType: sql.NullString{String: "integer", Valid: true},
					Language:   "sql", PlannerSupport: plannerSupportValue, Parallel: parallelValue},
				builtin.BaseType{Oid: 2, Schema: "public", Name: "base", Input: "typin", Output: "typout", Category: "U"},
				builtin.CompositeType{Oid: 3, Schema: "public", Name: "composite", Attributes: []builtin.Attribute{{Name: "foo", Type: "integer"}}},
				builtin.Domain{Oid: 4, Schema: "public", Name: "domain", BaseType: "numeric"},
				builtin.Table{
					Relation:        builtin.Relation{Oid: 5, Schema: "public", Name: "relation"},
					TableDefinition: builtin.TableDefinition{DistPolicy: "DISTRIBUTED RANDOMLY", ColumnDefs: []builtin.ColumnDefinition{}},
				},
				builtin.ExternalProtocol{Oid: 6, Name: "ext_protocol", Trusted: true, ReadFunction: 2, WriteFunction: 1, Validator: 0},
				builtin.RangeType{Oid: 7, Schema: "public", Name: "rangetype1"},
			}
			metadataMap = builtin.MetadataMap{
				builtin.UniqueID{ClassID: builtin.PG_PROC_OID, Oid: 1}:        builtin.ObjectMetadata{Comment: "function"},
				builtin.UniqueID{ClassID: builtin.PG_TYPE_OID, Oid: 2}:        builtin.ObjectMetadata{Comment: "base type"},
				builtin.UniqueID{ClassID: builtin.PG_TYPE_OID, Oid: 3}:        builtin.ObjectMetadata{Comment: "composite type"},
				builtin.UniqueID{ClassID: builtin.PG_TYPE_OID, Oid: 4}:        builtin.ObjectMetadata{Comment: "domain"},
				builtin.UniqueID{ClassID: builtin.PG_CLASS_OID, Oid: 5}:       builtin.ObjectMetadata{Comment: "relation"},
				builtin.UniqueID{ClassID: builtin.PG_EXTPROTOCOL_OID, Oid: 6}: builtin.ObjectMetadata{Comment: "protocol"},
				builtin.UniqueID{ClassID: builtin.PG_TYPE_OID, Oid: 7}:        builtin.ObjectMetadata{Comment: "range type"},
			}
		})
		It("prints create statements for dependent types, functions, protocols, and tables (domain has a constraint)", func() {
			constraints := []builtin.Constraint{
				{Name: "check_constraint", Def: sql.NullString{String: "CHECK (VALUE > 2)", Valid: true}, OwningObject: "public.domain"},
			}
			builtin.PrintDependentObjectStatements(backupfile, tocfile, objects, metadataMap, constraints, funcInfoMap)
			testhelper.ExpectRegexp(buffer, fmt.Sprintf(`
CREATE FUNCTION public.function(integer, integer) RETURNS integer AS
$_$SELECT $1 + $2$_$
LANGUAGE sql%s;


COMMENT ON FUNCTION public.function(integer, integer) IS 'function';


CREATE TYPE public.base (
	INPUT = typin,
	OUTPUT = typout
);


COMMENT ON TYPE public.base IS 'base type';


CREATE TYPE public.composite AS (
	foo integer
);

COMMENT ON TYPE public.composite IS 'composite type';

CREATE DOMAIN public.domain AS numeric
	CONSTRAINT check_constraint CHECK (VALUE > 2);


COMMENT ON DOMAIN public.domain IS 'domain';


CREATE TABLE public.relation (
) DISTRIBUTED RANDOMLY;


COMMENT ON TABLE public.relation IS 'relation';


CREATE TRUSTED PROTOCOL ext_protocol (readfunc = public.read_from_s3, writefunc = public.write_to_s3);


COMMENT ON PROTOCOL ext_protocol IS 'protocol';
`, default_parallel))
		})
		It("prints create statements for dependent types, functions, protocols, and tables (no domain constraint)", func() {
			constraints := make([]builtin.Constraint, 0)
			builtin.PrintDependentObjectStatements(backupfile, tocfile, objects, metadataMap, constraints, funcInfoMap)
			testhelper.ExpectRegexp(buffer, fmt.Sprintf(`
CREATE FUNCTION public.function(integer, integer) RETURNS integer AS
$_$SELECT $1 + $2$_$
LANGUAGE sql%s;


COMMENT ON FUNCTION public.function(integer, integer) IS 'function';


CREATE TYPE public.base (
	INPUT = typin,
	OUTPUT = typout
);


COMMENT ON TYPE public.base IS 'base type';


CREATE TYPE public.composite AS (
	foo integer
);

COMMENT ON TYPE public.composite IS 'composite type';

CREATE DOMAIN public.domain AS numeric;


COMMENT ON DOMAIN public.domain IS 'domain';


CREATE TABLE public.relation (
) DISTRIBUTED RANDOMLY;


COMMENT ON TABLE public.relation IS 'relation';


CREATE TRUSTED PROTOCOL ext_protocol (readfunc = public.read_from_s3, writefunc = public.write_to_s3);


COMMENT ON PROTOCOL ext_protocol IS 'protocol';
`, default_parallel))
		})
	})
})
