package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	// "github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/cloudberrydb/cbcopy/internal/testhelper"

	// "github.com/greenplum-db/gpbackup/backup"
	"github.com/cloudberrydb/cbcopy/meta/builtin"

	// "github.com/greenplum-db/gpbackup/testutils"
	"github.com/cloudberrydb/cbcopy/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("cbcopy integration create statement tests", func() {
	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintCreateOperatorStatement", func() {
		It("creates operator", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")

			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION testschema.\"testFunc\" (path,path) RETURNS path AS 'SELECT $1' LANGUAGE SQL IMMUTABLE")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION testschema.\"testFunc\" (path,path)")

			operator := builtin.Operator{Oid: 0, Schema: "testschema", Name: "##", Procedure: "testschema.\"testFunc\"", LeftArgType: "path", RightArgType: "path", CommutatorOp: "0", NegatorOp: "0", RestrictFunction: "-", JoinFunction: "-", CanHash: false, CanMerge: false}

			builtin.PrintCreateOperatorStatement(backupfile, tocfile, operator, builtin.ObjectMetadata{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR testschema.##(path, path)")

			resultOperators := builtin.GetOperators(connectionPool)
			Expect(resultOperators).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&operator, &resultOperators[0], "Oid")
		})
		It("creates operator with owner and comment", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")

			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION testschema.\"testFunc\" (path,path) RETURNS path AS 'SELECT $1' LANGUAGE SQL IMMUTABLE")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION testschema.\"testFunc\" (path,path)")

			operatorMetadata := testutils.DefaultMetadata("OPERATOR", false, false, true, false)
			operator := builtin.Operator{Oid: 1, Schema: "testschema", Name: "##", Procedure: "testschema.\"testFunc\"", LeftArgType: "path", RightArgType: "path", CommutatorOp: "0", NegatorOp: "0", RestrictFunction: "-", JoinFunction: "-", CanHash: false, CanMerge: false}

			builtin.PrintCreateOperatorStatement(backupfile, tocfile, operator, operatorMetadata)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR testschema.##(path, path)")

			resultOperators := builtin.GetOperators(connectionPool)
			Expect(resultOperators).To(HaveLen(1))
			resultMetadataMap := builtin.GetCommentsForObjectType(connectionPool, builtin.TYPE_OPERATOR)
			resultMetadata := resultMetadataMap[resultOperators[0].GetUniqueID()]
			structmatcher.ExpectStructsToMatchExcluding(&operator, &resultOperators[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&resultMetadata, &operatorMetadata, "Oid")
		})
	})
	Describe("PrintCreateOperatorFamilyStatements", func() {
		BeforeEach(func() {
			testutils.SkipIfBefore5(connectionPool)
		})
		It("creates operator family", func() {
			operatorFamily := builtin.OperatorFamily{Oid: 1, Schema: "public", Name: "testfam", IndexMethod: "hash"}
			operatorFamilies := []builtin.OperatorFamily{operatorFamily}

			builtin.PrintCreateOperatorFamilyStatements(backupfile, tocfile, operatorFamilies, builtin.MetadataMap{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testfam USING hash")

			resultOperatorFamilies := builtin.GetOperatorFamilies(connectionPool)
			Expect(resultOperatorFamilies).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&operatorFamily, &resultOperatorFamilies[0], "Oid")
		})
		It("creates operator family with owner and comment", func() {
			operatorFamily := builtin.OperatorFamily{Oid: 1, Schema: "public", Name: "testfam", IndexMethod: "hash"}
			operatorFamilies := []builtin.OperatorFamily{operatorFamily}
			operatorFamilyMetadataMap := testutils.DefaultMetadataMap("OPERATOR FAMILY", false, true, true, false)
			operatorFamilyMetadata := operatorFamilyMetadataMap[operatorFamily.GetUniqueID()]

			builtin.PrintCreateOperatorFamilyStatements(backupfile, tocfile, operatorFamilies, operatorFamilyMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testfam USING hash")

			resultOperatorFamilies := builtin.GetOperatorFamilies(connectionPool)
			Expect(resultOperatorFamilies).To(HaveLen(1))
			resultMetadataMap := builtin.GetMetadataForObjectType(connectionPool, builtin.TYPE_OPERATORFAMILY)
			resultMetadata := resultMetadataMap[resultOperatorFamilies[0].GetUniqueID()]
			structmatcher.ExpectStructsToMatchExcluding(&operatorFamily, &resultOperatorFamilies[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&resultMetadata, &operatorFamilyMetadata, "Oid")
		})
	})
	Describe("PrintCreateOperatorClassStatement", func() {
		emptyMetadata := builtin.ObjectMetadata{}
		It("creates basic operator class", func() {
			operatorClass := builtin.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "hash", Type: "integer", Default: false, StorageType: "-", Operators: nil, Functions: nil}
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("5") { // Operator families do not exist prior to GPDB5
				operatorClass.FamilySchema = ""
				operatorClass.FamilyName = ""
			}

			builtin.PrintCreateOperatorClassStatement(backupfile, tocfile, operatorClass, emptyMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("5") {
				defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR CLASS public.testclass USING hash")
			} else {
				defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testclass USING hash CASCADE")
			}

			resultOperatorClasses := builtin.GetOperatorClasses(connectionPool)
			Expect(resultOperatorClasses).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&operatorClass, &resultOperatorClasses[0], "Oid")
		})
		/* comment out, due to CBDB/PG14 - GP7/PG12 behavior diff
		It("creates complex operator class", func() {
			testutils.SkipIfBefore5(connectionPool)
			operatorClass := builtin.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testfam", IndexMethod: "gist", Type: "integer", Default: true, StorageType: "-", Operators: nil, Functions: nil}

			operatorClass.Functions = []builtin.OperatorClassFunction{{ClassOid: 0, SupportNumber: 1, RightType: "integer", LeftType: "integer", FunctionName: "abs(integer)"}}
			if connectionPool.Version.Before("5") { // Operator families do not exist prior to GPDB5
				operatorClass.FamilySchema = ""
				operatorClass.FamilyName = ""
				operatorClass.Functions = []builtin.OperatorClassFunction{{ClassOid: 0, SupportNumber: 1, FunctionName: "abs(integer)"}}
			}

			expectedRecheck := false
			if connectionPool.Version.Before("6") {
				expectedRecheck = true
			}
			operatorClass.Operators = []builtin.OperatorClassOperator{{ClassOid: 0, StrategyNumber: 1, Operator: "=(integer,integer)", Recheck: expectedRecheck}}

			testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR FAMILY public.testfam USING gist")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testfam USING gist CASCADE")
			builtin.PrintCreateOperatorClassStatement(backupfile, tocfile, operatorClass, emptyMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultOperatorClasses := builtin.GetOperatorClasses(connectionPool)
			Expect(resultOperatorClasses).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&operatorClass, &resultOperatorClasses[0], "Oid", "Operators.ClassOid", "Functions.ClassOid")
		})

		*/
		/* comment out, due to CBDB/PG14 - GP7/PG12 behavior diff
		It("creates an operator class with an operator that has a sort family", func() {
			testutils.SkipIfBefore6(connectionPool)
			operatorClass := builtin.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "gist", Type: "integer", Default: true, StorageType: "-", Operators: nil, Functions: nil}
			operatorClass.Operators = []builtin.OperatorClassOperator{{ClassOid: 0, StrategyNumber: 1, Operator: "=(integer,integer)", Recheck: false, OrderByFamily: "public.sort_family_name"}}

			testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR FAMILY public.sort_family_name USING btree")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.sort_family_name USING btree")

			builtin.PrintCreateOperatorClassStatement(backupfile, tocfile, operatorClass, emptyMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testclass USING gist CASCADE")

			resultOperatorClasses := builtin.GetOperatorClasses(connectionPool)
			Expect(resultOperatorClasses).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&operatorClass, &resultOperatorClasses[0], "Oid", "Operators.ClassOid", "Functions.ClassOid")
		})

		*/
		It("creates basic operator class with a comment and owner", func() {
			operatorClass := builtin.OperatorClass{Oid: 1, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "hash", Type: "integer", Default: false, StorageType: "-", Operators: nil, Functions: nil}
			operatorClassMetadata := testutils.DefaultMetadata("OPERATOR CLASS", false, true, true, false)
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("5") { // Operator families do not exist prior to GPDB5
				operatorClass.FamilySchema = ""
				operatorClass.FamilyName = ""
			}

			builtin.PrintCreateOperatorClassStatement(backupfile, tocfile, operatorClass, operatorClassMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("5") {
				defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR CLASS public.testclass USING hash")
			} else {
				defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testclass USING hash CASCADE")
			}

			resultOperatorClasses := builtin.GetOperatorClasses(connectionPool)
			Expect(resultOperatorClasses).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&operatorClass, &resultOperatorClasses[0], "Oid")

			resultMetadataMap := builtin.GetMetadataForObjectType(connectionPool, builtin.TYPE_OPERATORCLASS)
			resultMetadata := resultMetadataMap[resultOperatorClasses[0].GetUniqueID()]
			structmatcher.ExpectStructsToMatchExcluding(&resultMetadata, &operatorClassMetadata, "Oid")

		})
	})
})
