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
		testutils.SkipIfBefore6(connectionPool)
	})
	Describe("PrintDefaultPrivilegesStatements", func() {
		It("create default privileges for a table", func() {
			privs := []builtin.ACL{{Grantee: "", Select: true}, testutils.DefaultACLForType("testrole", "TABLE")}
			defaultPrivileges := []builtin.DefaultPrivileges{{Schema: "", Privileges: privs, ObjectType: "r", Owner: "testrole"}}

			builtin.PrintDefaultPrivilegesStatements(backupfile, tocfile, defaultPrivileges)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "ALTER DEFAULT PRIVILEGES FOR ROLE testrole REVOKE ALL ON TABLES FROM PUBLIC;")

			resultPrivileges := builtin.GetDefaultPrivileges(connectionPool)

			Expect(resultPrivileges).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&defaultPrivileges[0], &resultPrivileges[0], "Oid")
		})
		It("create default privileges for a sequence with grant option in schema", func() {
			privs := []builtin.ACL{{Grantee: "testrole", SelectWithGrant: true}}
			defaultPrivileges := []builtin.DefaultPrivileges{{Schema: "", Privileges: privs, ObjectType: "S", Owner: "testrole"}}

			builtin.PrintDefaultPrivilegesStatements(backupfile, tocfile, defaultPrivileges)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			// Both of these statements are required to remove the entry from the pg_default_acl catalog table, otherwise it will pollute other tests
			defer testhelper.AssertQueryRuns(connectionPool, "ALTER DEFAULT PRIVILEGES FOR ROLE testrole GRANT ALL ON SEQUENCES TO testrole;")
			defer testhelper.AssertQueryRuns(connectionPool, "ALTER DEFAULT PRIVILEGES FOR ROLE testrole REVOKE ALL ON SEQUENCES FROM testrole;")

			resultPrivileges := builtin.GetDefaultPrivileges(connectionPool)

			Expect(resultPrivileges).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&defaultPrivileges[0], &resultPrivileges[0], "Oid")
		})
	})
})
