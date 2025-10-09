package builtin_test

import (
	"database/sql"
	"fmt"

	// "github.com/greenplum-db/gpbackup/backup"
	"github.com/cloudberry-contrib/cbcopy/meta/builtin"

	// "github.com/greenplum-db/gpbackup/testutils"
	"github.com/cloudberry-contrib/cbcopy/testutils"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"

	//"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/cloudberry-contrib/cbcopy/internal/testhelper"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/predata_acl tests", func() {
	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintObjectMetadata", func() {
		table := builtin.Table{Relation: builtin.Relation{Schema: "public", Name: "tablename"}}
		hasAllPrivileges := testutils.DefaultACLForType("anothertestrole", "TABLE")
		hasMostPrivileges := testutils.DefaultACLForType("testrole", "TABLE")
		hasMostPrivileges.Trigger = false
		hasSinglePrivilege := builtin.ACL{Grantee: "", Trigger: true}
		hasAllPrivilegesWithGrant := testutils.DefaultACLForTypeWithGrant("anothertestrole", "TABLE")
		hasMostPrivilegesWithGrant := testutils.DefaultACLForTypeWithGrant("testrole", "TABLE")
		hasMostPrivilegesWithGrant.TriggerWithGrant = false
		hasSinglePrivilegeWithGrant := builtin.ACL{Grantee: "", TriggerWithGrant: true}
		privileges := []builtin.ACL{hasAllPrivileges, hasMostPrivileges, hasSinglePrivilege}
		privilegesWithGrant := []builtin.ACL{hasAllPrivilegesWithGrant, hasMostPrivilegesWithGrant, hasSinglePrivilegeWithGrant}
		It("prints a block with a table comment", func() {
			tableMetadata := builtin.ObjectMetadata{Comment: "This is a table comment."}
			builtin.PrintObjectMetadata(backupfile, tocfile, tableMetadata, table, "")
			testhelper.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';`)
		})
		It("prints a block with a table comment with special characters", func() {
			tableMetadata := builtin.ObjectMetadata{Comment: `This is a ta'ble 1+=;,./\>,<@\\n^comment.`}
			builtin.PrintObjectMetadata(backupfile, tocfile, tableMetadata, table, "")
			testhelper.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a ta''ble 1+=;,./\>,<@\\n^comment.';`)
		})
		It("prints an ALTER TABLE ... OWNER TO statement to set the table owner", func() {
			tableMetadata := builtin.ObjectMetadata{Owner: "testrole"}
			builtin.PrintObjectMetadata(backupfile, tocfile, tableMetadata, table, "")
			testhelper.ExpectRegexp(buffer, `

ALTER TABLE public.tablename OWNER TO testrole;`)
		})
		It("prints a block of REVOKE and GRANT statements", func() {
			tableMetadata := builtin.ObjectMetadata{Privileges: privileges}
			builtin.PrintObjectMetadata(backupfile, tocfile, tableMetadata, table, "")
			testhelper.ExpectRegexp(buffer, `

REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC;`)
		})
		It("prints a block of REVOKE and GRANT statements WITH GRANT OPTION", func() {
			tableMetadata := builtin.ObjectMetadata{Privileges: privilegesWithGrant}
			builtin.PrintObjectMetadata(backupfile, tocfile, tableMetadata, table, "")
			testhelper.ExpectRegexp(buffer, `

REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
GRANT ALL ON TABLE public.tablename TO anothertestrole WITH GRANT OPTION;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole WITH GRANT OPTION;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC WITH GRANT OPTION;`)
		})
		It("prints a block of REVOKE and GRANT statements, some with WITH GRANT OPTION, some without", func() {
			tableMetadata := builtin.ObjectMetadata{Privileges: []builtin.ACL{hasAllPrivileges, hasMostPrivilegesWithGrant}}
			builtin.PrintObjectMetadata(backupfile, tocfile, tableMetadata, table, "")
			testhelper.ExpectRegexp(buffer, `

REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole WITH GRANT OPTION;`)
		})
		It("prints both an ALTER TABLE ... OWNER TO statement and a table comment", func() {
			tableMetadata := builtin.ObjectMetadata{Comment: "This is a table comment.", Owner: "testrole"}
			builtin.PrintObjectMetadata(backupfile, tocfile, tableMetadata, table, "")
			testhelper.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';


ALTER TABLE public.tablename OWNER TO testrole;`)
		})
		It("prints both a block of REVOKE and GRANT statements and an ALTER TABLE ... OWNER TO statement", func() {
			tableMetadata := builtin.ObjectMetadata{Privileges: privileges, Owner: "testrole"}
			builtin.PrintObjectMetadata(backupfile, tocfile, tableMetadata, table, "")
			testhelper.ExpectRegexp(buffer, `

ALTER TABLE public.tablename OWNER TO testrole;


REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
REVOKE ALL ON TABLE public.tablename FROM testrole;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC;`)
		})
		It("prints both a block of REVOKE and GRANT statements and a table comment", func() {
			tableMetadata := builtin.ObjectMetadata{Privileges: privileges, Comment: "This is a table comment."}
			builtin.PrintObjectMetadata(backupfile, tocfile, tableMetadata, table, "")
			testhelper.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';


REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC;`)
		})
		It("prints REVOKE and GRANT statements, an ALTER TABLE ... OWNER TO statement, and comments", func() {
			tableMetadata := builtin.ObjectMetadata{Privileges: privileges, Owner: "testrole", Comment: "This is a table comment."}
			builtin.PrintObjectMetadata(backupfile, tocfile, tableMetadata, table, "")
			testhelper.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';


ALTER TABLE public.tablename OWNER TO testrole;


REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
REVOKE ALL ON TABLE public.tablename FROM testrole;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC;`)
		})
		It("prints SERVER for ALTER and FOREIGN SERVER for GRANT/REVOKE for a foreign server", func() {
			server := builtin.ForeignServer{Name: "foreignserver"}
			serverPrivileges := testutils.DefaultACLForType("testrole", "FOREIGN SERVER")
			serverMetadata := builtin.ObjectMetadata{Privileges: []builtin.ACL{serverPrivileges}, Owner: "testrole"}
			builtin.PrintObjectMetadata(backupfile, tocfile, serverMetadata, server, "")
			testhelper.ExpectRegexp(buffer, `

ALTER SERVER foreignserver OWNER TO testrole;


REVOKE ALL ON FOREIGN SERVER foreignserver FROM PUBLIC;
REVOKE ALL ON FOREIGN SERVER foreignserver FROM testrole;
GRANT ALL ON FOREIGN SERVER foreignserver TO testrole;`)
		})
		It("prints FUNCTION for REVOKE and AGGREGATE for ALTER for an aggregate function", func() {
			aggregate := builtin.Aggregate{Schema: "public", Name: "testagg"}
			aggregatePrivileges := testutils.DefaultACLForType("testrole", "AGGREGATE")
			aggregateMetadata := builtin.ObjectMetadata{Privileges: []builtin.ACL{aggregatePrivileges}, Owner: "testrole"}
			builtin.PrintObjectMetadata(backupfile, tocfile, aggregateMetadata, aggregate, "")
			testhelper.ExpectRegexp(buffer, `

ALTER AGGREGATE public.testagg(*) OWNER TO testrole;


REVOKE ALL ON FUNCTION public.testagg(*) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.testagg(*) FROM testrole;`)
		})
		Context("Views and sequences have owners", func() {
			view := builtin.View{Schema: "public", Name: "viewname"}
			sequence := builtin.Sequence{Relation: builtin.Relation{Schema: "public", Name: "sequencename"}}
			objectMetadata := builtin.ObjectMetadata{Owner: "testrole"}

			It("prints an ALTER TABLE ... OWNER TO statement to set the owner for a sequence", func() {
				expectedKeyword := `TABLE`
				if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
					expectedKeyword = `SEQUENCE`
				}

				builtin.PrintObjectMetadata(backupfile, tocfile, objectMetadata, sequence, "public.sequencename")
				testhelper.ExpectRegexp(buffer, fmt.Sprintf(`

ALTER %s public.sequencename OWNER TO testrole;`, expectedKeyword))
			})
			It("prints an ALTER TABLE ... OWNER TO statement to set the owner for a view", func() {
				expectedKeyword := `TABLE`
				if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
					expectedKeyword = `VIEW`
				}

				builtin.PrintObjectMetadata(backupfile, tocfile, objectMetadata, view, "public.viewname")
				testhelper.ExpectRegexp(buffer, fmt.Sprintf(`

ALTER %s public.viewname OWNER TO testrole;`, expectedKeyword))
			})
		})
	})
	Describe("PrintDefaultPrivilegeStatements", func() {
		privs := []builtin.ACL{{Grantee: "", Usage: true}}
		It("prints ALTER DEFAULT PRIVILEGES statement for relation", func() {
			defaultPrivileges := []builtin.DefaultPrivileges{{Owner: "testrole", Schema: "", Privileges: privs, ObjectType: "r"}}
			builtin.PrintDefaultPrivilegesStatements(backupfile, tocfile, defaultPrivileges)
			testhelper.ExpectRegexp(buffer, `
ALTER DEFAULT PRIVILEGES FOR ROLE testrole REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE testrole REVOKE ALL ON TABLES FROM testrole;
ALTER DEFAULT PRIVILEGES FOR ROLE testrole GRANT USAGE ON TABLES TO PUBLIC;
`)
		})
		It("prints ALTER DEFAULT PRIVILEGES statement for sequence", func() {
			defaultPrivileges := []builtin.DefaultPrivileges{{Owner: "testrole", Schema: "", Privileges: privs, ObjectType: "S"}}
			builtin.PrintDefaultPrivilegesStatements(backupfile, tocfile, defaultPrivileges)
			testhelper.ExpectRegexp(buffer, `
ALTER DEFAULT PRIVILEGES FOR ROLE testrole REVOKE ALL ON SEQUENCES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE testrole REVOKE ALL ON SEQUENCES FROM testrole;
ALTER DEFAULT PRIVILEGES FOR ROLE testrole GRANT USAGE ON SEQUENCES TO PUBLIC;
`)
		})
		It("prints ALTER DEFAULT PRIVILEGES statement for function", func() {
			defaultPrivileges := []builtin.DefaultPrivileges{{Owner: "testrole", Schema: "", Privileges: privs, ObjectType: "f"}}
			builtin.PrintDefaultPrivilegesStatements(backupfile, tocfile, defaultPrivileges)
			testhelper.ExpectRegexp(buffer, `
ALTER DEFAULT PRIVILEGES FOR ROLE testrole REVOKE ALL ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE testrole REVOKE ALL ON FUNCTIONS FROM testrole;
ALTER DEFAULT PRIVILEGES FOR ROLE testrole GRANT USAGE ON FUNCTIONS TO PUBLIC;
`)
		})
		It("prints ALTER DEFAULT PRIVILEGES statement for type", func() {
			defaultPrivileges := []builtin.DefaultPrivileges{{Owner: "testrole", Schema: "", Privileges: privs, ObjectType: "T"}}
			builtin.PrintDefaultPrivilegesStatements(backupfile, tocfile, defaultPrivileges)
			testhelper.ExpectRegexp(buffer, `
ALTER DEFAULT PRIVILEGES FOR ROLE testrole REVOKE ALL ON TYPES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE testrole REVOKE ALL ON TYPES FROM testrole;
ALTER DEFAULT PRIVILEGES FOR ROLE testrole GRANT ALL ON TYPES TO PUBLIC;
`)
		})
		It("prints ALTER DEFAULT PRIVILEGES statement for role", func() {
			localPrivs := []builtin.ACL{{Grantee: "somerole", Usage: true}}
			defaultPrivileges := []builtin.DefaultPrivileges{{Schema: "", Owner: "somerole", Privileges: localPrivs, ObjectType: "r"}}
			builtin.PrintDefaultPrivilegesStatements(backupfile, tocfile, defaultPrivileges)
			testhelper.ExpectRegexp(buffer, `
ALTER DEFAULT PRIVILEGES FOR ROLE somerole REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE somerole REVOKE ALL ON TABLES FROM somerole;
ALTER DEFAULT PRIVILEGES FOR ROLE somerole GRANT USAGE ON TABLES TO somerole;
`)
		})
		It("prints ALTER DEFAULT PRIVILEGES statement in schema", func() {
			defaultPrivileges := []builtin.DefaultPrivileges{{Owner: "testrole", Schema: "myschema", Privileges: privs, ObjectType: "r"}}
			builtin.PrintDefaultPrivilegesStatements(backupfile, tocfile, defaultPrivileges)
			testhelper.ExpectRegexp(buffer, `
ALTER DEFAULT PRIVILEGES FOR ROLE testrole IN SCHEMA myschema REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE testrole IN SCHEMA myschema REVOKE ALL ON TABLES FROM testrole;
ALTER DEFAULT PRIVILEGES FOR ROLE testrole IN SCHEMA myschema GRANT USAGE ON TABLES TO PUBLIC;
`)
		})
		It("prints ALTER DEFAULT PRIVILEGES statement for role in schema", func() {
			localPrivs := []builtin.ACL{{Grantee: "somerole", Usage: true}}
			defaultPrivileges := []builtin.DefaultPrivileges{{Schema: "myschema", Owner: "somerole", Privileges: localPrivs, ObjectType: "r"}}
			builtin.PrintDefaultPrivilegesStatements(backupfile, tocfile, defaultPrivileges)
			testhelper.ExpectRegexp(buffer, `
ALTER DEFAULT PRIVILEGES FOR ROLE somerole IN SCHEMA myschema REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE somerole IN SCHEMA myschema REVOKE ALL ON TABLES FROM somerole;
ALTER DEFAULT PRIVILEGES FOR ROLE somerole IN SCHEMA myschema GRANT USAGE ON TABLES TO somerole;
`)
		})
		It("prints ALTER DEFAULT PRIVILEGES statement with grant option", func() {
			localPrivs := []builtin.ACL{{Grantee: "somerole", Usage: true, UsageWithGrant: true}}
			defaultPrivileges := []builtin.DefaultPrivileges{{Owner: "testrole", Schema: "", Privileges: localPrivs, ObjectType: "r"}}
			builtin.PrintDefaultPrivilegesStatements(backupfile, tocfile, defaultPrivileges)
			testhelper.ExpectRegexp(buffer, `
ALTER DEFAULT PRIVILEGES FOR ROLE testrole REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE testrole REVOKE ALL ON TABLES FROM testrole;
ALTER DEFAULT PRIVILEGES FOR ROLE testrole GRANT USAGE ON TABLES TO somerole;
ALTER DEFAULT PRIVILEGES FOR ROLE testrole GRANT USAGE ON TABLES TO somerole WITH GRANT OPTION;
`)
		})
	})
	Describe("ConstructMetadataMap", func() {
		object1A := builtin.MetadataQueryStruct{UniqueID: builtin.UniqueID{Oid: 1}, Privileges: sql.NullString{String: "gpadmin=r/gpadmin", Valid: true}, Kind: "", Owner: "testrole", Comment: ""}
		object1B := builtin.MetadataQueryStruct{UniqueID: builtin.UniqueID{Oid: 1}, Privileges: sql.NullString{String: "testrole=r/testrole", Valid: true}, Kind: "", Owner: "testrole", Comment: ""}
		object2 := builtin.MetadataQueryStruct{UniqueID: builtin.UniqueID{Oid: 2}, Privileges: sql.NullString{String: "testrole=r/testrole", Valid: true}, Kind: "", Owner: "testrole", Comment: "this is a comment", SecurityLabelProvider: "some_provider", SecurityLabel: "some_label"}
		objectDefaultKind := builtin.MetadataQueryStruct{UniqueID: builtin.UniqueID{Oid: 3}, Privileges: sql.NullString{String: "", Valid: false}, Kind: "Default", Owner: "testrole", Comment: ""}
		objectEmptyKind := builtin.MetadataQueryStruct{UniqueID: builtin.UniqueID{Oid: 4}, Privileges: sql.NullString{String: "", Valid: false}, Kind: "Empty", Owner: "testrole", Comment: ""}
		var metadataList []builtin.MetadataQueryStruct
		BeforeEach(func() {
			rolnames := sqlmock.NewRows([]string{"rolename", "quotedrolename"}).
				AddRow("gpadmin", "gpadmin").
				AddRow("testrole", "testrole")
			mock.ExpectQuery("SELECT rolname (.*)").
				WillReturnRows(rolnames)
			metadataList = []builtin.MetadataQueryStruct{}
		})
		It("No objects", func() {
			metadataMap := builtin.ConstructMetadataMap(metadataList)
			Expect(metadataMap).To(BeEmpty())
		})
		It("One object", func() {
			metadataList = []builtin.MetadataQueryStruct{object2}
			metadataMap := builtin.ConstructMetadataMap(metadataList)
			expectedObjectMetadata := builtin.ObjectMetadata{Privileges: []builtin.ACL{{Grantee: "testrole", Select: true}}, Owner: "testrole", Comment: "this is a comment", SecurityLabelProvider: "some_provider", SecurityLabel: "some_label"}
			Expect(metadataMap).To(HaveLen(1))
			Expect(metadataMap[builtin.UniqueID{Oid: 2}]).To(Equal(expectedObjectMetadata))

		})
		It("One object with two ACL entries", func() {
			metadataList = []builtin.MetadataQueryStruct{object1A, object1B}
			metadataMap := builtin.ConstructMetadataMap(metadataList)
			expectedObjectMetadata := builtin.ObjectMetadata{Privileges: []builtin.ACL{{Grantee: "gpadmin", Select: true}, {Grantee: "testrole", Select: true}}, Owner: "testrole"}
			Expect(metadataMap).To(HaveLen(1))
			Expect(metadataMap[builtin.UniqueID{Oid: 1}]).To(Equal(expectedObjectMetadata))
		})
		It("Multiple objects", func() {
			metadataList = []builtin.MetadataQueryStruct{object1A, object1B, object2}
			metadataMap := builtin.ConstructMetadataMap(metadataList)
			expectedObjectMetadataOne := builtin.ObjectMetadata{Privileges: []builtin.ACL{{Grantee: "gpadmin", Select: true}, {Grantee: "testrole", Select: true}}, Owner: "testrole"}
			expectedObjectMetadataTwo := builtin.ObjectMetadata{Privileges: []builtin.ACL{{Grantee: "testrole", Select: true}}, Owner: "testrole", Comment: "this is a comment", SecurityLabelProvider: "some_provider", SecurityLabel: "some_label"}
			Expect(metadataMap).To(HaveLen(2))
			Expect(metadataMap[builtin.UniqueID{Oid: 1}]).To(Equal(expectedObjectMetadataOne))
			Expect(metadataMap[builtin.UniqueID{Oid: 2}]).To(Equal(expectedObjectMetadataTwo))
		})
		It("Default Kind", func() {
			metadataList = []builtin.MetadataQueryStruct{objectDefaultKind}
			metadataMap := builtin.ConstructMetadataMap(metadataList)
			expectedObjectMetadata := builtin.ObjectMetadata{Privileges: []builtin.ACL{}, Owner: "testrole"}
			Expect(metadataMap).To(HaveLen(1))
			Expect(metadataMap[builtin.UniqueID{Oid: 3}]).To(Equal(expectedObjectMetadata))
		})
		It("'Empty' Kind", func() {
			metadataList = []builtin.MetadataQueryStruct{objectEmptyKind}
			metadataMap := builtin.ConstructMetadataMap(metadataList)
			expectedObjectMetadata := builtin.ObjectMetadata{Privileges: []builtin.ACL{{Grantee: "GRANTEE"}}, Owner: "testrole"}
			Expect(metadataMap).To(HaveLen(1))
			Expect(metadataMap[builtin.UniqueID{Oid: 4}]).To(Equal(expectedObjectMetadata))
		})
	})
	Describe("ParseACL", func() {
		var quotedRoleNames map[string]string
		BeforeEach(func() {
			quotedRoleNames = map[string]string{
				"testrole":  "testrole",
				"Test|role": `"Test|role"`,
			}
			builtin.SetQuotedRoleNames(quotedRoleNames)
		})
		It("parses an ACL string representing default privileges", func() {
			aclStr := ""
			result := builtin.ParseACL(aclStr)
			Expect(result).To(BeNil())
		})
		It("parses an ACL string representing no privileges", func() {
			aclStr := "GRANTEE=/GRANTOR"
			expected := builtin.ACL{Grantee: "GRANTEE"}
			result := builtin.ParseACL(aclStr)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
		It("parses an ACL string containing a role with multiple privileges", func() {
			aclStr := "testrole=arwdDxt/gpadmin"
			expected := testutils.DefaultACLForType("testrole", "TABLE")
			result := builtin.ParseACL(aclStr)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
		It("parses an ACL string containing a role with one privilege", func() {
			aclStr := "testrole=a/gpadmin"
			expected := builtin.ACL{Grantee: "testrole", Insert: true}
			result := builtin.ParseACL(aclStr)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
		It("parses an ACL string containing a role name with special characters", func() {
			aclStr := `Test|role=a/gpadmin`
			expected := builtin.ACL{Grantee: `"Test|role"`, Insert: true}
			result := builtin.ParseACL(aclStr)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
		It("parses an ACL string containing a role with some privileges with GRANT and some without including GRANT", func() {
			aclStr := "testrole=ar*w*d*tXUCTc/gpadmin"
			expected := builtin.ACL{Grantee: "testrole", Insert: true, SelectWithGrant: true, UpdateWithGrant: true,
				DeleteWithGrant: true, Trigger: true, Execute: true, Usage: true, Create: true, Temporary: true, Connect: true}
			result := builtin.ParseACL(aclStr)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
		It("parses an ACL string containing a role with all privileges including GRANT", func() {
			aclStr := "testrole=a*D*x*t*X*U*C*T*c*/gpadmin"
			expected := builtin.ACL{Grantee: "testrole", InsertWithGrant: true, TruncateWithGrant: true, ReferencesWithGrant: true,
				TriggerWithGrant: true, ExecuteWithGrant: true, UsageWithGrant: true, CreateWithGrant: true, TemporaryWithGrant: true, ConnectWithGrant: true}
			result := builtin.ParseACL(aclStr)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
		It("parses an ACL string granting privileges to PUBLIC", func() {
			aclStr := "=a/gpadmin"
			expected := builtin.ACL{Grantee: "", Insert: true}
			result := builtin.ParseACL(aclStr)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
	})
	Describe("ConstructDefaultPrivileges", func() {
		object1A := builtin.DefaultPrivilegesQueryStruct{Oid: 1, Owner: "testrole", Schema: "myschema", Kind: "", ObjectType: "r", Privileges: sql.NullString{String: "gpadmin=r/gpadmin", Valid: true}}
		object1B := builtin.DefaultPrivilegesQueryStruct{Oid: 1, Owner: "testrole", Schema: "myschema", Kind: "", ObjectType: "r", Privileges: sql.NullString{String: "testrole=r/testrole", Valid: true}}
		object2 := builtin.DefaultPrivilegesQueryStruct{Oid: 2, Owner: "testrole", Schema: "myschema", Kind: "", ObjectType: "S", Privileges: sql.NullString{String: "testrole=r/testrole", Valid: true}}
		objectDefaultKind := builtin.DefaultPrivilegesQueryStruct{Oid: 3, Owner: "testrole", Schema: "", Kind: "Default", ObjectType: "T", Privileges: sql.NullString{String: "", Valid: false}}
		objectEmptyKind := builtin.DefaultPrivilegesQueryStruct{Oid: 4, Owner: "testrole", Schema: "", Kind: "Empty", ObjectType: "f", Privileges: sql.NullString{String: "", Valid: false}}
		var privilegesQuerylist []builtin.DefaultPrivilegesQueryStruct
		BeforeEach(func() {
			rolnames := sqlmock.NewRows([]string{"rolename", "quotedrolename"}).
				AddRow("gpadmin", "gpadmin").
				AddRow("testrole", "testrole")
			mock.ExpectQuery("SELECT rolname (.*)").
				WillReturnRows(rolnames)
			privilegesQuerylist = []builtin.DefaultPrivilegesQueryStruct{}
		})
		It("returns no privileges when no default privileges exist", func() {
			privilegesList := builtin.ConstructDefaultPrivileges(privilegesQuerylist)
			Expect(privilegesList).To(BeEmpty())
		})
		It("constructs a single sequence default privilege in a specific schema", func() {
			privilegesQuerylist = []builtin.DefaultPrivilegesQueryStruct{object2}
			privilegesList := builtin.ConstructDefaultPrivileges(privilegesQuerylist)
			expectedDefaultPrivileges := builtin.DefaultPrivileges{Privileges: []builtin.ACL{{Grantee: "testrole", Select: true}}, Owner: "testrole", Schema: "myschema", ObjectType: "S"}
			Expect(privilegesList).To(HaveLen(1))
			Expect(privilegesList[0]).To(Equal(expectedDefaultPrivileges))
		})
		It("constructs multiple default privileges on a single relation in a specific schema", func() {
			privilegesQuerylist = []builtin.DefaultPrivilegesQueryStruct{object1A, object1B}
			privilegesList := builtin.ConstructDefaultPrivileges(privilegesQuerylist)
			expectedObjectMetadata := builtin.DefaultPrivileges{Privileges: []builtin.ACL{{Grantee: "gpadmin", Select: true}, {Grantee: "testrole", Select: true}}, Owner: "testrole", Schema: "myschema", ObjectType: "r"}
			Expect(privilegesList).To(HaveLen(1))
			Expect(privilegesList[0]).To(Equal(expectedObjectMetadata))
		})
		It("constructs multiple default privileges on multiple objects in a specific schema", func() {
			privilegesQuerylist = []builtin.DefaultPrivilegesQueryStruct{object1A, object1B, object2}
			privilegesList := builtin.ConstructDefaultPrivileges(privilegesQuerylist)
			expectedObjectMetadataOne := builtin.DefaultPrivileges{Privileges: []builtin.ACL{{Grantee: "gpadmin", Select: true}, {Grantee: "testrole", Select: true}}, Owner: "testrole", Schema: "myschema", ObjectType: "r"}
			expectedObjectMetadataTwo := builtin.DefaultPrivileges{Privileges: []builtin.ACL{{Grantee: "testrole", Select: true}}, Owner: "testrole", Schema: "myschema", ObjectType: "S"}
			Expect(privilegesList).To(HaveLen(2))
			Expect(privilegesList[0]).To(Equal(expectedObjectMetadataOne))
			Expect(privilegesList[1]).To(Equal(expectedObjectMetadataTwo))
		})
		It("constructs a default privilege for a type with a 'Default' kind", func() {
			privilegesQuerylist = []builtin.DefaultPrivilegesQueryStruct{objectDefaultKind}
			privilegesList := builtin.ConstructDefaultPrivileges(privilegesQuerylist)
			expectedObjectMetadata := builtin.DefaultPrivileges{Privileges: []builtin.ACL{}, Owner: "testrole", Schema: "", ObjectType: "T"}
			Expect(privilegesList).To(HaveLen(1))
			Expect(privilegesList[0]).To(Equal(expectedObjectMetadata))
		})
		It("constructs a default privilege for a function with an 'Empty' kind", func() {
			privilegesQuerylist = []builtin.DefaultPrivilegesQueryStruct{objectEmptyKind}
			privilegesList := builtin.ConstructDefaultPrivileges(privilegesQuerylist)
			expectedObjectMetadata := builtin.DefaultPrivileges{Privileges: []builtin.ACL{{Grantee: "GRANTEE"}}, Owner: "testrole", Schema: "", ObjectType: "f"}
			Expect(privilegesList).To(HaveLen(1))
			Expect(privilegesList[0]).To(Equal(expectedObjectMetadata))
		})
	})
})
