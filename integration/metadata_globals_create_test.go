package integration

import (
	"fmt"
	"regexp"

	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	// "github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/cloudberrydb/cbcopy/internal/testhelper"

	// "github.com/greenplum-db/gpbackup/backup"
	"github.com/cloudberrydb/cbcopy/meta/builtin"

	// "github.com/greenplum-db/gpbackup/testutils"
	"github.com/cloudberrydb/cbcopy/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
)

var _ = Describe("cbcopy integration create statement tests", func() {
	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintCreateDatabaseStatement", func() {
		emptyDB := builtin.Database{}
		emptyMetadataMap := builtin.MetadataMap{}
		BeforeEach(func() {
			connectionPool.DBName = "create_test_db"
		})
		AfterEach(func() {
			connectionPool.DBName = "testdb"
		})
		It("creates a basic database", func() {
			db := builtin.Database{Oid: 1, Name: "create_test_db", Tablespace: "pg_default", Encoding: "UTF8"}
			builtin.PrintCreateDatabaseStatement(backupfile, tocfile, emptyDB, db, emptyMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP DATABASE create_test_db")

			resultDB := builtin.GetDatabaseInfo(connectionPool)
			structmatcher.ExpectStructsToMatchExcluding(&db, &resultDB, "Oid", "Collate", "CType")
		})
		It("creates a database with properties the same as defaults", func() {
			defaultDB := builtin.GetDefaultDatabaseEncodingInfo(connectionPool)
			var db builtin.Database
			db = builtin.Database{Oid: 1, Name: "create_test_db", Tablespace: "pg_default", Encoding: "UTF8", Collate: defaultDB.Collate, CType: defaultDB.Collate}

			builtin.PrintCreateDatabaseStatement(backupfile, tocfile, defaultDB, db, emptyMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP DATABASE create_test_db")

			resultDB := builtin.GetDatabaseInfo(connectionPool)
			structmatcher.ExpectStructsToMatchExcluding(&db, &resultDB, "Oid")
		})
		It("creates a database with all properties", func() {
			var db builtin.Database
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("6") {
				db = builtin.Database{Oid: 1, Name: "create_test_db", Tablespace: "pg_default", Encoding: "UTF8", Collate: "", CType: ""}
			} else {
				db = builtin.Database{Oid: 1, Name: "create_test_db", Tablespace: "pg_default", Encoding: "UTF8", Collate: "en_US.utf-8", CType: "en_US.utf-8"}
			}

			builtin.PrintCreateDatabaseStatement(backupfile, tocfile, emptyDB, db, emptyMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP DATABASE create_test_db")

			resultDB := builtin.GetDatabaseInfo(connectionPool)
			structmatcher.ExpectStructsToMatchExcluding(&db, &resultDB, "Oid")
		})
	})
	Describe("PrintDatabaseGUCs", func() {
		It("creates database GUCs with correct quoting", func() {
			enableNestLoopGUC := "SET enable_nestloop TO 'true'"
			searchPathGUC := "SET search_path TO pg_catalog, public"
			defaultStorageGUC := "SET gp_default_storage_options TO 'appendonly=true, compresslevel=6, orientation=row, compresstype=none'"
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDB() {
				defaultStorageGUC = "SET gp_default_storage_options TO 'compresslevel=6, compresstype=none'"
			}

			gucs := []string{enableNestLoopGUC, searchPathGUC, defaultStorageGUC}

			builtin.PrintDatabaseGUCs(backupfile, tocfile, gucs, "testdb")
			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "ALTER DATABASE testdb RESET enable_nestloop")
			defer testhelper.AssertQueryRuns(connectionPool, "ALTER DATABASE testdb RESET search_path")
			defer testhelper.AssertQueryRuns(connectionPool, "ALTER DATABASE testdb RESET gp_default_storage_options")
			resultGUCs := builtin.GetDatabaseGUCs(connectionPool)
			Expect(resultGUCs).To(Equal(gucs))
		})
	})
	Describe("PrintCreateResourceQueueStatements", func() {
		It("creates a basic resource queue with a comment", func() {
			basicQueue := builtin.ResourceQueue{Oid: 1, Name: `"basicQueue"`, ActiveStatements: -1, MaxCost: "32.80", CostOvercommit: false, MinCost: "0.00", Priority: "medium", MemoryLimit: "-1"}
			resQueueMetadataMap := testutils.DefaultMetadataMap("RESOURCE QUEUE", false, false, true, false)
			resQueueMetadata := resQueueMetadataMap[basicQueue.GetUniqueID()]

			builtin.PrintCreateResourceQueueStatements(backupfile, tocfile, []builtin.ResourceQueue{basicQueue}, resQueueMetadataMap)

			// CREATE RESOURCE QUEUE statements can not be part of a multi-command statement, so
			// feed the CREATE RESOURCE QUEUE and COMMENT ON statements separately.
			hunks := regexp.MustCompile(";\n\n").Split(buffer.String(), 2)
			testhelper.AssertQueryRuns(connectionPool, hunks[0])
			defer testhelper.AssertQueryRuns(connectionPool, `DROP RESOURCE QUEUE "basicQueue"`)
			testhelper.AssertQueryRuns(connectionPool, hunks[1])

			resultResourceQueues := builtin.GetResourceQueues(connectionPool)
			resQueueUniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "basicQueue", builtin.TYPE_RESOURCEQUEUE)
			resultMetadataMap := builtin.GetCommentsForObjectType(connectionPool, builtin.TYPE_RESOURCEQUEUE)
			resultMetadata := resultMetadataMap[resQueueUniqueID]
			structmatcher.ExpectStructsToMatch(&resultMetadata, &resQueueMetadata)

			for _, resultQueue := range resultResourceQueues {
				if resultQueue.Name == `"basicQueue"` {
					structmatcher.ExpectStructsToMatchExcluding(&basicQueue, &resultQueue, "Oid")
					return
				}
			}
		})
		It("creates a resource queue with all attributes", func() {
			everythingQueue := builtin.ResourceQueue{Oid: 1, Name: `"everythingQueue"`, ActiveStatements: 7, MaxCost: "32.80", CostOvercommit: true, MinCost: "22.80", Priority: "low", MemoryLimit: "2GB"}
			emptyMetadataMap := map[builtin.UniqueID]builtin.ObjectMetadata{}

			builtin.PrintCreateResourceQueueStatements(backupfile, tocfile, []builtin.ResourceQueue{everythingQueue}, emptyMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, `DROP RESOURCE QUEUE "everythingQueue"`)

			resultResourceQueues := builtin.GetResourceQueues(connectionPool)

			for _, resultQueue := range resultResourceQueues {
				if resultQueue.Name == `"everythingQueue"` {
					structmatcher.ExpectStructsToMatchExcluding(&everythingQueue, &resultQueue, "Oid")
					return
				}
			}
			Fail("Could not find everythingQueue")
		})
	})
	Describe("PrintCreateResourceGroupStatements", func() {
		BeforeEach(func() {
			testutils.SkipIfBefore5(connectionPool)
		})
		It("creates a basic resource group", func() {
			emptyMetadataMap := map[builtin.UniqueID]builtin.ObjectMetadata{}

			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("7") {
				someGroup := builtin.ResourceGroupBefore7{ResourceGroup: builtin.ResourceGroup{Oid: 1, Name: "some_group", Concurrency: "15", Cpuset: "-1"}, CPURateLimit: "10", MemoryLimit: "20", MemorySharedQuota: "25", MemorySpillRatio: "30", MemoryAuditor: "0"}
				builtin.PrintCreateResourceGroupStatementsBefore7(backupfile, tocfile, []builtin.ResourceGroupBefore7{someGroup}, emptyMetadataMap)
				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, `DROP RESOURCE GROUP some_group`)

				resultResourceGroups := builtin.GetResourceGroups[builtin.ResourceGroupBefore7](connectionPool)
				for _, resultGroup := range resultResourceGroups {
					if resultGroup.Name == "some_group" {
						structmatcher.ExpectStructsToMatchExcluding(&someGroup, &resultGroup, "ResourceGroup.Oid")
						return
					}
				}
			} else { // GPDB7+
				return
				/* comment out, due to CBDB/PG14 - GP7/PG12 behavior diff
				someGroup := builtin.ResourceGroupAtLeast7{ResourceGroup: builtin.ResourceGroup{Oid: 1, Name: "some_group", Concurrency: "15", Cpuset: "-1"}, CpuMaxPercent: "10", CpuSoftPriority: "100"}
				builtin.PrintCreateResourceGroupStatementsAtLeast7(backupfile, tocfile, []builtin.ResourceGroupAtLeast7{someGroup}, emptyMetadataMap)
				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, `DROP RESOURCE GROUP some_group`)

				resultResourceGroups := builtin.GetResourceGroups[builtin.ResourceGroupAtLeast7](connectionPool)
				for _, resultGroup := range resultResourceGroups {
					if resultGroup.Name == "some_group" {
						structmatcher.ExpectStructsToMatchExcluding(&someGroup, &resultGroup, "ResourceGroup.Oid")
						return
					}
				}

				*/
			}
			Fail("Could not find some_group")
		})
		It("creates a resource group with defaults", func() {
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("7") {
				expectedDefaults := builtin.ResourceGroupBefore7{ResourceGroup: builtin.ResourceGroup{Oid: 1, Name: "some_group", Concurrency: concurrencyDefault, Cpuset: cpuSetDefault}, CPURateLimit: "10", MemoryLimit: "20",
					MemorySharedQuota: memSharedDefault, MemorySpillRatio: memSpillDefault, MemoryAuditor: memAuditDefault}

				testhelper.AssertQueryRuns(connectionPool, "CREATE RESOURCE GROUP some_group WITH (CPU_RATE_LIMIT=10, MEMORY_LIMIT=20);")
				defer testhelper.AssertQueryRuns(connectionPool, `DROP RESOURCE GROUP some_group`)

				resultResourceGroups := builtin.GetResourceGroups[builtin.ResourceGroupBefore7](connectionPool)

				for _, resultGroup := range resultResourceGroups {
					if resultGroup.Name == "some_group" {
						structmatcher.ExpectStructsToMatchExcluding(&expectedDefaults, &resultGroup, "ResourceGroup.Oid")
						return
					}
				}
			} else { // GPDB7+
				return
				/* comment out, due to CBDB/PG14 - GP7/PG12 behavior diff
				expectedDefaults := builtin.ResourceGroupAtLeast7{ResourceGroup: builtin.ResourceGroup{Oid: 1, Name: "some_group", Concurrency: concurrencyDefault, Cpuset: cpuSetDefault},
					CpuMaxPercent: "10", CpuSoftPriority: "100"}

				testhelper.AssertQueryRuns(connectionPool, "CREATE RESOURCE GROUP some_group WITH (CPU_MAX_PERCENT=10, CPU_WEIGHT=100);")
				defer testhelper.AssertQueryRuns(connectionPool, `DROP RESOURCE GROUP some_group`)

				resultResourceGroups := builtin.GetResourceGroups[builtin.ResourceGroupAtLeast7](connectionPool)

				for _, resultGroup := range resultResourceGroups {
					if resultGroup.Name == "some_group" {
						structmatcher.ExpectStructsToMatchExcluding(&expectedDefaults, &resultGroup, "ResourceGroup.Oid")
						return
					}
				}

				*/
			}
			Fail("Could not find some_group")
		})
		It("creates a resource group using old format for MemorySpillRatio", func() {
			// temporarily special case for 5x resource groups #temp5xResGroup
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("5.20.0") {
				Skip("Test only applicable to GPDB 5.20 and above")
			}

			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("7") {
				expectedDefaults := builtin.ResourceGroupBefore7{ResourceGroup: builtin.ResourceGroup{Oid: 1, Name: "some_group", Concurrency: concurrencyDefault, Cpuset: cpuSetDefault},
					CPURateLimit: "10", MemoryLimit: "20", MemorySharedQuota: memSharedDefault, MemorySpillRatio: "19", MemoryAuditor: memAuditDefault}

				testhelper.AssertQueryRuns(connectionPool, "CREATE RESOURCE GROUP some_group WITH (CPU_RATE_LIMIT=10, MEMORY_LIMIT=20, MEMORY_SPILL_RATIO=19);")
				defer testhelper.AssertQueryRuns(connectionPool, `DROP RESOURCE GROUP some_group`)

				resultResourceGroups := builtin.GetResourceGroups[builtin.ResourceGroupBefore7](connectionPool)

				for _, resultGroup := range resultResourceGroups {
					if resultGroup.Name == "some_group" {
						structmatcher.ExpectStructsToMatchExcluding(&expectedDefaults, &resultGroup, "ResourceGroup.Oid")
						return
					}
				}
			} else { // GPDB7+
				return
				/* comment out, due to CBDB/PG14 - GP7/PG12 behavior diff
				expectedDefaults := builtin.ResourceGroupAtLeast7{ResourceGroup: builtin.ResourceGroup{Oid: 1, Name: "some_group", Concurrency: concurrencyDefault, Cpuset: cpuSetDefault},
					CpuMaxPercent: "10", CpuSoftPriority: "100"}

				testhelper.AssertQueryRuns(connectionPool, "CREATE RESOURCE GROUP some_group WITH (CPU_MAX_PERCENT=10, CPU_WEIGHT=100);")
				defer testhelper.AssertQueryRuns(connectionPool, `DROP RESOURCE GROUP some_group`)

				resultResourceGroups := builtin.GetResourceGroups[builtin.ResourceGroupAtLeast7](connectionPool)

				for _, resultGroup := range resultResourceGroups {
					if resultGroup.Name == "some_group" {
						structmatcher.ExpectStructsToMatchExcluding(&expectedDefaults, &resultGroup, "ResourceGroup.Oid")
						return
					}
				}

				*/
			}
			Fail("Could not find some_group")
		})
		It("alters a default resource group", func() {
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("7") {
				defaultGroup := builtin.ResourceGroupBefore7{ResourceGroup: builtin.ResourceGroup{Oid: 1, Name: "default_group", Concurrency: "15", Cpuset: "-1"},
					CPURateLimit: "10", MemoryLimit: "20", MemorySharedQuota: "25", MemorySpillRatio: "30", MemoryAuditor: "0"}
				emptyMetadataMap := map[builtin.UniqueID]builtin.ObjectMetadata{}

				builtin.PrintCreateResourceGroupStatementsBefore7(backupfile, tocfile, []builtin.ResourceGroupBefore7{defaultGroup}, emptyMetadataMap)

				hunks := regexp.MustCompile(";\n\n").Split(buffer.String(), 5)
				for i := 0; i < 5; i++ {
					testhelper.AssertQueryRuns(connectionPool, hunks[i])
				}
				resultResourceGroups := builtin.GetResourceGroups[builtin.ResourceGroupBefore7](connectionPool)

				for _, resultGroup := range resultResourceGroups {
					if resultGroup.Name == "default_group" {
						structmatcher.ExpectStructsToMatchExcluding(&defaultGroup, &resultGroup, "ResourceGroup.Oid")
						return
					}
				}
			} else {
				return
				/* comment out, due to CBDB/PG14 - GP7/PG12 behavior diff
				defaultGroup := builtin.ResourceGroupAtLeast7{ResourceGroup: builtin.ResourceGroup{Oid: 1, Name: "default_group", Concurrency: "15", Cpuset: "-1"},
					CpuMaxPercent: "10", CpuSoftPriority: "100"}
				emptyMetadataMap := map[builtin.UniqueID]builtin.ObjectMetadata{}

				builtin.PrintCreateResourceGroupStatementsAtLeast7(backupfile, tocfile, []builtin.ResourceGroupAtLeast7{defaultGroup}, emptyMetadataMap)

				hunks := regexp.MustCompile(";\n\n").Split(buffer.String(), 5)
				for i := 0; i < 3; i++ {
					testhelper.AssertQueryRuns(connectionPool, hunks[i])
				}
				resultResourceGroups := builtin.GetResourceGroups[builtin.ResourceGroupAtLeast7](connectionPool)

				for _, resultGroup := range resultResourceGroups {
					if resultGroup.Name == "default_group" {
						structmatcher.ExpectStructsToMatchExcluding(&defaultGroup, &resultGroup, "ResourceGroup.Oid")
						return
					}
				}

				*/
			}
			Fail("Could not find default_group")
		})
	})
	Describe("PrintCreateRoleStatements", func() {
		var role1 builtin.Role
		BeforeEach(func() {
			role1 = builtin.Role{
				Oid:             1,
				Name:            "role1",
				Super:           true,
				Inherit:         false,
				CreateRole:      false,
				CreateDB:        false,
				CanLogin:        false,
				ConnectionLimit: -1,
				Password:        "",
				ValidUntil:      "",
				ResQueue:        "pg_default",
				ResGroup:        "default_group",
				Createrexthttp:  false,
				Createrextgpfd:  false,
				Createwextgpfd:  false,
				Createrexthdfs:  false,
				Createwexthdfs:  false,
				TimeConstraints: nil,
			}
		})
		emptyMetadataMap := builtin.MetadataMap{}
		It("creates a basic role", func() {
			if connectionPool.Version.IsGPDB() && connectionPool.Version.Before("5") {
				role1.ResGroup = ""
			}

			builtin.PrintCreateRoleStatements(backupfile, tocfile, []builtin.Role{role1}, emptyMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE "role1"`)
			role1.Oid = testutils.OidFromObjectName(connectionPool, "", "role1", builtin.TYPE_ROLE)

			resultRoles := builtin.GetRoles(connectionPool)
			for _, role := range resultRoles {
				if role.Name == "role1" {
					structmatcher.ExpectStructsToMatch(&role1, role)
					return
				}
			}
			Fail("Role 'role1' was not found")
		})
		It("creates a role with all attributes", func() {
			role1 := builtin.Role{
				Oid:             1,
				Name:            "role1",
				Super:           false,
				Inherit:         true,
				CreateRole:      true,
				CreateDB:        true,
				CanLogin:        true,
				ConnectionLimit: 4,
				Password:        "md5a8b2c77dfeba4705f29c094592eb3369",
				ValidUntil:      "2099-01-01 08:00:00-00",
				ResQueue:        "pg_default",
				ResGroup:        "",
				Createrexthttp:  true,
				Createrextgpfd:  true,
				Createwextgpfd:  true,
				Createrexthdfs:  true,
				Createwexthdfs:  true,
				TimeConstraints: []builtin.TimeConstraint{
					{
						Oid:       0,
						StartDay:  0,
						StartTime: "13:30:00",
						EndDay:    3,
						EndTime:   "14:30:00",
					}, {
						Oid:       0,
						StartDay:  5,
						StartTime: "00:00:00",
						EndDay:    5,
						EndTime:   "24:00:00",
					},
				},
			}
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("5")) || connectionPool.Version.IsCBDB() {
				role1.ResGroup = "default_group"
			}
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDB() {
				role1.Createrexthdfs = false
				role1.Createwexthdfs = false
			}
			metadataMap := testutils.DefaultMetadataMap("ROLE", false, false, true, includeSecurityLabels)

			builtin.PrintCreateRoleStatements(backupfile, tocfile, []builtin.Role{role1}, metadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE "role1"`)
			role1.Oid = testutils.OidFromObjectName(connectionPool, "", "role1", builtin.TYPE_ROLE)

			resultRoles := builtin.GetRoles(connectionPool)
			for _, role := range resultRoles {
				if role.Name == "role1" {
					structmatcher.ExpectStructsToMatchExcluding(&role1, role, "TimeConstraints.Oid")
					return
				}
			}
			Fail("Role 'role1' was not found")
		})
		It("creates a role with replication", func() {
			testutils.SkipIfBefore6(connectionPool)

			role1.Replication = true
			builtin.PrintCreateRoleStatements(backupfile, tocfile, []builtin.Role{role1}, emptyMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE "role1"`)
			role1.Oid = testutils.OidFromObjectName(connectionPool, "", "role1", builtin.TYPE_ROLE)

			resultRoles := builtin.GetRoles(connectionPool)
			for _, role := range resultRoles {
				if role.Name == "role1" {
					structmatcher.ExpectStructsToMatch(&role1, role)
					return
				}
			}
			Fail("Role 'role1' was not found")
		})
	})
	Describe("PrintRoleMembershipStatements", func() {
		BeforeEach(func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE ROLE usergroup`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE ROLE testuser`)
		})
		AfterEach(func() {
			defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE usergroup`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE testuser`)
		})
		It("grants a role without ADMIN OPTION", func() {
			numRoleMembers := len(builtin.GetRoleMembers(connectionPool))
			expectedRoleMember := builtin.RoleMember{Role: "usergroup", Member: "testuser", Grantor: "testrole", IsAdmin: false}
			builtin.PrintRoleMembershipStatements(backupfile, tocfile, []builtin.RoleMember{expectedRoleMember})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultRoleMembers := builtin.GetRoleMembers(connectionPool)
			Expect(resultRoleMembers).To(HaveLen(numRoleMembers + 1))
			for _, roleMember := range resultRoleMembers {
				if roleMember.Role == "usergroup" {
					structmatcher.ExpectStructsToMatch(&expectedRoleMember, &roleMember)
					return
				}
			}
			Fail("Role 'testuser' is not a member of role 'usergroup'")
		})
		It("grants a role WITH ADMIN OPTION", func() {
			numRoleMembers := len(builtin.GetRoleMembers(connectionPool))
			expectedRoleMember := builtin.RoleMember{Role: "usergroup", Member: "testuser", Grantor: "testrole", IsAdmin: true}
			builtin.PrintRoleMembershipStatements(backupfile, tocfile, []builtin.RoleMember{expectedRoleMember})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultRoleMembers := builtin.GetRoleMembers(connectionPool)
			Expect(resultRoleMembers).To(HaveLen(numRoleMembers + 1))
			for _, roleMember := range resultRoleMembers {
				if roleMember.Role == "usergroup" {
					structmatcher.ExpectStructsToMatch(&expectedRoleMember, &roleMember)
					return
				}
			}
			Fail("Role 'testuser' is not a member of role 'usergroup'")
		})
	})
	Describe("PrintRoleGUCStatements", func() {
		BeforeEach(func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE ROLE testuser`)
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, `DROP ROLE testuser`)
		})
		It("Sets GUCs for a particular role", func() {
			defaultStorageOptionsString := "appendonly=true, compresslevel=6, orientation=row, compresstype=none"
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDB() {
				defaultStorageOptionsString = "compresslevel=6, compresstype=none"
			}

			roleConfigMap := map[string][]builtin.RoleGUC{
				"testuser": {
					{RoleName: "testuser", Config: fmt.Sprintf("SET gp_default_storage_options TO '%s'", defaultStorageOptionsString)},
					{RoleName: "testuser", Config: "SET search_path TO public"}},
			}

			builtin.PrintRoleGUCStatements(backupfile, tocfile, roleConfigMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultGUCs := builtin.GetRoleGUCs(connectionPool)
			Expect(resultGUCs["testuser"]).To(ConsistOf(roleConfigMap["testuser"]))
		})
		It("Sets GUCs for a role in a particular database", func() {
			testutils.SkipIfBefore6(connectionPool)

			defaultStorageOptionsString := "appendonly=true, compresslevel=6, orientation=row, compresstype=none"
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("7")) || connectionPool.Version.IsCBDB() {
				defaultStorageOptionsString = "compresslevel=6, compresstype=none"
			}

			roleConfigMap := map[string][]builtin.RoleGUC{
				"testuser": {
					{RoleName: "testuser", DbName: "testdb", Config: fmt.Sprintf("SET gp_default_storage_options TO '%s'", defaultStorageOptionsString)},
					{RoleName: "testuser", DbName: "testdb", Config: "SET search_path TO public"}},
			}

			builtin.PrintRoleGUCStatements(backupfile, tocfile, roleConfigMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultGUCs := builtin.GetRoleGUCs(connectionPool)
			Expect(resultGUCs["testuser"]).To(ConsistOf(roleConfigMap["testuser"]))
		})
	})
	Describe("PrintCreateTablespaceStatements", func() {
		var expectedTablespace builtin.Tablespace
		BeforeEach(func() {
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDB() {
				expectedTablespace = builtin.Tablespace{Oid: 1, Tablespace: "test_tablespace", FileLocation: "'/tmp/test_dir'", SegmentLocations: []string{}}
			} else {
				expectedTablespace = builtin.Tablespace{Oid: 1, Tablespace: "test_tablespace", FileLocation: "test_dir"}
			}
		})
		It("creates a basic tablespace", func() {
			numTablespaces := len(builtin.GetTablespaces(connectionPool))
			emptyMetadataMap := builtin.MetadataMap{}
			builtin.PrintCreateTablespaceStatements(backupfile, tocfile, []builtin.Tablespace{expectedTablespace}, emptyMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLESPACE test_tablespace")

			resultTablespaces := builtin.GetTablespaces(connectionPool)
			Expect(resultTablespaces).To(HaveLen(numTablespaces + 1))
			for _, tablespace := range resultTablespaces {
				if tablespace.Tablespace == "test_tablespace" {
					structmatcher.ExpectStructsToMatchExcluding(&expectedTablespace, &tablespace, "Oid")
					return
				}
			}
			Fail("Tablespace 'test_tablespace' was not created")
		})
		It("creates a basic tablespace with different segment locations and options", func() {
			testutils.SkipIfBefore6(connectionPool)

			expectedTablespace = builtin.Tablespace{
				Oid: 1, Tablespace: "test_tablespace", FileLocation: "'/tmp/test_dir'",
				SegmentLocations: []string{"content0='/tmp/test_dir1'", "content1='/tmp/test_dir2'"},
				Options:          "seq_page_cost=123",
			}
			numTablespaces := len(builtin.GetTablespaces(connectionPool))
			emptyMetadataMap := builtin.MetadataMap{}
			builtin.PrintCreateTablespaceStatements(backupfile, tocfile, []builtin.Tablespace{expectedTablespace}, emptyMetadataMap)

			gbuffer := BufferWithBytes([]byte(buffer.String()))
			entries, _ := testutils.SliceBufferByEntries(tocfile.GlobalEntries, gbuffer)
			create, setOptions := entries[0], entries[1]
			testhelper.AssertQueryRuns(connectionPool, create)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLESPACE test_tablespace")
			testhelper.AssertQueryRuns(connectionPool, setOptions)

			resultTablespaces := builtin.GetTablespaces(connectionPool)
			Expect(resultTablespaces).To(HaveLen(numTablespaces + 1))
			for _, tablespace := range resultTablespaces {
				if tablespace.Tablespace == "test_tablespace" {
					structmatcher.ExpectStructsToMatchExcluding(&expectedTablespace, &tablespace, "Oid")
					return
				}
			}
			Fail("Tablespace 'test_tablespace' was not created")
		})
		It("creates a tablespace with permissions, an owner, security label, and a comment", func() {
			numTablespaces := len(builtin.GetTablespaces(connectionPool))
			tablespaceMetadataMap := testutils.DefaultMetadataMap("TABLESPACE", true, true, true, includeSecurityLabels)
			tablespaceMetadata := tablespaceMetadataMap[expectedTablespace.GetUniqueID()]
			builtin.PrintCreateTablespaceStatements(backupfile, tocfile, []builtin.Tablespace{expectedTablespace}, tablespaceMetadataMap)

			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDB() {
				/*
				 * In GPDB 6 and later, a CREATE TABLESPACE statement can't be run in a multi-command string
				 * with other statements, so we execute it separately from the metadata statements.
				 */
				gbuffer := BufferWithBytes([]byte(buffer.String()))
				entries, _ := testutils.SliceBufferByEntries(tocfile.GlobalEntries, gbuffer)
				for _, entry := range entries {
					testhelper.AssertQueryRuns(connectionPool, entry)
				}
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLESPACE test_tablespace")
			} else {
				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLESPACE test_tablespace")
			}

			resultTablespaces := builtin.GetTablespaces(connectionPool)
			resultMetadataMap := builtin.GetMetadataForObjectType(connectionPool, builtin.TYPE_TABLESPACE)
			Expect(resultTablespaces).To(HaveLen(numTablespaces + 1))
			for _, tablespace := range resultTablespaces {
				if tablespace.Tablespace == "test_tablespace" {
					resultMetadata := resultMetadataMap[tablespace.GetUniqueID()]
					structmatcher.ExpectStructsToMatchExcluding(&tablespaceMetadata, &resultMetadata, "Oid")
					structmatcher.ExpectStructsToMatchExcluding(&expectedTablespace, &tablespace, "Oid")
					return
				}
			}
			Fail("Tablespace 'test_tablespace' was not created")
		})
	})
})
