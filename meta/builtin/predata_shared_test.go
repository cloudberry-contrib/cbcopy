package builtin_test

import (
	"database/sql"

	// "github.com/greenplum-db/gpbackup/backup"
	"github.com/cloudberry-contrib/cbcopy/meta/builtin"

	// "github.com/greenplum-db/gpbackup/testutils"
	"github.com/cloudberry-contrib/cbcopy/testutils"

	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("backup/predata_shared tests", func() {
	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintConstraintStatement", func() {
		var (
			uniqueOne        builtin.Constraint
			uniqueTwo        builtin.Constraint
			uniqueNotValid   builtin.Constraint
			primarySingle    builtin.Constraint
			primaryComposite builtin.Constraint
			foreignOne       builtin.Constraint
			foreignTwo       builtin.Constraint
			checkConstraint  builtin.Constraint

			objectMetadata builtin.ObjectMetadata
		)
		BeforeEach(func() {
			uniqueOne = builtin.Constraint{Oid: 1, Name: "tablename_i_key", ConType: "u", Def: sql.NullString{String: "UNIQUE (i)", Valid: true}, OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false}
			uniqueTwo = builtin.Constraint{Oid: 0, Name: "tablename_j_key", ConType: "u", Def: sql.NullString{String: "UNIQUE (j)", Valid: true}, OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false}
			uniqueNotValid = builtin.Constraint{Oid: 1, Name: "tablename_k_key", ConType: "u", Def: sql.NullString{String: "UNIQUE (k) NOT VALID", Valid: true}, OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false}
			primarySingle = builtin.Constraint{Oid: 0, Name: "tablename_pkey", ConType: "p", Def: sql.NullString{String: "PRIMARY KEY (i)", Valid: true}, OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false}
			primaryComposite = builtin.Constraint{Oid: 0, Name: "tablename_pkey", ConType: "p", Def: sql.NullString{String: "PRIMARY KEY (i, j)", Valid: true}, OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false}
			foreignOne = builtin.Constraint{Oid: 0, Name: "tablename_i_fkey", ConType: "f", Def: sql.NullString{String: "FOREIGN KEY (i) REFERENCES other_tablename(a)", Valid: true}, OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false}
			foreignTwo = builtin.Constraint{Oid: 0, Name: "tablename_j_fkey", ConType: "f", Def: sql.NullString{String: "FOREIGN KEY (j) REFERENCES other_tablename(b)", Valid: true}, OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false}
			checkConstraint = builtin.Constraint{Oid: 0, Name: "check1", ConType: "c", Def: sql.NullString{String: "CHECK (VALUE <> 42::numeric)", Valid: true}, OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false, ConIsLocal: true}

			objectMetadata = testutils.DefaultMetadata("CONSTRAINT", false, false, false, false)
		})

		Context("Constraints involving different columns", func() {
			It("prints an ADD CONSTRAINT statement for one UNIQUE constraint with a comment", func() {
				withCommentMetadata := testutils.DefaultMetadata("CONSTRAINT", false, false, true, false)
				builtin.PrintConstraintStatement(backupfile, tocfile, uniqueOne, withCommentMetadata)
				testutils.ExpectEntry(tocfile.PredataEntries, 0, "", "public.tablename", "tablename_i_key", "CONSTRAINT")
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer,
					"ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);",
					"COMMENT ON CONSTRAINT tablename_i_key ON public.tablename IS 'This is a constraint comment.';")
			})
			It("prints an ADD CONSTRAINT statement for one UNIQUE constraint", func() {
				builtin.PrintConstraintStatement(backupfile, tocfile, uniqueOne, objectMetadata)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`)
			})
			It("prints ADD CONSTRAINT statements for two UNIQUE constraints", func() {
				builtin.PrintConstraintStatement(backupfile, tocfile, uniqueOne, objectMetadata)
				builtin.PrintConstraintStatement(backupfile, tocfile, uniqueTwo, objectMetadata)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_key UNIQUE (j);`)
			})
			It("prints an ADD CONSTRAINT statement in Postdata section for one UNIQUE constraint with a NOT VALID clause", func() {
				builtin.PrintConstraintStatement(backupfile, tocfile, uniqueNotValid, objectMetadata)
				testutils.ExpectEntryCount(tocfile.PredataEntries, 0)
				testutils.ExpectEntryCount(tocfile.PostdataEntries, 1)
				testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, `ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_k_key UNIQUE (k) NOT VALID;`)
			})
			It("prints an ADD CONSTRAINT statement for one PRIMARY KEY constraint on one column", func() {
				builtin.PrintConstraintStatement(backupfile, tocfile, primarySingle, objectMetadata)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);`)
			})
			It("prints an ADD CONSTRAINT statement for one composite PRIMARY KEY constraint on two columns", func() {
				builtin.PrintConstraintStatement(backupfile, tocfile, primaryComposite, objectMetadata)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);`)
			})
			It("prints an ADD CONSTRAINT statement for one FOREIGN KEY constraint", func() {
				builtin.PrintConstraintStatement(backupfile, tocfile, foreignOne, objectMetadata)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);`)
			})
			It("prints ADD CONSTRAINT statements for two FOREIGN KEY constraints", func() {
				builtin.PrintConstraintStatement(backupfile, tocfile, foreignOne, objectMetadata)
				builtin.PrintConstraintStatement(backupfile, tocfile, foreignTwo, objectMetadata)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);`)
			})
			It("prints ADD CONSTRAINT statements for one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				builtin.PrintConstraintStatement(backupfile, tocfile, uniqueOne, objectMetadata)
				builtin.PrintConstraintStatement(backupfile, tocfile, foreignTwo, objectMetadata)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);`)
			})
			It("prints ADD CONSTRAINT statements for one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				builtin.PrintConstraintStatement(backupfile, tocfile, primarySingle, objectMetadata)
				builtin.PrintConstraintStatement(backupfile, tocfile, foreignTwo, objectMetadata)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);`)
			})
			It("prints ADD CONSTRAINT statements for one two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				builtin.PrintConstraintStatement(backupfile, tocfile, primaryComposite, objectMetadata)
				builtin.PrintConstraintStatement(backupfile, tocfile, foreignTwo, objectMetadata)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);`)
			})
		})
		Context("Constraints involving the same column", func() {
			It("prints ADD CONSTRAINT statements for one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				builtin.PrintConstraintStatement(backupfile, tocfile, uniqueOne, objectMetadata)
				builtin.PrintConstraintStatement(backupfile, tocfile, foreignOne, objectMetadata)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);`)
			})
			It("prints ADD CONSTRAINT statements for one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				builtin.PrintConstraintStatement(backupfile, tocfile, primarySingle, objectMetadata)
				builtin.PrintConstraintStatement(backupfile, tocfile, foreignOne, objectMetadata)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);`)
			})
			It("prints ADD CONSTRAINT statements for a two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				builtin.PrintConstraintStatement(backupfile, tocfile, primaryComposite, objectMetadata)
				builtin.PrintConstraintStatement(backupfile, tocfile, foreignOne, objectMetadata)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);`)
			})
			It("prints an ADD CONSTRAINT statement for a parent partition table", func() {
				uniqueOne.IsPartitionParent = true
				builtin.PrintConstraintStatement(backupfile, tocfile, uniqueOne, objectMetadata)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `ALTER TABLE public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`)
			})
			It("prints an ADD CONSTRAINT [name] CHECK statement without keyword ONLY for a table with descendants (another table inherits it)", func() {
				builtin.PrintConstraintStatement(backupfile, tocfile, checkConstraint, objectMetadata)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `ALTER TABLE public.tablename ADD CONSTRAINT check1 CHECK (VALUE <> 42::numeric);`)
			})
		})
	})
	Describe("PrintCreateSchemaStatements", func() {
		It("can print a basic schema", func() {
			schemas := []builtin.Schema{{Oid: 0, Name: "schemaname"}}
			emptyMetadataMap := builtin.MetadataMap{}

			builtin.PrintCreateSchemaStatements(backupfile, tocfile, schemas, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "schemaname", "", "schemaname", "SCHEMA")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, "CREATE SCHEMA schemaname;")
		})
		It("can print a schema with privileges, an owner, security label, and a comment", func() {
			schemas := []builtin.Schema{{Oid: 1, Name: "schemaname"}}
			schemaMetadataMap := testutils.DefaultMetadataMap("SCHEMA", true, true, true, true)

			builtin.PrintCreateSchemaStatements(backupfile, tocfile, schemas, schemaMetadataMap)
			expectedStatements := []string{"CREATE SCHEMA schemaname;",
				"COMMENT ON SCHEMA schemaname IS 'This is a schema comment.';",
				"ALTER SCHEMA schemaname OWNER TO testrole;",
				`REVOKE ALL ON SCHEMA schemaname FROM PUBLIC;
REVOKE ALL ON SCHEMA schemaname FROM testrole;
GRANT ALL ON SCHEMA schemaname TO testrole;`,
				"SECURITY LABEL FOR dummy ON SCHEMA schemaname IS 'unclassified';"}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
	})
	Describe("PrintAccessMethodStatements", func() {
		It("can print a user defined access rights", func() {
			accessMethods := []builtin.AccessMethod{{Oid: 1, Name: "my_access_method", Handler: "my_access_handler", Type: "t"}}
			emptyMetadataMap := builtin.MetadataMap{}

			builtin.PrintAccessMethodStatements(backupfile, tocfile, accessMethods, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "", "", "my_access_method", "ACCESS METHOD")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, "CREATE ACCESS METHOD my_access_method TYPE TABLE HANDLER my_access_handler;")
		})
	})
})
