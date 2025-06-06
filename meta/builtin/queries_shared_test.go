package builtin_test

import (
	"database/sql"
	"database/sql/driver"

	// "github.com/greenplum-db/gpbackup/backup"
	"github.com/cloudberrydb/cbcopy/meta/builtin"

	// "github.com/greenplum-db/gpbackup/testutils"
	"github.com/cloudberrydb/cbcopy/testutils"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/queries_shared tests", func() {
	Describe("GetConstraints", func() {
		It("GetConstraints properly handles NULL constraint definitions", func() {
			if (connectionPool.Version.IsGPDB() && connectionPool.Version.AtLeast("6")) || connectionPool.Version.IsCBDBFamily() {
				Skip("Test does not apply for GPDB versions after 5")
			}

			header := []string{"oid", "schema", "name", "contype", "def", "conislocal", "owningobject", "isdomainconstraint", "ispartitionparent"}
			rowOne := []driver.Value{"1", "mock_schema", "mock_table", "mock_contype", "mock_condef", false, "mock_owningobject", false, false}
			rowTwo := []driver.Value{"2", "mock_schema2", "mock_table2", "mock_contype2", nil, false, "mock_owningobject2", false, false}
			fakeRows := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(fakeRows)
			result := builtin.GetConstraints(connectionPool)

			// Expect the GetConstraints function to return only the 1st row since the 2nd row has a NULL constraint definition
			expectedResult := []builtin.Constraint{{Oid: 1, Schema: "mock_schema", Name: "mock_table", ConType: "mock_contype",
				Def: sql.NullString{String: "mock_condef", Valid: true}, ConIsLocal: false, OwningObject: "mock_owningobject",
				IsDomainConstraint: false, IsPartitionParent: false}}
			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatch(&expectedResult[0], &result[0])
		})
	})
	Describe("RenameExchangedPartitionConstraints", func() {
		It("RenameExchangedPartitionConstraints properly renames constraints and their definitions", func() {
			testutils.SkipIfBefore7(connectionPool)

			constraints := []builtin.Constraint{
				{Oid: 1, Schema: "mock_schema", Name: "mock_constraint", ConType: "p", Def: sql.NullString{String: "PRIMARY KEY (a, b)", Valid: true},
					ConIsLocal: true, OwningObject: "mock_table", IsDomainConstraint: false, IsPartitionParent: true},
				{Oid: 2, Schema: "mock_schema", Name: "part_table_for_upgrade2_pkey", ConType: "p", Def: sql.NullString{String: "PRIMARY KEY (a, b)", Valid: true},
					ConIsLocal: true, OwningObject: "mock_table", IsDomainConstraint: false, IsPartitionParent: true}}
			header := []string{"origname", "newname"}
			rowOne := []driver.Value{"part_table_for_upgrade2_pkey", "like_table2_pkey"}
			fakeRows := sqlmock.NewRows(header).AddRow(rowOne...)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(fakeRows)
			builtin.RenameExchangedPartitionConstraints(connectionPool, &constraints)

			Expect(constraints).To(HaveLen(2))
			for _, idx := range constraints {
				switch idx.Oid {
				case 1:
					Expect(idx.Name).To(Equal("mock_constraint"))
					Expect(idx.Def.String).To(Equal("PRIMARY KEY (a, b)"))
				case 2:
					Expect(idx.Name).To(Equal("like_table2_pkey"))
					Expect(idx.Def.String).To(Equal("PRIMARY KEY (a, b)"))
				default:
					Fail("Unexpected index OID found")
				}
			}

		})
	})
})
