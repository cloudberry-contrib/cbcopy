package dbconn_test

import (
	"testing"

	"github.com/blang/semver"
	"github.com/cloudberrydb/cbcopy/internal/dbconn"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestVersion(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "DBConn Version HDW Mapping Suite")
}

var _ = Describe("internal/dbconn/version HDW Mapping", func() {

	// --- Test Data Setup ---
	// Represents an HDW 2.x database, which should be treated as GPDB 5.x for comparison.
	hdw2_5_0 := dbconn.GPDBVersion{
		VersionString: "PostgreSQL 9.4.26 (HashData Warehouse 2.5.0)",
		SemVer:        semver.MustParse("2.5.0"),
		Type:          dbconn.HDW,
	}

	// Represents an HDW 3.x database, which should be treated as GPDB 6.x for comparison.
	hdw3_5_0 := dbconn.GPDBVersion{
		VersionString: "PostgreSQL 9.4.26 (HashData Warehouse 3.5.0)",
		SemVer:        semver.MustParse("3.5.0"),
		Type:          dbconn.HDW,
	}

	// Represents a standard GPDB database to ensure its logic is not affected.
	gpdb7_1_0 := dbconn.GPDBVersion{
		VersionString: "PostgreSQL 12.12 (Greenplum Database 7.1.0)",
		SemVer:        semver.MustParse("7.1.0"),
		Type:          dbconn.GPDB,
	}

	Describe("AtLeast", func() {
		Context("with an HDW 3.x version (maps to GPDB 6.x)", func() {
			It("should pass native version checks (target < 4)", func() {
				Expect(hdw3_5_0.AtLeast("3.1.0")).To(BeTrue())
				Expect(hdw3_5_0.AtLeast("3.6.0")).To(BeFalse())
			})
			It("should pass mapped version checks (target >= 4)", func() {
				Expect(hdw3_5_0.AtLeast("4.0.0")).To(BeTrue())
				Expect(hdw3_5_0.AtLeast("6.0.0")).To(BeTrue())
				Expect(hdw3_5_0.AtLeast("7.0.0")).To(BeFalse())
			})
		})

		Context("with an HDW 2.x version (maps to GPDB 5.x)", func() {
			It("should pass native version checks (target < 4)", func() {
				Expect(hdw2_5_0.AtLeast("2.1.0")).To(BeTrue())
			})
			It("should pass mapped version checks (target >= 4)", func() {
				Expect(hdw2_5_0.AtLeast("5.0.0")).To(BeTrue())
				Expect(hdw2_5_0.AtLeast("6.0.0")).To(BeFalse())
			})
		})

		Context("with a standard GPDB version (no mapping)", func() {
			It("should behave normally", func() {
				Expect(gpdb7_1_0.AtLeast("7.0.0")).To(BeTrue())
				Expect(gpdb7_1_0.AtLeast("8.0.0")).To(BeFalse())
			})
		})
	})

	Describe("Is", func() {
		Context("with an HDW 3.x version (maps to GPDB 6.x)", func() {
			It("should be true for its native major version", func() {
				Expect(hdw3_5_0.Is("3")).To(BeTrue())
			})
			It("should be true for its mapped major version", func() {
				Expect(hdw3_5_0.Is("6")).To(BeTrue())
			})
			It("should be false for other major versions", func() {
				Expect(hdw3_5_0.Is("5")).To(BeFalse())
				Expect(hdw3_5_0.Is("7")).To(BeFalse())
			})
		})

		Context("with a standard GPDB version (no mapping)", func() {
			It("should behave normally", func() {
				Expect(gpdb7_1_0.Is("7")).To(BeTrue())
				Expect(gpdb7_1_0.Is("6")).To(BeFalse())
			})
		})
	})

	Describe("Before", func() {
		Context("with an HDW 3.x version (maps to GPDB 6.x)", func() {
			It("should pass native version checks (target < 4)", func() {
				Expect(hdw3_5_0.Before("3.6.0")).To(BeTrue())
				Expect(hdw3_5_0.Before("3.5.0")).To(BeFalse())
			})
			It("should pass mapped version checks (target >= 4)", func() {
				Expect(hdw3_5_0.Before("7.0.0")).To(BeTrue())
				Expect(hdw3_5_0.Before("6.0.0")).To(BeFalse())
			})
		})

		Context("with a standard GPDB version (no mapping)", func() {
			It("should behave normally", func() {
				Expect(gpdb7_1_0.Before("8.0.0")).To(BeTrue())
				Expect(gpdb7_1_0.Before("7.0.0")).To(BeFalse())
			})
		})
	})
})
