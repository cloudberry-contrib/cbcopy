package dbconn_test

import (
	"testing"

	"github.com/blang/semver"
	"github.com/cloudberry-contrib/cbcopy/internal/dbconn"

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

var _ = Describe("internal/dbconn/version ParseVersionInfo", func() {
	parse := func(versionString string) dbconn.GPDBVersion {
		v := dbconn.GPDBVersion{}
		v.ParseVersionInfo(versionString)
		return v
	}

	DescribeTable("detects database type and semantic version",
		func(versionString string, expectedType dbconn.DBType, expectedSemVer string) {
			v := parse(versionString)
			Expect(v.Type).To(Equal(expectedType))
			Expect(v.SemVer).To(Equal(semver.MustParse(expectedSemVer)))
		},
		Entry("Apache Cloudberry Incubating (qualifier before version)",
			"PostgreSQL 14.4 (Apache Cloudberry Incubating 2.1.0) (SynxDB4 4.8.1 build 200) on x86_64-pc-linux-gnu",
			dbconn.ACBDB, "2.1.0"),
		Entry("Apache Cloudberry -incubating build",
			"PostgreSQL 14.4 (Apache Cloudberry 2.1.0-incubating build 7) on x86_64-pc-linux-gnu",
			dbconn.ACBDB, "2.1.0"),
		Entry("Apache Cloudberry (no qualifier)",
			"PostgreSQL 14.4 (Apache Cloudberry 2.0.0 build commit:a071e3f8aa638786f01bbd08307b6474a1ba7890) on x86_64-pc-linux-gnu",
			dbconn.ACBDB, "2.0.0"),
		Entry("Cloudberry Database",
			"PostgreSQL 14.4 (Cloudberry Database 1.2.0 build commit:5b5ae3f8aa638786f01bbd08307b6474a1ba1997) on x86_64-pc-linux-gnu",
			dbconn.CBDB, "1.2.0"),
		Entry("Cloudberry Enterprise (HashData Enterprise)",
			"PostgreSQL 14.4 (Cloudberry Database 1.1.5 build dev) (HashData Enterprise 1.1.5 build dev) on aarch64-unknown-linux-gnu",
			dbconn.CBEDB, "1.1.5"),
		Entry("Greenplum Database",
			"PostgreSQL 9.4.26 (Greenplum Database 6.27.1 build commit:ab5a612bfdc355ad2d601860dfb70a47778c8dd7 Open Source) on x86_64-unknown-linux-gnu",
			dbconn.GPDB, "6.27.1"),
		Entry("HashData Warehouse",
			"PostgreSQL 9.4.26 (Greenplum Database 6.20.0 build 9999) (HashData Warehouse 3.13.8 build 27594) on x86_64-unknown-linux-gnu",
			dbconn.HDW, "3.13.8"),
	)
})
