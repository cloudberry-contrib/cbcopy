package dbconn

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"

	"github.com/blang/semver"
)

// DBType represents the type of database
type DBType int

const (
	Unknown DBType = iota
	GPDB           // Greenplum Database
	CBDB           // Cloudberry Database
	CBEDB          // Cloudberry Enterprise Database
	ACBDB          // Apache Cloudberry Database
	HDW            // HashData Database
	PGSQL          // PostgreSQL
)

const (
	gpdbPattern  = `\(Greenplum Database ([0-9]+\.[0-9]+\.[0-9]+)[^)]*\)`
	cbdbPattern  = `\(Cloudberry Database ([0-9]+\.[0-9]+\.[0-9]+)[^)]*\)`
	cbedbPattern = `\(HashData Enterprise ([0-9]+\.[0-9]+\.[0-9]+)[^)]*\)`
	acbdbPattern = `\(Apache Cloudberry ([0-9]+\.[0-9]+\.[0-9]+)[^)]*\)`
	hdwPattern   = `\(HashData Warehouse ([0-9]+\.[0-9]+\.[0-9]+)[^)]*\)`
	pgsqlPattern = `^PostgreSQL\s+([0-9]+\.[0-9]+\.[0-9]+)`
)

// String provides string representation of DBType
func (t DBType) String() string {
	switch t {
	case GPDB:
		return "Greenplum Database"
	case CBDB:
		return "Cloudberry Database"
	case CBEDB:
		return "Cloudberry Enterprise Database"
	case ACBDB:
		return "Apache Cloudberry Database"
	case HDW:
		return "HashData Database"
	case PGSQL:
		return "PostgreSQL"
	default:
		return "Unknown Database"
	}
}

// GPDBVersion represents version information for a database
type GPDBVersion struct {
	VersionString string
	SemVer        semver.Version
	Type          DBType
}

/*
 * This constructor is intended as a convenience function for testing and
 * setting defaults; the dbconn.Connect function will automatically initialize
 * the version of the database to which it is connecting.
 *
 * The versionStr argument here should be a semantic version in the form X.Y.Z,
 * not a GPDB version string like the one returned by "SELECT version()".  If
 * an invalid semantic version is passed, that is considered programmer error
 * and the function will panic.
 */
func NewVersion(versionStr string) GPDBVersion {
	version := GPDBVersion{
		VersionString: versionStr,
		SemVer:        semver.MustParse(versionStr),
		Type:          GPDB,
	}
	return version
}

// Version string patterns

// InitializeVersion parses database version string and returns version information
// Version string samples for different databases:
//
// HashData:
//
//	PostgreSQL 9.4.26 (Greenplum Database 6.20.0 build 9999) (HashData Warehouse 3.13.8 build 27594)
//	on x86_64-unknown-linux-gnu, compiled by gcc (GCC) 10.2.1 20210130 (Red Hat 10.2.1-11), 64-bit compiled on Feb 10 2023 17:22:03
//
// Cloudberry:
//
//	PostgreSQL 14.4 (Cloudberry Database 1.2.0 build commit:5b5ae3f8aa638786f01bbd08307b6474a1ba1997)
//	on x86_64-pc-linux-gnu, compiled by gcc (GCC) 10.2.1 20210130 (Red Hat 10.2.1-11), 64-bit compiled on Feb 16 2023 23:44:39
//
// Cloudberry Enterprise:
//
//	PostgreSQL 14.4 (Cloudberry Database 1.1.5 build dev) (HashData Enterprise 1.1.5 build dev)
//	on aarch64-unknown-linux-gnu, compiled by gcc (GCC) 10.2.1 20210130 (Red Hat 10.2.1-11), 64-bit compiled on Feb 13 2025 14:37:41
//
// Apache Cloudberry:
//
//	PostgreSQL 14.4 (Apache Cloudberry 2.0.0 build commit:a071e3f8aa638786f01bbd08307b6474a1ba7890)
//	on x86_64-pc-linux-gnu, compiled by gcc (GCC) 10.2.1 20210130 (Red Hat 10.2.1-11), 64-bit compiled on Jun 10 2025 15:42:54
//
// Greenplum:
//
//	PostgreSQL 12.12 (Greenplum Database 7.0.0 build commit:bf073b87c0bac9759631746dca1c4c895a304afb)
//	on x86_64-pc-linux-gnu, compiled by gcc (GCC) 10.2.1 20210130 (Red Hat 10.2.1-11), 64-bit compiled on May  6 2023 16:12:25
//
// PostgreSQL:
//
//	PostgreSQL 12.18 on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 9.4.0-1ubuntu1~20.04.2) 9.4.0, 64-bit
func InitializeVersion(dbconn *DBConn) (dbversion GPDBVersion, err error) {
	err = dbconn.Get(&dbversion, "SELECT version() AS versionstring")
	if err != nil {
		return
	}

	// Determine database type and parse version
	dbversion.ParseVersionInfo(dbversion.VersionString)

	gplog.Info("Initialized database version - Full Version: %s, Database Type: %s, Semantic Version: %s",
		dbversion.VersionString, dbversion.Type, dbversion.SemVer)
	return
}

func (dbversion *GPDBVersion) ParseVersionInfo(versionString string) {
	dbversion.VersionString = versionString
	dbversion.Type = Unknown

	// Try to match each database type
	if ver, ok := dbversion.extractVersion(cbedbPattern); ok {
		dbversion.Type = CBEDB
		dbversion.SemVer = ver
	} else if ver, ok := dbversion.extractVersion(cbdbPattern); ok {
		dbversion.Type = CBDB
		dbversion.SemVer = ver
	} else if ver, ok := dbversion.extractVersion(acbdbPattern); ok {
		dbversion.Type = ACBDB
		dbversion.SemVer = ver
	} else if ver, ok := dbversion.extractVersion(hdwPattern); ok {
		dbversion.Type = HDW
		dbversion.SemVer = ver
	} else if ver, ok := dbversion.extractVersion(gpdbPattern); ok {
		dbversion.Type = GPDB
		dbversion.SemVer = ver
	} else if ver, ok := dbversion.extractVersion(pgsqlPattern); ok {
		dbversion.Type = PGSQL
		dbversion.SemVer = ver
	}
}

func (dbversion GPDBVersion) extractVersion(pattern string) (semver.Version, bool) {
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(dbversion.VersionString)
	if len(matches) < 2 {
		return semver.Version{}, false
	}

	ver, err := semver.Make(matches[1])
	if err != nil {
		return semver.Version{}, false
	}
	return ver, true
}

func (dbversion GPDBVersion) StringToSemVerRange(versionStr string) semver.Range {
	numDigits := len(strings.Split(versionStr, "."))
	if numDigits < 3 {
		versionStr += ".x"
	}
	validRange := semver.MustParseRange(versionStr)
	return validRange
}

// getComparableSemVer returns a version object suitable for comparison.
// It maps HDW versions to corresponding GPDB major versions for compatibility checks,
// but only when comparing against a target version that appears to be a GPDB version (e.g., major version >= 4).
// This allows for native version checks for HDW-specific features (e.g., AtLeast("3.1.0")).
func (dbversion GPDBVersion) getComparableSemVer(targetVersion string) semver.Version {
	v := dbversion.SemVer

	if !dbversion.IsHDW() {
		return v
	}

	// Heuristic to decide if we're comparing against a GPDB version.
	// We only map HDW versions if the target comparison version is for GPDB 4 or greater.
	targetMajorStr := strings.Split(targetVersion, ".")[0]
	targetMajor, err := strconv.Atoi(targetMajorStr)
	// If targetVersion is not a valid number string, we don't map.
	if err != nil {
		return v
	}

	// Based on product versioning, HDW has a max major version of 3,
	// while GPDB has a min major version of 4. Therefore, any target
	// version >= 4 is considered a check against GPDB features.
	if targetMajor >= 4 {
		switch v.Major {
		// HDW 2.x is compatible with GPDB 5.x
		case 2:
			v.Major = 5
		// HDW 3.x is compatible with GPDB 6.x
		case 3:
			v.Major = 6
		}
	}
	return v
}

func (dbversion GPDBVersion) Before(targetVersion string) bool {
	validRange := dbversion.StringToSemVerRange("<" + targetVersion)
	return validRange(dbversion.getComparableSemVer(targetVersion))
}

func (dbversion GPDBVersion) AtLeast(targetVersion string) bool {
	validRange := dbversion.StringToSemVerRange(">=" + targetVersion)
	return validRange(dbversion.getComparableSemVer(targetVersion))
}

func (dbversion GPDBVersion) Is(targetVersion string) bool {
	validRange := dbversion.StringToSemVerRange("==" + targetVersion)
	return validRange(dbversion.getComparableSemVer(targetVersion))
}

func (dbversion GPDBVersion) IsCBDB() bool {
	return dbversion.Type == CBDB
}

func (dbversion GPDBVersion) IsCBEDB() bool {
	return dbversion.Type == CBEDB
}

func (dbversion GPDBVersion) IsACBDB() bool {
	return dbversion.Type == ACBDB
}

func (dbversion GPDBVersion) IsCBDBFamily() bool {
	return dbversion.IsCBDB() || dbversion.IsCBEDB() || dbversion.IsACBDB()
}

func (dbversion GPDBVersion) IsGPDB() bool {
	// we treat HDW as GPDB for now
	return dbversion.Type == GPDB || dbversion.Type == HDW
}

func (dbversion GPDBVersion) IsHDW() bool {
	return dbversion.Type == HDW
}

func (dbversion GPDBVersion) IsPGSQL() bool {
	return dbversion.Type == PGSQL
}

func (srcVersion GPDBVersion) Equals(destVersion GPDBVersion) bool {
	if srcVersion.Type != destVersion.Type {
		return false
	}

	return srcVersion.SemVer.Major == destVersion.SemVer.Major
}
