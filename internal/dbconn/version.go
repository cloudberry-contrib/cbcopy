package dbconn

import (
	"regexp"
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
	HDW            // HashData Database
	PGSQL          // PostgreSQL
)

const (
	gpdbPattern  = `\(Greenplum Database ([0-9]+\.[0-9]+\.[0-9]+)[^)]*\)`
	cbdbPattern  = `\(Cloudberry Database ([0-9]+\.[0-9]+\.[0-9]+)[^)]*\)`
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
	if ver, ok := dbversion.extractVersion(cbdbPattern); ok {
		dbversion.Type = CBDB
		dbversion.SemVer = ver
	} else if ver, ok := dbversion.extractVersion(gpdbPattern); ok {
		dbversion.Type = GPDB
		dbversion.SemVer = ver
	} else if ver, ok := dbversion.extractVersion(hdwPattern); ok {
		dbversion.Type = HDW
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

func (dbversion GPDBVersion) Before(targetVersion string) bool {
	validRange := dbversion.StringToSemVerRange("<" + targetVersion)
	return validRange(dbversion.SemVer)
}

func (dbversion GPDBVersion) AtLeast(targetVersion string) bool {
	validRange := dbversion.StringToSemVerRange(">=" + targetVersion)
	return validRange(dbversion.SemVer)
}

func (dbversion GPDBVersion) Is(targetVersion string) bool {
	validRange := dbversion.StringToSemVerRange("==" + targetVersion)
	return validRange(dbversion.SemVer)
}

func (dbversion GPDBVersion) IsCBDB() bool {
	return dbversion.Type == CBDB
}

func (dbversion GPDBVersion) IsGPDB() bool {
	return dbversion.Type == GPDB
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
