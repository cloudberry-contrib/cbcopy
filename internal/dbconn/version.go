package dbconn

import (
	"regexp"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"

	"github.com/blang/semver"
)

type GPDBVersion struct {
	VersionString string
	SemVer        semver.Version
	IsCBDB        bool
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
		IsCBDB:        false,
	}
	return version
}

func InitializeVersion(dbconn *DBConn) (dbversion, hdwversion GPDBVersion, err error) {
	err = dbconn.Get(&dbversion, "SELECT version() AS versionstring")
	if err != nil {
		return
	}

	gplog.Info("InitializeVersion, dbversion.VersionString: %v", dbversion.VersionString)

	/* version sample
		1x
	 PostgreSQL 8.2.15 (Greenplum Database 4.3.99.00 build frozen) (HashData Warehouse 1.4.0.0 build 1085) on x86_64-unknown-linux-gnu, compiled by GCC gcc (GCC) 4.8.5 20150623 (Red Hat 4.8.5-16) compiled on Jan 11 2018 18:11:55

		3x
	PostgreSQL 9.4.26 (Greenplum Database 6.20.0 build 9999) (HashData Warehouse 3.13.8 build 27594) on x86_64-unknown-linux-gnu, compiled by gcc (GCC) 10.2.1 20210130 (Red Hat 10.2.1-11), 64-bit compiled on Feb 10 2023 17:22:03

		cloudberry/cbdb
	PostgreSQL 14.4 (Cloudberry Database 1.2.0 build commit:5b5ae3f8aa638786f01bbd08307b6474a1ba1997) on x86_64-pc-linux-gnu, compiled by gcc (GCC) 10.2.1 20210130 (Red Hat 10.2.1-11), 64-bit compiled on Feb 16 2023 23:44:39

		gp6
	PostgreSQL 9.4.26 (Greenplum Database 6.21.0 build commit:d0087e3b24c54d203ca8bb315559205f13cd6393 Open Source) on x86_64-unknown-linux-gnu, compiled by gcc (GCC) 6.4.0, 64-bit compiled on Jun 10 2022 01:57:17

		gp7
	PostgreSQL 12.12 (Greenplum Database 7.0.0-beta.3 build commit:bf073b87c0bac9759631746dca1c4c895a304afb) on x86_64-pc-linux-gnu, compiled by gcc (GCC) 10.2.1 20210130 (Red Hat 10.2.1-11), 64-bit compiled on May  6 2023 16:12:25 Bhuvnesh C.
	*/

	/* get hdw version */
	validFormat := regexp.MustCompile(`^.+\(.+\) \((.+)\) on .+$`)
	sl := validFormat.FindStringSubmatch(dbversion.VersionString)
	hdwThreeDigitVersion := "0.0.0"

	if len(sl) == 2 {
		if sl = strings.Split(sl[1], " "); len(sl) > 2 {
			hdwThreeDigitVersion = sl[2]

			// if 1x, 1.4.0.0, semver.Make 会报错 "Invalid character(s) found in patch number "0.0"
			if hdwThreeDigitVersion == "1.4.0.0" {
				hdwThreeDigitVersion = "1.4.0"
			}
		}
	}

	versionStart := strings.Index(dbversion.VersionString, "(Greenplum Database ") + len("(Greenplum Database ")
	if strings.Contains(dbversion.VersionString, "Cloudberry") {
		versionStart = strings.Index(dbversion.VersionString, "(Cloudberry Database ") + len("(Cloudberry Database ")
		dbversion.IsCBDB = true
	}
	versionEnd := strings.Index(dbversion.VersionString, ")")
	dbversion.VersionString = dbversion.VersionString[versionStart:versionEnd]
	hdwversion.VersionString = hdwThreeDigitVersion

	// 1x, 4.3.99.00, below code would give us 4.3.99
	pattern := regexp.MustCompile(`\d+\.\d+\.\d+`)
	threeDigitVersion := pattern.FindStringSubmatch(dbversion.VersionString)[0]

	gplog.Info("InitializeVersion, threeDigitVersion: %v, hdwThreeDigitVersion: %v， IsCBDB: %v", threeDigitVersion, hdwThreeDigitVersion, dbversion.IsCBDB)

	dbversion.SemVer, err = semver.Make(threeDigitVersion)
	if hdwThreeDigitVersion == "2.x" {
		hdwThreeDigitVersionWorkaround := "2.5.6"
		gplog.Info("InitializeVersion, workaround, hdwThreeDigitVersion: %v --> %v", hdwThreeDigitVersion, hdwThreeDigitVersionWorkaround)
		hdwversion.SemVer, err = semver.Make(hdwThreeDigitVersionWorkaround)
	} else {
		hdwversion.SemVer, err = semver.Make(hdwThreeDigitVersion)
	}
	return
}

func StringToSemVerRange(versionStr string) semver.Range {
	numDigits := len(strings.Split(versionStr, "."))
	if numDigits < 3 {
		versionStr += ".x"
	}
	validRange := semver.MustParseRange(versionStr)
	return validRange
}

func (dbversion GPDBVersion) Before(targetVersion string) bool {
	validRange := StringToSemVerRange("<" + targetVersion)
	return doVersionCheck(dbversion, validRange)
}

func (dbversion GPDBVersion) AtLeast(targetVersion string) bool {
	validRange := StringToSemVerRange(">=" + targetVersion)
	return doVersionCheck(dbversion, validRange)
}

func (dbversion GPDBVersion) Is(targetVersion string) bool {
	validRange := StringToSemVerRange("==" + targetVersion)
	return doVersionCheck(dbversion, validRange)
}

func doVersionCheck(dbversion GPDBVersion, validRange semver.Range) bool {
	version := dbversion.SemVer
	// todo: if cbdb has more versions in the future and corresponding version has impact, here it needs to be modified.
	if dbversion.IsCBDB {
		version, _ = semver.Make("7.0.0")
	}
	return validRange(version)
}
