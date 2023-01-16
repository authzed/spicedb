package releases

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"

	"github.com/jzelinskie/cobrautil/v2"
	"golang.org/x/mod/semver"
)

// CurrentVersion returns the current version of the binary.
func CurrentVersion() (string, error) {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "", fmt.Errorf("failed to read BuildInfo because the program was compiled with Go %s", runtime.Version())
	}

	return cobrautil.VersionWithFallbacks(bi), nil
}

// SoftwareUpdateState is the state of this software relative to whether updates are available.
type SoftwareUpdateState int

const (
	// Unknown indicates an unknown state.
	Unknown SoftwareUpdateState = iota

	// UnreleasedVersion indicates that the current software version running is an unreleased version.
	UnreleasedVersion

	// UpdateAvailable indicates an update is available.
	UpdateAvailable

	// UpToDate indicates that the software is fully up-to-date.
	UpToDate
)

// CheckIsLatestVersion returns whether the current version of the software is the latest released.
// Returns the state, as well as the latest release.
func CheckIsLatestVersion(
	ctx context.Context,
	getCurrentVersion func() (string, error),
	getLatestRelease func(context.Context) (*Release, error),
) (SoftwareUpdateState, string, *Release, error) {
	currentVersion, err := getCurrentVersion()
	if err != nil {
		return Unknown, currentVersion, nil, err
	}

	if currentVersion == "" || !semver.IsValid(currentVersion) {
		return UnreleasedVersion, currentVersion, nil, nil
	}

	release, err := getLatestRelease(ctx)
	if err != nil {
		return Unknown, currentVersion, nil, err
	}

	if !semver.IsValid(release.Version) {
		return Unknown, currentVersion, nil, err
	}

	if semver.Compare(currentVersion, release.Version) < 0 {
		return UpdateAvailable, currentVersion, release, nil
	}

	return UpToDate, currentVersion, release, nil
}
