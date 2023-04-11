package releases

import (
	"context"
	"fmt"
	"time"

	"github.com/google/go-github/v43/github"
)

const (
	githubNamespace  = "authzed"
	githubRepository = "spicedb"
)

// GetSourceRepository returns the source repository path for SpiceDB.
func GetSourceRepository() string {
	return fmt.Sprintf("github.com/%s/%s", githubNamespace, githubRepository)
}

// Release represents a release of SpiceDB.
type Release struct {
	// Version is the version of the release.
	Version string

	// PublishedAt is when the release was published, in UTC.
	PublishedAt time.Time

	// ViewURL is the URL at which the release can be viewed.
	ViewURL string
}

// GetLatestRelease returns the latest release of SpiceDB, as reported by the GitHub API.
func GetLatestRelease(ctx context.Context) (*Release, error) {
	client := github.NewClient(nil)
	release, _, err := client.Repositories.GetLatestRelease(ctx, githubNamespace, githubRepository)
	if err != nil {
		return nil, err
	}

	if release == nil {
		return nil, fmt.Errorf("latest release not found")
	}

	return &Release{
		Version:     *release.Name,
		PublishedAt: (*release.PublishedAt).UTC(),
		ViewURL:     *release.HTMLURL,
	}, nil
}
