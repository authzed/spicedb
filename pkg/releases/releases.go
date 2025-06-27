package releases

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
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
	return getLatestReleaseWithClient(ctx, nil /* use default */)
}

func getLatestReleaseWithClient(ctx context.Context, httpClient *http.Client) (*Release, error) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/releases/latest", githubNamespace, githubRepository)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %s", resp.Status)
	}

	var ghResp struct {
		Name        string    `json:"name"`
		PublishedAt time.Time `json:"published_at"`
		HTMLURL     string    `json:"html_url"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&ghResp); err != nil {
		return nil, err
	}

	return &Release{
		Version:     ghResp.Name,
		PublishedAt: ghResp.PublishedAt.UTC(),
		ViewURL:     ghResp.HTMLURL,
	}, nil
}
