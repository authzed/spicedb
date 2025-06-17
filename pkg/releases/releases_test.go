package releases

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGetSourceRepository(t *testing.T) {
	repo := GetSourceRepository()
	require.Equal(t, "github.com/authzed/spicedb", repo)
}

func TestGetLatestRelease(t *testing.T) {
	t.Run("creates default http client", func(t *testing.T) {
		release, err := getLatestReleaseWithClient(t.Context(), nil)
		require.NoError(t, err)
		require.NotNil(t, release)
	})

	t.Run("successful release fetch", func(t *testing.T) {
		var receivedPath string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedPath = r.URL.Path
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{
				"name": "v1.5.6",
				"published_at": "2023-01-01T12:00:00Z",
				"html_url": "https://github.com/authzed/spicedb/releases/tag/v1.5.6"
			}`)
		}))
		defer server.Close()

		httpClient := &http.Client{
			Transport: &testRoundTripper{
				baseURL: server.URL,
			},
		}

		release, err := getLatestReleaseWithClient(t.Context(), httpClient)
		require.NoError(t, err)
		require.NotNil(t, release)
		require.Equal(t, "/repos/authzed/spicedb/releases/latest", receivedPath)
		require.Equal(t, "v1.5.6", release.Version)
		require.Equal(t, "https://github.com/authzed/spicedb/releases/tag/v1.5.6", release.ViewURL)
		require.True(t, release.PublishedAt.Equal(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)))
	})

	t.Run("api error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		httpClient := &http.Client{
			Transport: &testRoundTripper{
				baseURL: server.URL,
			},
		}

		release, err := getLatestReleaseWithClient(t.Context(), httpClient)
		require.Error(t, err)
		require.Nil(t, release)
	})

	t.Run("invalid json response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{invalid json}`)
		}))
		defer server.Close()

		httpClient := &http.Client{
			Transport: &testRoundTripper{
				baseURL: server.URL,
			},
		}

		release, err := getLatestReleaseWithClient(t.Context(), httpClient)
		require.Error(t, err)
		require.Nil(t, release)
	})
}

type testRoundTripper struct {
	baseURL string
}

func (t *testRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Host == "api.github.com" {
		parsedURL, _ := url.Parse(t.baseURL)
		req.URL.Scheme = parsedURL.Scheme
		req.URL.Host = parsedURL.Host
	}
	return http.DefaultTransport.RoundTrip(req)
}
