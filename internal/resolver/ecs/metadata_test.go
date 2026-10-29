package ecs

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

const sampleTaskARN = "arn:aws:ecs:us-east-1:123456789012:task/my-cluster/0123456789abcdef"

const sampleTaskMetadata = `{
  "TaskARN": "` + sampleTaskARN + `",
  "Containers": [
    {"Name": "fluent-bit", "Networks": []},
    {"Name": "spicedb", "Networks": [{"IPv4Addresses": ["10.20.30.40"]}]}
  ]
}`

func TestTaskMetadataSelf(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v4/abc/task" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(sampleTaskMetadata))
	}))
	t.Cleanup(srv.Close)
	t.Setenv(metadataEnvVar, srv.URL+"/v4/abc")

	self, err := taskMetadataSelf(t.Context())
	require.NoError(t, err)
	require.Equal(t, Self{TaskARN: sampleTaskARN, IP: "10.20.30.40"}, self)
}

func TestTaskMetadataSelfOutsideECS(t *testing.T) {
	t.Setenv(metadataEnvVar, "")

	_, err := taskMetadataSelf(t.Context())
	require.ErrorContains(t, err, metadataEnvVar)
}

func TestFetchSelfErrors(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		wantErr string
	}{
		{name: "bad status", status: http.StatusInternalServerError, body: "{}", wantErr: "unexpected status"},
		{name: "bad json", status: http.StatusOK, body: "{", wantErr: "decoding task metadata"},
		{name: "no address", status: http.StatusOK, body: `{"TaskARN":"arn","Containers":[{"Networks":[{"IPv4Addresses":[]}]}]}`, wantErr: "no IPv4 address"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(srv.Close)

			_, err := fetchSelf(t.Context(), srv.Client(), srv.URL+"/task")
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}
