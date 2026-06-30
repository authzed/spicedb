//go:build image

package integration_test

import (
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/authzed/spicedb/pkg/testutil/sdbtestcontainer"
)

func TestRESTGateway(t *testing.T) {
	require := require.New(t)

	container, err := sdbtestcontainer.Run(t.Context(), sdbtestcontainer.DefaultImageReference,
		defaultSchemaOption,
		testcontainers.WithExposedPorts(readOnlyGRPCPort, readOnlyHTTPPort),
		testcontainers.WithCmd("serve-testing"),
	)
	require.NoError(err)
	testcontainers.CleanupContainer(t, container)
	require.NoError(err)

	resp, err := http.Get(fmt.Sprintf("http://%s", container.HTTPEndpoint()))
	require.NoError(err)
	t.Cleanup(func() {
		_ = resp.Body.Close()
	})

	body, err := io.ReadAll(resp.Body)
	require.NoError(err)
	require.JSONEq(`{"code":5,"message":"Not Found","details":[]}`, string(body))

	// Attempt to read schema without a valid Auth header.
	readURL := fmt.Sprintf("http://%s/v1/schema/read", container.HTTPEndpoint())
	resp, err = http.Post(readURL, "", nil) //nolint:gosec
	require.NoError(err)

	body, err = io.ReadAll(resp.Body)
	require.NoError(err)

	require.Equal(401, resp.StatusCode)
	require.Contains(string(body), "Unauthenticated")

	// Attempt to read schema with an invalid Auth header.
	req, err := http.NewRequest("POST", readURL, nil)
	require.NoError(err)
	req.Header.Add("Authorization", "Bearer notcorrect")

	resp, err = http.DefaultClient.Do(req) //nolint:gosec  // SSRF isn't an issue in a test
	require.NoError(err)

	body, err = io.ReadAll(resp.Body)
	require.NoError(err)
	t.Cleanup(func() {
		_ = resp.Body.Close()
	})

	require.Equal(403, resp.StatusCode)
	require.Contains(string(body), "invalid preshared key: invalid token")

	// Read with the correct token.
	req, err = http.NewRequest("POST", readURL, nil)
	require.NoError(err)
	req.Header.Add("Authorization", "Bearer "+container.PresharedKey())

	resp, err = http.DefaultClient.Do(req) //nolint:gosec  // SSRF isn't an issue in a test
	require.NoError(err)

	body, err = io.ReadAll(resp.Body)
	require.NoError(err)

	require.Equal(200, resp.StatusCode)
	require.Contains(string(body), "definition user {")

	// Execute a watch call with an invalid auth header and ensure it 403s.
	watchURL := fmt.Sprintf("http://%s/v1/watch", container.HTTPEndpoint())
	watchReq, err := http.NewRequest("POST", watchURL, nil)
	require.NoError(err)
	watchReq.Header.Add("Authorization", "Bearer notcorrect")

	watchResp, err := http.DefaultClient.Do(watchReq) //nolint:gosec  // SSRF isn't an issue in a test
	require.NoError(err)
	require.Equal(403, watchResp.StatusCode)
	t.Cleanup(func() {
		_ = watchResp.Body.Close()
	})
}
