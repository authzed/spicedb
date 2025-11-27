//go:build docker && image

package integration_test

import (
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
)

func TestRESTGateway(t *testing.T) {
	require := require.New(t)

	tester, err := newTester(t,
		&dockertest.RunOptions{
			Repository:   "authzed/spicedb",
			Tag:          "ci",
			Cmd:          []string{"serve", "--log-level", "debug", "--grpc-preshared-key", "somerandomkeyhere", "--http-enabled"},
			ExposedPorts: []string{"50051/tcp", "8443/tcp"},
		},
		"somerandomkeyhere",
		false,
	)
	require.NoError(err)

	resp, err := http.Get(fmt.Sprintf("http://localhost:%s", tester.HTTPPort))
	require.NoError(err)
	t.Cleanup(func() {
		_ = resp.Body.Close()
	})

	body, err := io.ReadAll(resp.Body)
	require.NoError(err)
	require.JSONEq(`{"code":5,"message":"Not Found","details":[]}`, string(body))

	// Attempt to read schema without a valid Auth header.
	readURL := fmt.Sprintf("http://localhost:%s/v1/schema/read", tester.HTTPPort)
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

	resp, err = http.DefaultClient.Do(req)
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
	req.Header.Add("Authorization", "Bearer somerandomkeyhere")

	resp, err = http.DefaultClient.Do(req)
	require.NoError(err)

	body, err = io.ReadAll(resp.Body)
	require.NoError(err)

	require.Equal(200, resp.StatusCode)
	require.Contains(string(body), "definition user {")

	// Execute a watch call with an invalid auth header and ensure it 403s.
	watchURL := fmt.Sprintf("http://localhost:%s/v1/watch", tester.HTTPPort)
	watchReq, err := http.NewRequest("POST", watchURL, nil)
	require.NoError(err)
	watchReq.Header.Add("Authorization", "Bearer notcorrect")

	watchResp, err := http.DefaultClient.Do(watchReq)
	require.NoError(err)
	require.Equal(403, watchResp.StatusCode)
	t.Cleanup(func() {
		_ = watchResp.Body.Close()
	})
}
