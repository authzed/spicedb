//go:build docker
// +build docker

package main

import (
	"fmt"
	"io/ioutil"
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
			Tag:          "latest",
			Cmd:          []string{"serve", "--log-level", "debug", "--grpc-preshared-key", "somerandomkeyhere", "--http-enabled"},
			ExposedPorts: []string{"50051/tcp", "8443/tcp"},
		},
		"somerandomkeyhere",
	)
	require.NoError(err)
	defer tester.cleanup()

	resp, err := http.Get(fmt.Sprintf("http://localhost:%s", tester.httpPort))
	require.NoError(err)

	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(err)
	require.JSONEq(`{"code":5,"message":"Not Found","details":[]}`, string(body))

	// Attempt to read schema without a valid Auth header.
	readUrl := fmt.Sprintf("http://localhost:%s/v1/schema/read", tester.httpPort)
	resp, err = http.Post(readUrl, "", nil)
	require.NoError(err)

	body, err = ioutil.ReadAll(resp.Body)
	require.NoError(err)

	require.Equal(500, resp.StatusCode)
	require.Contains(string(body), "Unauthenticated")

	// Attempt to read schema with an invalid Auth header.
	req, err := http.NewRequest("POST", readUrl, nil)
	req.Header.Add("Authorization", "Bearer notcorrect")

	resp, err = http.DefaultClient.Do(req)
	require.NoError(err)

	body, err = ioutil.ReadAll(resp.Body)
	require.NoError(err)

	require.Equal(500, resp.StatusCode)
	require.Contains(string(body), "invalid preshared key: invalid token")

	// Read with the correct token.
	req, err = http.NewRequest("POST", readUrl, nil)
	req.Header.Add("Authorization", "Bearer somerandomkeyhere")

	resp, err = http.DefaultClient.Do(req)
	require.NoError(err)

	body, err = ioutil.ReadAll(resp.Body)
	require.NoError(err)

	require.Equal(200, resp.StatusCode)
	require.Contains(string(body), "definition user {")
}
