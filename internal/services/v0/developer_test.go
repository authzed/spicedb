package v0

import (
	"context"
	"testing"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestDeveloperSharing(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"), goleak.IgnoreCurrent())

	require := require.New(t)

	store := NewInMemoryShareStore("flavored")
	srv := NewDeveloperServer(store)

	// Check for non-existent share.
	resp, err := srv.LookupShared(context.Background(), &v0.LookupShareRequest{
		ShareReference: "someref",
	})
	require.NoError(err)
	require.Equal(v0.LookupShareResponse_UNKNOWN_REFERENCE, resp.Status)

	// Add a share resource.
	sresp, err := srv.Share(context.Background(), &v0.ShareRequest{
		Schema:            "s",
		RelationshipsYaml: "ry",
		ValidationYaml:    "vy",
		AssertionsYaml:    "ay",
	})
	require.NoError(err)

	// Lookup again.
	lresp, err := srv.LookupShared(context.Background(), &v0.LookupShareRequest{
		ShareReference: sresp.ShareReference,
	})
	require.NoError(err)
	require.Equal(v0.LookupShareResponse_VALID_REFERENCE, lresp.Status)
	require.Equal("s", lresp.Schema)
}

func TestDeveloperSharingConverted(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"), goleak.IgnoreCurrent())

	require := require.New(t)

	store := NewInMemoryShareStore("flavored")
	srv := NewDeveloperServer(store)

	// Add a share resource in V1 format.
	store.(*inMemoryShareStore).shared["foo"] = []byte(`{
		"version": "1",
		"namespace_configs": [
			"name: \"foo\""
		],
		"relation_tuples": "rt",
		"validation_yaml": "vy",
		"assertions_yaml": "ay"
}`)

	// Lookup and ensure converted.
	lresp, err := srv.LookupShared(context.Background(), &v0.LookupShareRequest{
		ShareReference: "foo",
	})
	require.NoError(err)
	require.Equal(v0.LookupShareResponse_UPGRADED_REFERENCE, lresp.Status)
	require.Equal("rt", lresp.RelationshipsYaml)
	require.Equal("vy", lresp.ValidationYaml)
	require.Equal("ay", lresp.AssertionsYaml)

	require.Equal("definition foo {}\n\n", lresp.Schema)
}
