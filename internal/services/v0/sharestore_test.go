package v0

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/stretchr/testify/require"
)

func TestS3ShareStore(t *testing.T) {
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	require := require.New(t)

	s3Config := aws.Config{
		Credentials:  credentials.NewStaticCredentialsProvider("YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", ""),
		BaseEndpoint: aws.String(ts.URL),
		Region:       "eu-central-1",
	}

	sharestore, err := NewS3ShareStore("foo", "bar", s3Config, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	require.NoError(err)

	err = sharestore.(*s3ShareStore).createBucketForTesting(context.Background())
	require.NoError(err)

	// Check for invalid share.
	_, _, err = sharestore.LookupSharedByReference("someref/foobar")
	require.Error(err)

	// Check for non-existent share.
	_, status, err := sharestore.LookupSharedByReference("someref")
	require.NoError(err)
	require.Equal(LookupNotFound, status)

	// Add a share.
	reference, err := sharestore.StoreShared(SharedDataV2{
		Version: sharedDataVersion,
		Schema:  "foo",
	})
	require.NoError(err)

	// Lookup the share and compare.
	sd, status, err := sharestore.LookupSharedByReference(reference)
	require.NoError(err)
	require.Equal(LookupSuccess, status)
	require.Equal("foo", sd.Schema)
}
