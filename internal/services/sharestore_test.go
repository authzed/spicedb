package services

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
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

	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials("YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", ""),
		Endpoint:         aws.String(ts.URL),
		Region:           aws.String("eu-central-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}

	sharestore, err := NewS3ShareStore("foo", "bar", s3Config)
	require.NoError(err)

	err = sharestore.createBucketForTesting()
	require.NoError(err)

	// Check for invalid share.
	_, _, err = sharestore.LookupSharedByReference("someref/foobar")
	require.Error(err)

	// Check for non-existent share.
	_, ok, err := sharestore.LookupSharedByReference("someref")
	require.NoError(err)
	require.False(ok)

	// Add a share.
	reference, err := sharestore.StoreShared(SharedData{
		Version:          sharedDataVersion,
		NamespaceConfigs: []string{"foo", "bar"},
	})
	require.NoError(err)

	// Lookup the share and compare.
	sd, ok, err := sharestore.LookupSharedByReference(reference)
	require.NoError(err)
	require.True(ok)
	require.Equal([]string{"foo", "bar"}, sd.NamespaceConfigs)
}
