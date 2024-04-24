package datastore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnknownCredentialsProvider(t *testing.T) {
	unknownCredentialsProviders := []string{"", " ", "some-unknown-credentials-provider"}
	for _, unknownCredentialsProvider := range unknownCredentialsProviders {
		t.Run(unknownCredentialsProvider, func(t *testing.T) {
			credentialsProvider, err := NewCredentialsProvider(context.Background(), unknownCredentialsProvider)
			require.Nil(t, credentialsProvider)
			require.Error(t, err)
		})
	}
}

func TestAWSIAMCredentialsProvider(t *testing.T) {
	// set up the environment, so we don't make any external calls to AWS
	t.Setenv("AWS_CONFIG_FILE", "file_not_exists")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "file_not_exists")
	t.Setenv("AWS_ENDPOINT_URL", "http://169.254.169.254/aws")
	t.Setenv("AWS_ACCESS_KEY", "access_key")
	t.Setenv("AWS_SECRET_KEY", "secret_key")
	t.Setenv("AWS_REGION", "us-east-1")

	credentialsProvider, err := NewCredentialsProvider(context.Background(), AWSIAMCredentialProvider)
	require.NotNil(t, credentialsProvider)
	require.NoError(t, err)

	require.True(t, credentialsProvider.IsCleartextToken(), "AWS IAM tokens should be communicated in cleartext")

	username, password, err := credentialsProvider.Get(context.Background(), "some-hostname:5432", "some-user")
	require.NoError(t, err)
	require.Equal(t, "some-user", username)
	require.Containsf(t, password, "X-Amz-Algorithm", "signed token should contain algorithm attribute")
}
