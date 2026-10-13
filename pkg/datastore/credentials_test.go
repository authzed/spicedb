package datastore

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/stretchr/testify/require"
)

func TestUnknownCredentialsProvider(t *testing.T) {
	unknownCredentialsProviders := []string{"", " ", "some-unknown-credentials-provider"}
	for _, unknownCredentialsProvider := range unknownCredentialsProviders {
		t.Run(unknownCredentialsProvider, func(t *testing.T) {
			credentialsProvider, err := NewCredentialsProvider(t.Context(), unknownCredentialsProvider)
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

	credentialsProvider, err := NewCredentialsProvider(t.Context(), AWSIAMCredentialProvider)
	require.NotNil(t, credentialsProvider)
	require.NoError(t, err)

	require.True(t, credentialsProvider.IsCleartextToken(), "AWS IAM tokens should be communicated in cleartext")

	username, password, err := credentialsProvider.Get(t.Context(), "some-hostname:5432", "some-user")
	require.NoError(t, err)
	require.Equal(t, "some-user", username)
	require.Containsf(t, password, "X-Amz-Algorithm", "signed token should contain algorithm attribute")
}

type fakeDescribeGlobalClusters struct {
	calls     int
	clusterID string
	writerArn string
	err       error
}

func (f *fakeDescribeGlobalClusters) DescribeGlobalClusters(_ context.Context, params *rds.DescribeGlobalClustersInput, _ ...func(*rds.Options)) (*rds.DescribeGlobalClustersOutput, error) {
	f.calls++
	f.clusterID = aws.ToString(params.GlobalClusterIdentifier)
	if f.err != nil {
		return nil, f.err
	}
	return &rds.DescribeGlobalClustersOutput{
		GlobalClusters: []types.GlobalCluster{{
			GlobalClusterMembers: []types.GlobalClusterMember{
				{DBClusterArn: aws.String("arn:aws:rds:us-east-1:123456789012:cluster:reader"), IsWriter: aws.Bool(false)},
				{DBClusterArn: aws.String(f.writerArn), IsWriter: aws.Bool(true)},
			},
		}},
	}, nil
}

func TestAWSIAMCredentialsProviderGlobalWriterRegion(t *testing.T) {
	t.Setenv("AWS_CONFIG_FILE", "file_not_exists")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "file_not_exists")
	t.Setenv("AWS_ENDPOINT_URL", "http://169.254.169.254/aws")
	t.Setenv("AWS_ACCESS_KEY", "access_key")
	t.Setenv("AWS_SECRET_KEY", "secret_key")
	t.Setenv("AWS_REGION", "us-east-1")

	credentialsProvider, err := NewCredentialsProvider(t.Context(), AWSIAMCredentialProvider)
	require.NoError(t, err)

	fake := &fakeDescribeGlobalClusters{writerArn: "arn:aws:rds:eu-west-1:123456789012:cluster:writer"}
	provider := credentialsProvider.(*awsIamCredentialsProvider)
	provider.rdsClient = fake

	for range 2 {
		_, password, err := provider.Get(t.Context(), "my-global.global-abc123.global.rds.amazonaws.com:5432", "some-user")
		require.NoError(t, err)
		require.Contains(t, password, "eu-west-1", "token should be signed for the writer region")
	}
	require.Equal(t, "my-global", fake.clusterID)
	require.Equal(t, 1, fake.calls, "writer region should be cached")

	_, password, err := provider.Get(t.Context(), "my-cluster.cluster-abc123.us-east-1.rds.amazonaws.com:5432", "some-user")
	require.NoError(t, err)
	require.Contains(t, password, "us-east-1", "regional endpoints should use the configured region")
	require.Equal(t, 1, fake.calls)
}

func TestAWSIAMCredentialsProviderGlobalWriterRegionFallback(t *testing.T) {
	t.Setenv("AWS_CONFIG_FILE", "file_not_exists")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "file_not_exists")
	t.Setenv("AWS_ENDPOINT_URL", "http://169.254.169.254/aws")
	t.Setenv("AWS_ACCESS_KEY", "access_key")
	t.Setenv("AWS_SECRET_KEY", "secret_key")
	t.Setenv("AWS_REGION", "us-east-1")

	credentialsProvider, err := NewCredentialsProvider(t.Context(), AWSIAMCredentialProvider)
	require.NoError(t, err)

	fake := &fakeDescribeGlobalClusters{err: errors.New("access denied")}
	provider := credentialsProvider.(*awsIamCredentialsProvider)
	provider.rdsClient = fake

	for range 2 {
		username, password, err := provider.Get(t.Context(), "my-global.global-abc123.global.rds.amazonaws.com:5432", "some-user")
		require.NoError(t, err)
		require.Equal(t, "some-user", username)
		require.Contains(t, password, "us-east-1", "token should be signed for the configured region when the lookup fails")
	}
	require.Equal(t, 1, fake.calls, "fallback region should be cached")
}
