package datastore

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	rdsauth "github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"golang.org/x/exp/maps"

	log "github.com/authzed/spicedb/internal/logging"
)

// CredentialsProvider allows datastore credentials to be retrieved dynamically
type CredentialsProvider interface {
	// Name returns the name of the provider
	Name() string
	// IsCleartextToken returns true if the token returned represents a token (rather than a password) that must be sent in cleartext to the datastore, or false otherwise.
	// This may be used to configure the datastore options to avoid sending a hash of the token instead of its value.
	// Note that it is always recommended that communication channel be encrypted.
	IsCleartextToken() bool
	// Get returns the username and password to use when connecting to the underlying datastore
	Get(ctx context.Context, dbEndpoint string, dbUser string) (string, string, error)
}

var NoCredentialsProvider CredentialsProvider = nil

type credentialsProviderBuilderFunc func(ctx context.Context) (CredentialsProvider, error)

const (
	// AWSIAMCredentialProvider generates AWS IAM tokens for authenticating with the datastore (i.e. RDS)
	AWSIAMCredentialProvider = "aws-iam"
)

var BuilderForCredentialProvider = map[string]credentialsProviderBuilderFunc{
	AWSIAMCredentialProvider: newAWSIAMCredentialsProvider,
}

// CredentialsProviderOptions returns the full set of credential provider names, sorted and quoted into a string.
func CredentialsProviderOptions() string {
	ids := maps.Keys(BuilderForCredentialProvider)
	sort.Strings(ids)
	quoted := make([]string, 0, len(ids))
	for _, id := range ids {
		quoted = append(quoted, `"`+id+`"`)
	}
	return strings.Join(quoted, ", ")
}

// NewCredentialsProvider create a new CredentialsProvider for the given name
// returns an error if no match is found, of if there is a problem creating the given CredentialsProvider
func NewCredentialsProvider(ctx context.Context, name string) (CredentialsProvider, error) {
	builder, ok := BuilderForCredentialProvider[name]
	if !ok {
		return nil, fmt.Errorf("unknown credentials provider: %s", name)
	}
	return builder(ctx)
}

// AWS IAM provider

func newAWSIAMCredentialsProvider(ctx context.Context) (CredentialsProvider, error) {
	awsSdkConfig, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	return &awsIamCredentialsProvider{awsSdkConfig: awsSdkConfig}, nil
}

type awsIamCredentialsProvider struct {
	awsSdkConfig aws.Config
}

func (d awsIamCredentialsProvider) Name() string {
	return AWSIAMCredentialProvider
}

func (d awsIamCredentialsProvider) IsCleartextToken() bool {
	// The AWS IAM token can be of an arbitrary length and must not be hashed or truncated by the datastore driver
	// See https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html
	return true
}

func (d awsIamCredentialsProvider) Get(ctx context.Context, dbEndpoint string, dbUser string) (string, string, error) {
	authToken, err := rdsauth.BuildAuthToken(ctx, dbEndpoint, d.awsSdkConfig.Region, dbUser, d.awsSdkConfig.Credentials)
	if err != nil {
		return "", "", err
	}
	log.Ctx(ctx).Trace().Str("region", d.awsSdkConfig.Region).Str("endpoint", dbEndpoint).Str("user", dbUser).Msg("successfully retrieved IAM auth token for DB")
	return dbUser, authToken, nil
}
