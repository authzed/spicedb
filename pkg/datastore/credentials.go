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
	// Get returns the username and password to use when connecting to the underlying datastore
	Get(ctx context.Context, dbHostname string, dbPort uint16, dbUser string) (string, string, error)
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
// returns nil if no match is found
// returns an error if there is a problem initializing the given CredentialsProvider
func NewCredentialsProvider(ctx context.Context, name string) (CredentialsProvider, error) {
	builder, ok := BuilderForCredentialProvider[name]
	if !ok {
		return nil, nil
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

func (d awsIamCredentialsProvider) Get(ctx context.Context, dbHostname string, dbPort uint16, dbUser string) (string, string, error) {
	dbEndpoint := fmt.Sprintf("%s:%d", dbHostname, dbPort)
	authToken, err := rdsauth.BuildAuthToken(ctx, dbEndpoint, d.awsSdkConfig.Region, dbUser, d.awsSdkConfig.Credentials)
	if err != nil {
		log.Ctx(ctx).Trace().Str("region", d.awsSdkConfig.Region).Str("endpoint", dbEndpoint).Str("user", dbUser).Msg("successfully retrieved IAM auth token for DB")
	}
	return dbUser, authToken, err
}
