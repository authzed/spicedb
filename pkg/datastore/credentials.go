package datastore

import (
	"context"
	"fmt"
	"maps"
	"net"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	rdsauth "github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/aws/aws-sdk-go-v2/service/rds"

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
	ids := slices.Collect(maps.Keys(BuilderForCredentialProvider))
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

const (
	// awsGlobalEndpointSuffix is the hostname suffix of Aurora Global Database writer endpoints
	awsGlobalEndpointSuffix = ".global.rds.amazonaws.com"
	// awsGlobalWriterRegionTTL is how long the writer region of a global cluster is cached
	awsGlobalWriterRegionTTL = 30 * time.Second
)

type describeGlobalClustersAPI interface {
	DescribeGlobalClusters(ctx context.Context, params *rds.DescribeGlobalClustersInput, optFns ...func(*rds.Options)) (*rds.DescribeGlobalClustersOutput, error)
}

func newAWSIAMCredentialsProvider(ctx context.Context) (CredentialsProvider, error) {
	awsSdkConfig, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	return &awsIamCredentialsProvider{awsSdkConfig: awsSdkConfig, rdsClient: rds.NewFromConfig(awsSdkConfig)}, nil
}

type awsIamCredentialsProvider struct {
	awsSdkConfig aws.Config
	rdsClient    describeGlobalClustersAPI

	mu                   sync.Mutex
	writerRegion         string
	writerRegionExpireAt time.Time
}

func (d *awsIamCredentialsProvider) Name() string {
	return AWSIAMCredentialProvider
}

func (d *awsIamCredentialsProvider) IsCleartextToken() bool {
	// The AWS IAM token can be of an arbitrary length and must not be hashed or truncated by the datastore driver
	// See https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html
	return true
}

func (d *awsIamCredentialsProvider) Get(ctx context.Context, dbEndpoint string, dbUser string) (string, string, error) {
	region := d.awsSdkConfig.Region
	host, _, err := net.SplitHostPort(dbEndpoint)
	if err != nil {
		host = dbEndpoint
	}
	if strings.HasSuffix(host, awsGlobalEndpointSuffix) {
		region = d.globalWriterRegion(ctx, host)
	}

	authToken, err := rdsauth.BuildAuthToken(ctx, dbEndpoint, region, dbUser, d.awsSdkConfig.Credentials)
	if err != nil {
		return "", "", err
	}
	log.Ctx(ctx).Trace().Str("region", region).Str("endpoint", dbEndpoint).Str("user", dbUser).Msg("successfully retrieved IAM auth token for DB")
	return dbUser, authToken, nil
}

// globalWriterRegion returns the region of the writer cluster of the Aurora Global Database
// behind the given global writer endpoint. Global writer endpoints have no region in their
// hostname, and the IAM token must be signed for the region the writer is currently in.
// If the writer region cannot be determined, the configured region is used.
func (d *awsIamCredentialsProvider) globalWriterRegion(ctx context.Context, host string) string {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.writerRegion != "" && time.Now().Before(d.writerRegionExpireAt) {
		return d.writerRegion
	}

	globalClusterID, _, _ := strings.Cut(host, ".")
	region, err := d.lookupWriterRegion(ctx, globalClusterID)
	if err != nil {
		log.Ctx(ctx).Warn().Err(err).Str("region", d.awsSdkConfig.Region).Msg("unable to determine the writer region of the global cluster, using the configured region")
		region = d.awsSdkConfig.Region
	}

	d.writerRegion = region
	d.writerRegionExpireAt = time.Now().Add(awsGlobalWriterRegionTTL)
	return region
}

func (d *awsIamCredentialsProvider) lookupWriterRegion(ctx context.Context, globalClusterID string) (string, error) {
	out, err := d.rdsClient.DescribeGlobalClusters(ctx, &rds.DescribeGlobalClustersInput{
		GlobalClusterIdentifier: aws.String(globalClusterID),
	})
	if err != nil {
		return "", fmt.Errorf("unable to describe global cluster %s: %w", globalClusterID, err)
	}

	for _, cluster := range out.GlobalClusters {
		for _, member := range cluster.GlobalClusterMembers {
			if !aws.ToBool(member.IsWriter) {
				continue
			}
			clusterArn, err := arn.Parse(aws.ToString(member.DBClusterArn))
			if err != nil {
				return "", fmt.Errorf("unable to parse writer cluster ARN of global cluster %s: %w", globalClusterID, err)
			}
			return clusterArn.Region, nil
		}
	}

	return "", fmt.Errorf("no writer found for global cluster %s", globalClusterID)
}
