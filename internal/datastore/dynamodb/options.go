package dynamodb

import (
	"time"
)

type dynamodbOption struct {
	awsUrl                      string
	awsAccessKeyId              string
	awsScreteAccessKey          string
	awsRegion                   string
	awsSharedProfile            string
	gcWindow                    time.Duration
	followerReadDelay           time.Duration
	revisionQuantization        time.Duration
	maxRevisionStalenessPercent float64
}

type Option func(*dynamodbOption)

func generateConfig(options []Option) (dynamodbOption, error) {
	computed := dynamodbOption{
		awsUrl:             "",
		awsAccessKeyId:     "",
		awsScreteAccessKey: "",
		awsRegion:          "",
		awsSharedProfile:   "",
	}

	for _, option := range options {
		option(&computed)
	}

	return computed, nil
}

func AwsUrl(awsurl string) Option {
	return func(so *dynamodbOption) {
		so.awsUrl = awsurl
	}
}

func AwsAccessKeyId(awsAccessKeyId string) Option {
	return func(so *dynamodbOption) {
		so.awsAccessKeyId = awsAccessKeyId
	}
}

func AwsScreteAccessKey(awsScreteAccessKey string) Option {
	return func(so *dynamodbOption) {
		so.awsScreteAccessKey = awsScreteAccessKey
	}
}

func AwsRegion(awsRegion string) Option {
	return func(so *dynamodbOption) {
		so.awsRegion = awsRegion
	}
}

func AwsSharedProfile(awsSharedProfile string) Option {
	return func(so *dynamodbOption) {
		so.awsSharedProfile = awsSharedProfile
	}
}

type queryOpt struct {
	indexName        string
	limit            int32
	consistentReads  bool
	scanIndexForward bool
}

type QueryOption func(*queryOpt)

func generateQueryOpt(options []QueryOption) (queryOpt, error) {
	computed := queryOpt{
		indexName: "",
		limit:     0,
	}

	for _, option := range options {
		option(&computed)
	}

	return computed, nil
}

func WithIndexName(indexName string) QueryOption {
	return func(so *queryOpt) {
		so.indexName = indexName
	}
}

func WithLimit(limit int32) QueryOption {
	return func(so *queryOpt) {
		so.limit = limit
	}
}

func WithConsistentReads(consistentReads bool) QueryOption {
	return func(so *queryOpt) {
		so.consistentReads = consistentReads
	}
}

func WithScanIndexForward(scanIndexForward bool) QueryOption {
	return func(so *queryOpt) {
		so.scanIndexForward = scanIndexForward
	}
}
