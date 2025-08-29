package dynamodb

type dynamodbOption struct {
	awsUrl             string
	awsAccessKeyId     string
	awsScreteAccessKey string
	awsRegion          string
	awsSharedProfile   string
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
