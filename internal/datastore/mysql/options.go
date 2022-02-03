package mysql

import (
	"time"
)

type mysqlOptions struct {
	revisionFuzzingTimedelta time.Duration
}

// Option provides the facility to configure how clients within the
// Postgres datastore interact with the running Postgres database.
type Option func(*mysqlOptions)

func generateConfig(options []Option) (mysqlOptions, error) {
	computed := mysqlOptions{}

	for _, option := range options {
		option(&computed)
	}

	return computed, nil
}

// RevisionFuzzingTimedelta is the time bucket size to which advertised
// revisions will be rounded.
//
// This value defaults to 5 seconds.
func RevisionFuzzingTimedelta(delta time.Duration) Option {
	return func(mo *mysqlOptions) {
		mo.revisionFuzzingTimedelta = delta
	}
}
