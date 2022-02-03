package mysql

type mysqlOptions struct {
	watchBufferLength uint16
}

const (
	defaultWatchBufferLength = 128
)

// Option provides the facility to configure how clients within the
// MySQL datastore interact with the running MySQL database.
type Option func(*mysqlOptions)

func generateConfig(options []Option) (mysqlOptions, error) {
	computed := mysqlOptions{
		watchBufferLength: defaultWatchBufferLength,
	}

	for _, option := range options {
		option(&computed)
	}

	return computed, nil
}
