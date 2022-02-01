package common

var (
	// ErrUnableToInstantiate unable to instantiate mysql driver error
	ErrUnableToInstantiate = "unable to instantiate MysqlDriver: %w"

	ErrUnableToQueryTuples  = "unable to query tuples: %w"
	ErrUnableToWriteTuples  = "unable to write tuples: %w"
	ErrUnableToDeleteTuples = "unable to delete tuples: %w"
)
