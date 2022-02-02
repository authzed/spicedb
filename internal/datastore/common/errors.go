package common

var (
	ErrUnableToInstantiate = "unable to instantiate MysqlDriver: %w"

	ErrUnableToQueryTuples  = "unable to query tuples: %w"
	ErrUnableToWriteTuples  = "unable to write tuples: %w"
	ErrUnableToDeleteTuples = "unable to delete tuples: %w"

	ErrUnableToWriteConfig    = "unable to write namespace config: %w"
	ErrUnableToReadConfig     = "unable to read namespace config: %w"
	ErrUnableToDeleteConfig   = "unable to delete namespace config: %w"
	ErrUnableToListNamespaces = "unable to list namespaces: %w"
)
