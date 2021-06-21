package sharederrors

// UnknownNamespaceError is an error raised when a namespace was not found.
type UnknownNamespaceError interface {
	// NotFoundNamespaceName is the name of the namespace that was not found.
	NotFoundNamespaceName() string
}

// UnknownRelationError is an error raised when a relation was not found.
type UnknownRelationError interface {
	// NamespaceName is the name of the namespace under which the relation was not found.
	NamespaceName() string

	// NotFoundRelationName is the name of the relation that was not found.
	NotFoundRelationName() string
}
