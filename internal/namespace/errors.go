package namespace

import "fmt"

// ErrNamespaceNotFound occurs when a namespace was not found.
type ErrNamespaceNotFound struct {
	error
	namespaceName string
}

func (enf ErrNamespaceNotFound) NotFoundNamespaceName() string {
	return enf.namespaceName
}

func NewNamespaceNotFoundErr(nsName string) error {
	return ErrNamespaceNotFound{
		error:         fmt.Errorf("namespace `%s` not found", nsName),
		namespaceName: nsName,
	}
}

// ErrRelationNotFound occurs when a relation was not found under a namespace.
type ErrRelationNotFound struct {
	error
	namespaceName string
	relationName  string
}

func (erf ErrRelationNotFound) NamespaceName() string {
	return erf.namespaceName
}

func (erf ErrRelationNotFound) NotFoundRelationName() string {
	return erf.relationName
}

func NewRelationNotFoundErr(nsName string, relationName string) error {
	return ErrRelationNotFound{
		error:         fmt.Errorf("relation/permission `%s` not found under namespace `%s`", relationName, nsName),
		namespaceName: nsName,
		relationName:  relationName,
	}
}
