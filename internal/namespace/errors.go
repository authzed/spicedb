package namespace

import (
	"fmt"

	"github.com/rs/zerolog"

	"github.com/authzed/spicedb/internal/sharederrors"
)

// ErrRelationNotFound occurs when a relation was not found under a namespace.
type ErrRelationNotFound struct {
	error
	namespaceName string
	relationName  string
}

// NamespaceName returns the name of the namespace in which the relation was not found.
func (erf ErrRelationNotFound) NamespaceName() string {
	return erf.namespaceName
}

// NotFoundRelationName returns the name of the relation not found.
func (erf ErrRelationNotFound) NotFoundRelationName() string {
	return erf.relationName
}

func (erf ErrRelationNotFound) MarshalZerologObject(e *zerolog.Event) {
	e.Str("error", erf.Error()).Str("namespace", erf.namespaceName).Str("relation", erf.relationName)
}

// NewRelationNotFoundErr constructs a new relation not found error.
func NewRelationNotFoundErr(nsName string, relationName string) error {
	return ErrRelationNotFound{
		error:         fmt.Errorf("relation/permission `%s` not found under definition `%s`", relationName, nsName),
		namespaceName: nsName,
		relationName:  relationName,
	}
}

var _ sharederrors.UnknownRelationError = ErrRelationNotFound{}
