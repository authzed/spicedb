package namespace

import (
	"context"

	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// ReadNamespaceAndRelation checks that the specified namespace and relation exist in the
// datastore.
//
// Returns ErrNamespaceNotFound if the namespace cannot be found.
// Returns ErrRelationNotFound if the relation was not found in the namespace.
// Returns the direct downstream error for all other unknown error.
func ReadNamespaceAndRelation(ctx context.Context, namespace, relation string, revision decimal.Decimal, manager Manager) (*core.NamespaceDefinition, *core.Relation, error) {
	config, err := manager.ReadNamespace(ctx, namespace, revision)
	if err != nil {
		return nil, nil, err
	}

	for _, rel := range config.Relation {
		if rel.Name == relation {
			return config, rel, nil
		}
	}

	return nil, nil, NewRelationNotFoundErr(namespace, relation)
}

// CheckNamespaceAndRelation checks that the specified namespace and relation exist in the
// datastore.
//
// Returns ErrNamespaceNotFound if the namespace cannot be found.
// Returns ErrRelationNotFound if the relation was not found in the namespace.
// Returns the direct downstream error for all other unknown error.
func CheckNamespaceAndRelation(ctx context.Context, namespace, relation string, allowEllipsis bool, revision decimal.Decimal, manager Manager) error {
	config, err := manager.ReadNamespace(ctx, namespace, revision)
	if err != nil {
		return err
	}

	if allowEllipsis && relation == datastore.Ellipsis {
		return nil
	}

	for _, rel := range config.Relation {
		if rel.Name == relation {
			return nil
		}
	}

	return NewRelationNotFoundErr(namespace, relation)
}

// ReadNamespaceAndTypes reads a namespace definition, version, and type system and returns it if found.
func ReadNamespaceAndTypes(ctx context.Context, nsName string, revision decimal.Decimal, manager Manager) (*core.NamespaceDefinition, *NamespaceTypeSystem, error) {
	nsDef, err := manager.ReadNamespace(ctx, nsName, revision)
	if err != nil {
		return nsDef, nil, err
	}

	ts, terr := BuildNamespaceTypeSystemForManager(nsDef, manager, revision)
	return nsDef, ts, terr
}
