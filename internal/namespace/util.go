package namespace

import (
	"context"

	"github.com/authzed/spicedb/internal/util"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// ReadNamespaceAndRelation checks that the specified namespace and relation exist in the
// datastore.
//
// Returns ErrNamespaceNotFound if the namespace cannot be found.
// Returns ErrRelationNotFound if the relation was not found in the namespace.
// Returns the direct downstream error for all other unknown error.
func ReadNamespaceAndRelation(
	ctx context.Context,
	namespace string,
	relation string,
	ds datastore.Reader,
) (*core.NamespaceDefinition, *core.Relation, error) {
	config, _, err := ds.ReadNamespace(ctx, namespace)
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
// Returns datastore.ErrNamespaceNotFound if the namespace cannot be found.
// Returns ErrRelationNotFound if the relation was not found in the namespace.
// Returns the direct downstream error for all other unknown error.
func CheckNamespaceAndRelation(
	ctx context.Context,
	namespace string,
	relation string,
	allowEllipsis bool,
	ds datastore.Reader,
) error {
	config, _, err := ds.ReadNamespace(ctx, namespace)
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
func ReadNamespaceAndTypes(
	ctx context.Context,
	nsName string,
	ds datastore.Reader,
) (*core.NamespaceDefinition, *TypeSystem, error) {
	nsDef, _, err := ds.ReadNamespace(ctx, nsName)
	if err != nil {
		return nil, nil, err
	}

	ts, terr := BuildNamespaceTypeSystemForDatastore(nsDef, ds)
	return nsDef, ts, terr
}

// ListReferencedNamespaces returns the names of all namespaces referenced in the
// given namespace definitions. This includes the namespaces themselves, as well as
// any found in type information on relations.
func ListReferencedNamespaces(nsdefs []*core.NamespaceDefinition) []string {
	referencedNamespaceNamesSet := util.NewSet[string]()
	for _, nsdef := range nsdefs {
		referencedNamespaceNamesSet.Add(nsdef.Name)

		for _, relation := range nsdef.Relation {
			if relation.GetTypeInformation() != nil {
				for _, allowedRel := range relation.GetTypeInformation().AllowedDirectRelations {
					referencedNamespaceNamesSet.Add(allowedRel.GetNamespace())
				}
			}
		}
	}
	return referencedNamespaceNamesSet.AsSlice()
}
