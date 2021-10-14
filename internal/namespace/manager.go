package namespace

import (
	"context"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("spicedb/internal/namespace")

// Manager is a subset of the datastore interface that can read (and possibly cache) namespaces.
type Manager interface {
	// ReadNamespace reads a namespace definition and version and returns it if found.
	//
	// Returns ErrNamespaceNotFound if the namespace cannot be found.
	// Returns the direct downstream error for all other unknown error.
	ReadNamespace(ctx context.Context, nsName string) (*v0.NamespaceDefinition, decimal.Decimal, error)

	// CheckNamespaceAndRelation checks that the specified namespace and relation exist in the
	// datastore.
	//
	// Returns ErrNamespaceNotFound if the namespace cannot be found.
	// Returns ErrRelationNotFound if the relation was not found in the namespace.
	// Returns the direct downstream error for all other unknown error.
	CheckNamespaceAndRelation(ctx context.Context, namespace, relation string, allowEllipsis bool) error

	// ReadNamespaceAndTypes reads a namespace definition, version, and type system and returns it if found.
	ReadNamespaceAndTypes(ctx context.Context, nsName string) (*v0.NamespaceDefinition, *NamespaceTypeSystem, decimal.Decimal, error)

	// Closes the namespace manager, disposing of any resources.
	//
	// NOTE: Should *not* call Close on the datastore.
	Close() error
}
