package namespace

import (
	"context"

	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

var tracer = otel.Tracer("spicedb/internal/namespace")

// Manager is a subset of the datastore interface that can read (and possibly nsCache) namespaces.
type Manager interface {
	// ReadNamespace reads a namespace definition and version and returns it if found.
	//
	// Returns ErrNamespaceNotFound if the namespace cannot be found.
	// Returns the direct downstream error for all other unknown error.
	ReadNamespace(ctx context.Context, nsName string, revision decimal.Decimal) (*core.NamespaceDefinition, error)

	// ReadNamespaceAndRelation checks that the specified namespace and relation exist in the
	// datastore.
	//
	// Returns ErrNamespaceNotFound if the namespace cannot be found.
	// Returns ErrRelationNotFound if the relation was not found in the namespace.
	// Returns the direct downstream error for all other unknown error.
	ReadNamespaceAndRelation(ctx context.Context, namespace, relation string, revision decimal.Decimal) (*core.NamespaceDefinition, *core.Relation, error)

	// CheckNamespaceAndRelation checks that the specified namespace and relation exist in the
	// datastore.
	//
	// Returns ErrNamespaceNotFound if the namespace cannot be found.
	// Returns ErrRelationNotFound if the relation was not found in the namespace.
	// Returns the direct downstream error for all other unknown error.
	CheckNamespaceAndRelation(ctx context.Context, namespace, relation string, allowEllipsis bool, revision decimal.Decimal) error

	// ReadNamespaceAndTypes reads a namespace definition, version, and type system and returns it if found.
	ReadNamespaceAndTypes(ctx context.Context, nsName string, revision decimal.Decimal) (*core.NamespaceDefinition, *TypeSystem, error)

	// Close closes the namespace manager, disposing of any resources.
	//
	// NOTE: Should *not* call Close on the datastore.
	Close() error
}
