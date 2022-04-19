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

	// Close closes the namespace manager, disposing of any resources.
	//
	// NOTE: Should *not* call Close on the datastore.
	Close() error
}
