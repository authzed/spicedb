//go:build wasm

package testingserver

import (
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
)

// NewDispatcher returns the testing server's dispatcher. Under WASM, it has no relationship
// chunk cache, since caching is unsupported there; the cache only affects performance.
func NewDispatcher(caveatTypeSet *caveattypes.TypeSet) (dispatch.Dispatcher, error) {
	return graph.NewLocalOnlyDispatcher(graph.DispatcherParameters{
		TypeSet:           caveatTypeSet,
		ConcurrencyLimits: graph.SharedConcurrencyLimits(defaultConcurrencyLimit),
		DispatchChunkSize: defaultMaxChunkSize,
	})
}
