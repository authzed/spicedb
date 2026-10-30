//go:build !wasm

package testingserver

import (
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
)

// NewDispatcher returns the testing server's dispatcher.
func NewDispatcher(caveatTypeSet *caveattypes.TypeSet) (dispatch.Dispatcher, error) {
	params, err := graph.NewDefaultDispatcherParametersForTesting()
	if err != nil {
		return nil, err
	}
	params.TypeSet = caveatTypeSet
	params.ConcurrencyLimits = graph.SharedConcurrencyLimits(defaultConcurrencyLimit)
	params.DispatchChunkSize = defaultMaxChunkSize
	return graph.NewLocalOnlyDispatcher(params)
}
