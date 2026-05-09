/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package spanner

import (
	"context"
	"sync"
	"sync/atomic"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

type channelFinder struct {
	updateMu sync.Mutex

	databaseID  atomic.Uint64
	recipeCache *keyRecipeCache
	rangeCache  *keyRangeCache
}

func newChannelFinder(endpointCache channelEndpointCache) *channelFinder {
	return &channelFinder{
		recipeCache: newKeyRecipeCache(),
		rangeCache:  newKeyRangeCache(endpointCache),
	}
}

func (f *channelFinder) useDeterministicRandom() {
	f.rangeCache.useDeterministicRandom()
}

func (f *channelFinder) update(update *sppb.CacheUpdate) {
	if update == nil {
		return
	}
	f.updateMu.Lock()
	defer f.updateMu.Unlock()

	currentID := f.databaseID.Load()
	if currentID != update.GetDatabaseId() {
		if currentID != 0 {
			f.recipeCache.clear()
			f.rangeCache.clear()
		}
		f.databaseID.Store(update.GetDatabaseId())
	}
	if update.GetKeyRecipes() != nil {
		f.recipeCache.addRecipes(update.GetKeyRecipes())
	}
	f.rangeCache.addRanges(update)
}

func (f *channelFinder) findServerRead(ctx context.Context, req *sppb.ReadRequest, preferLeader bool) channelEndpoint {
	if req == nil {
		return nil
	}
	f.recipeCache.computeReadKeys(req)
	hint := ensureReadRoutingHint(req)
	return f.fillRoutingHint(ctx, preferLeader, rangeModeCoveringSplit, req.GetDirectedReadOptions(), hint)
}

func (f *channelFinder) findServerReadWithTransaction(ctx context.Context, req *sppb.ReadRequest) channelEndpoint {
	if req == nil {
		return nil
	}
	return f.findServerRead(ctx, req, preferLeaderFromSelector(req.GetTransaction()))
}

func (f *channelFinder) findServerExecuteSQL(ctx context.Context, req *sppb.ExecuteSqlRequest, preferLeader bool) channelEndpoint {
	if req == nil {
		return nil
	}
	f.recipeCache.computeQueryKeys(req)
	hint := ensureExecuteSQLRoutingHint(req)
	return f.fillRoutingHint(ctx, preferLeader, rangeModePickRandom, req.GetDirectedReadOptions(), hint)
}

func (f *channelFinder) findServerExecuteSQLWithTransaction(ctx context.Context, req *sppb.ExecuteSqlRequest) channelEndpoint {
	if req == nil {
		return nil
	}
	return f.findServerExecuteSQL(ctx, req, preferLeaderFromSelector(req.GetTransaction()))
}

func (f *channelFinder) findServerBeginTransaction(ctx context.Context, req *sppb.BeginTransactionRequest) channelEndpoint {
	if req == nil || req.GetMutationKey() == nil {
		return nil
	}
	return f.routeMutation(ctx, req.GetMutationKey(), preferLeaderFromTransactionOptions(req.GetOptions()), ensureBeginTransactionRoutingHint(req))
}

func (f *channelFinder) fillCommitRoutingHint(ctx context.Context, req *sppb.CommitRequest) channelEndpoint {
	if req == nil {
		return nil
	}
	mutation := selectMutationProtoForRouting(req.GetMutations())
	if mutation == nil {
		return nil
	}
	return f.routeMutation(ctx, mutation, true, ensureCommitRoutingHint(req))
}

func (f *channelFinder) routeMutation(ctx context.Context, mutation *sppb.Mutation, preferLeader bool, hint *sppb.RoutingHint) channelEndpoint {
	if mutation == nil || hint == nil {
		return nil
	}
	f.recipeCache.applySchemaGeneration(hint)
	target := f.recipeCache.mutationToTargetRange(mutation)
	if target == nil {
		return nil
	}
	f.recipeCache.applyTargetRange(hint, target)
	return f.fillRoutingHint(ctx, preferLeader, rangeModeCoveringSplit, &sppb.DirectedReadOptions{}, hint)
}

func (f *channelFinder) fillRoutingHint(ctx context.Context, preferLeader bool, mode rangeMode, directedReadOptions *sppb.DirectedReadOptions, hint *sppb.RoutingHint) channelEndpoint {
	if hint == nil {
		return nil
	}
	databaseID := f.databaseID.Load()
	if databaseID == 0 {
		return nil
	}
	hint.DatabaseId = databaseID
	return f.rangeCache.fillRoutingHint(ctx, preferLeader, mode, directedReadOptions, hint)
}

func preferLeaderFromSelector(selector *sppb.TransactionSelector) bool {
	if selector == nil {
		return true
	}
	switch s := selector.GetSelector().(type) {
	case *sppb.TransactionSelector_Begin:
		if s.Begin == nil || s.Begin.GetReadOnly() == nil {
			return true
		}
		return s.Begin.GetReadOnly().GetStrong()
	case *sppb.TransactionSelector_SingleUse:
		if s.SingleUse == nil || s.SingleUse.GetReadOnly() == nil {
			return true
		}
		return s.SingleUse.GetReadOnly().GetStrong()
	default:
		return true
	}
}

func preferLeaderFromTransactionOptions(options *sppb.TransactionOptions) bool {
	if options == nil || options.GetReadOnly() == nil {
		return true
	}
	return options.GetReadOnly().GetStrong()
}
