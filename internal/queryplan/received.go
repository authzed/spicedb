package queryplan

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/authzed/spicedb/internal/querysignals"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/query/queryopt"
)

// FromWire prepares an already-optimized dispatch body under the receiver's policy.
// Wire identity and order survive local scheduling; learned transforms run on the sender.
func (p Planner) FromWire(ctx context.Context, request Request, wire []byte) (*Prepared, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if len(wire) == 0 {
		return nil, errors.New("DispatchQueryPlan: missing serialized plan")
	}

	profile := p.Profile.ForOperation(request.Params.Operation)
	reader, params := request.Schema, request.Params
	passes := profile.ReceivedOptimizers(params)
	var iterator query.Iterator
	var ledger *planhint.Ledger
	var err error

	// Local recommendations are reconstructed from the wire, rather than trusting
	// sender concurrency limits. Cache keys include exact bytes and local policy;
	// CheckMany shares the normalized Check body while retaining runtime result mode.
	if profile.ReuseCompiledPlan(params.Operation) || len(passes) > 0 {
		key := Key{Profile: profile, Operation: params.Operation, SerializedPlan: string(wire)}
		iterator, ledger, err = Load(ctx, reader, key, func(shared context.Context) (query.CanonicalOutline, error) {
			decoded, err := decode(shared, reader, strings.NewReader(key.SerializedPlan))
			if err != nil {
				return query.CanonicalOutline{}, err
			}

			outline, err := receivedOutline(decoded)
			if err != nil {
				return query.CanonicalOutline{}, err
			}

			return queryopt.ApplyOptimizations(outline, passes, params)
		})
	} else {
		iterator, err = decode(ctx, reader, bytes.NewReader(wire))
	}

	if err != nil {
		return nil, err
	}

	var collector *querysignals.Collector

	if profile.CollectSignals() {
		view, err := querysignals.Bind(p.Signals, reader, request.Revision, string(request.SchemaHash))
		if err != nil {
			return nil, err
		}

		collector = querysignals.NewCollector(view, querysignals.CollectorOptions{})
	}

	return prepared(profile, iterator, ledger, collector), nil
}

func decode(ctx context.Context, reader datalayer.SchemaReader, wire io.Reader) (query.Iterator, error) {
	schema, err := query.LoadSchema(ctx, reader)
	if err != nil {
		return nil, fmt.Errorf("DispatchQueryPlan: failed to build schema: %w", err)
	}

	iterator, err := query.Deserialize(wire, &query.DeserializeContext{Schema: schema})
	if err != nil {
		return nil, fmt.Errorf("DispatchQueryPlan: failed to deserialize plan: %w", err)
	}

	return iterator, nil
}

func receivedOutline(iterator query.Iterator) (query.CanonicalOutline, error) {
	outline, err := query.Decompile(iterator)
	if err != nil {
		return query.CanonicalOutline{}, err
	}

	// Preserve canonical keys from the sender rather than canonicalizing again:
	// receiver scheduling must not alter result-cache or nested dispatch identity.
	keys := map[query.OutlineNodeID]query.CanonicalKey{}
	outline = query.FillMissingNodeIDs(outline, keys)
	var retainKeys func(query.Outline, query.Iterator)
	retainKeys = func(node query.Outline, iterator query.Iterator) {
		keys[node.ID] = iterator.CanonicalKey()

		for index, child := range iterator.Subiterators() {
			retainKeys(node.SubOutlines[index], child)
		}
	}
	retainKeys(outline, iterator)
	return query.CanonicalOutline{Root: outline, CanonicalKeys: keys}, nil
}
