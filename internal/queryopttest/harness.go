// Package queryopttest provides generated semantic equivalence tests for outline optimizations.
// It is test infrastructure and must not be imported by production query code.
package queryopttest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/query/queryopt/optimization"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
	"github.com/authzed/spicedb/pkg/schema/v2"
	schematesting "github.com/authzed/spicedb/pkg/schema/v2/testing"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Config identifies one pass and its supported request domain. Cases supplement
// the broad schema generator with shapes that specifically exercise the pass.
type Config struct {
	// Optimizers lists the composed passes to verify; when empty, Optimizer is used.
	Optimizers []optimization.Optimizer

	// Signals constructs the signal view used for a generated request.
	Signals func(Request) signal.ReadView

	// Hints constructs initial recommendations for a generated outline and request.
	Hints func(query.CanonicalOutline, Request) []planhint.Hint

	// Inspect optionally validates the optimized outline before semantic checks.
	Inspect func(query.CanonicalOutline) error

	// Optimizer is the individual pass verified when Optimizers is empty.
	Optimizer optimization.Optimizer

	// Applicable reports whether the optimizer supports the generated request.
	Applicable func(optimization.RequestParams) bool

	// Cases adds targeted schema and relationship generators to broad properties.
	Cases map[string]func(*rapid.T) Case

	// Setup initializes per-test state before generated properties run.
	Setup func(testing.TB)
}

// Case contains a generated schema, its valid relationships and bounded requests.
// RequireChange rejects targeted cases where every applicable plan is unchanged.
type Case struct {
	// Schema is the schema used to generate and evaluate the case.
	Schema *schema.Schema

	// CaveatDefinitions preserves compiler parameter types omitted by Schema.ToDefinitions.
	CaveatDefinitions []*core.CaveatDefinition

	// Relationships contains valid relationships for the generated schema.
	Relationships []tuple.Relationship

	// Requests contains bounded requests used to compare optimized semantics.
	Requests []Request

	// RequireChange requires at least one applicable plan to be changed.
	RequireChange bool
}

// Request defines the operation and its inputs. Lookups use Resource/Subject as
// their anchors and Params as filters; Permission is always a relation on Resource.
type Request struct {
	// Params contains request-specific optimization inputs.
	Params optimization.RequestParams

	// Resource is the resource anchor for the generated request.
	Resource query.Object

	// Permission is the relation or permission evaluated on the resource.
	Permission string

	// Subject is the subject anchor or filter for the generated request.
	Subject query.ObjectAndRelation

	// CaveatContexts lists request contexts used to verify caveated semantics.
	CaveatContexts []map[string]any
}

// Check runs broad and targeted equivalence properties with Rapid replay/shrinking.
func Check(t *testing.T, config Config) {
	t.Helper()

	if config.Setup != nil {
		config.Setup(t)
	}

	t.Run("generated", func(t *testing.T) {
		schematesting.CheckWithSchema(t, func(rt *rapid.T, s *schema.Schema, rg schematesting.RelationshipGenerator) {
			c := broadCase(rt, s, rg)
			_, err := verifyCase(rt.Context(), config, c)
			require.NoError(
				rt,
				err,
				"optimization=%s schema=%v relationships=%v requests=%+v",
				config.Optimizer.Name,
				schemaDescription(s),
				c.Relationships,
				c.Requests,
			)
		})
	})

	for _, name := range slices.Sorted(maps.Keys(config.Cases)) {
		t.Run(name, func(t *testing.T) {
			rapid.Check(t, func(rt *rapid.T) {
				c := config.Cases[name](rt)
				_, err := verifyCase(rt.Context(), config, c)
				require.NoError(
					rt,
					err,
					"optimization=%s schema=%v relationships=%v requests=%+v",
					config.Optimizer.Name,
					schemaDescription(c.Schema),
					c.Relationships,
					c.Requests,
				)
			})
		})
	}
}

func schemaDescription(s *schema.Schema) any {
	if s == nil {
		return nil
	}

	defs, caveats, err := s.ToDefinitions()
	return fmt.Sprintf("definitions=%v caveats=%v error=%v", defs, caveats, err)
}

func caseReader(ctx context.Context, c Case) (datalayer.RevisionedReader, func(), error) {
	ds, err := memdb.NewMemdbDatastore(0, time.Second, memdb.DisableGC)
	if err != nil {
		return nil, nil, err
	}

	closeDS := func() { _ = ds.Close() }
	defs, caveatDefs, err := c.Schema.ToDefinitions()
	if err != nil {
		closeDS()
		return nil, nil, err
	}

	if len(c.Schema.Caveats()) > 0 {
		if len(c.CaveatDefinitions) != len(c.Schema.Caveats()) {
			closeDS()
			return nil, nil, errors.New("caveated case must retain original caveat definitions; use ParseCase")
		}

		caveatDefs = c.CaveatDefinitions
	}

	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		if err := tx.LegacyWriteNamespaces(ctx, defs...); err != nil {
			return err
		}

		if len(caveatDefs) > 0 {
			if err := tx.LegacyWriteCaveats(ctx, caveatDefs); err != nil {
				return err
			}
		}

		updates := make([]tuple.RelationshipUpdate, 0, len(c.Relationships))

		for _, rel := range c.Relationships {
			updates = append(updates, tuple.Touch(rel))
		}

		return tx.WriteRelationships(ctx, updates)
	})
	if err != nil {
		closeDS()
		return nil, nil, err
	}

	return datalayer.NewDataLayer(ds).SnapshotReader(revision, datalayer.NoSchemaHashForTesting), closeDS, nil
}

func verifyCase(ctx context.Context, config Config, c Case) (bool, error) {
	if c.Schema == nil {
		return false, errors.New("nil case schema")
	}

	reader, closeReader, err := caseReader(ctx, c)
	if err != nil {
		return false, err
	}

	defer closeReader()
	schemaReader, err := reader.ReadSchema(ctx)
	if err != nil {
		return false, err
	}

	changed, applied := false, 0

	for _, req := range c.Requests {
		if req.Params.SubjectType != req.Subject.ObjectType || req.Params.SubjectRelation != req.Subject.Relation {
			return false, fmt.Errorf("request params do not match subject: %+v", req)
		}

		if config.Applicable != nil && !config.Applicable(req.Params) {
			continue
		}

		applied++
		before, err := query.BuildOutlineFromSchema(c.Schema, req.Resource.ObjectType, req.Permission)
		if err != nil {
			return false, err
		}

		after, err := query.BuildOutlineFromSchema(c.Schema, req.Resource.ObjectType, req.Permission)
		if err != nil {
			return false, err
		}

		opts := config.Optimizers
		if len(opts) == 0 {
			opts = []optimization.Optimizer{config.Optimizer}
		}

		p, planErr := queryopt.NewPipeline(opts)
		if planErr != nil {
			return false, planErr
		}

		var view signal.ReadView

		if config.Signals != nil {
			view = config.Signals(req)
		}

		optctx := optimization.NewContext(req.Params, view, nil)

		// Fake signal providers use a simple single-invocation/body population. Keep
		// this fixture here rather than supplying production defaults inside a pass.
		// Describe the current subtree so earlier composed rewrites cannot stale the key.
		optctx.Key = querysignalstest.ExecutionKey

		if config.Hints != nil {
			after = query.UniqueOutlineOccurrences(after)
			facts := query.DescribeOutline(after.Root)

			for _, hint := range config.Hints(after, req) {
				hint.Target.Fingerprint = facts[query.OutlineNodeID(hint.Target.Node)].Identity

				if _, hintErr := optctx.Ledger.Issue(hint); hintErr != nil {
					return false, hintErr
				}
			}
		}

		after, err = p.Run(after, optctx)
		if err != nil {
			return false, err
		}

		changed = changed || !before.Root.Equals(after.Root) || len(after.Hints) > 0

		if config.Inspect != nil {
			if err := config.Inspect(after); err != nil {
				return false, err
			}
		}

		contexts := req.CaveatContexts
		if len(contexts) == 0 {
			contexts = []map[string]any{nil}
		}

		for _, caveatContext := range contexts {
			left, err := execute(ctx, reader, before, req, caveatContext)
			if err != nil {
				return false, fmt.Errorf("before execution %+v: %w", req, err)
			}

			right, err := execute(ctx, reader, after, req, caveatContext)
			if err != nil {
				return false, fmt.Errorf("after execution %+v: %w", req, err)
			}

			if err := compareResults(ctx, schemaReader, req.Params.Operation, left, right, caveatContext); err != nil {
				return false, fmt.Errorf(
					"semantic mismatch: %s request=%+v context=%v before=%s after=%s: %w",
					config.Optimizer.Name,
					req,
					caveatContext,
					describeOutline(before.Root),
					describeOutline(after.Root),
					err,
				)
			}
		}
	}

	if applied == 0 {
		return false, errors.New("case has no applicable requests")
	}

	if c.RequireChange && !changed {
		return false, errors.New("targeted case did not change any outline")
	}

	return changed, nil
}

func execute(
	ctx context.Context,
	reader datalayer.RevisionedReader,
	plan query.CanonicalOutline,
	req Request,
	caveatContext map[string]any,
) ([]*query.Path, error) {
	it, err := plan.Compile()
	if err != nil {
		return nil, err
	}

	qctx := query.NewLocalContext(
		ctx,
		query.WithRevisionedReader(reader),
		query.WithCaveatRunner(caveats.NewCaveatRunner(types.Default.TypeSet)),
		query.WithCaveatContext(caveatContext),
	)

	if req.Params.Operation == query.OperationCheck {
		path, err := qctx.Check(it, req.Resource, req.Subject)
		if path == nil || err != nil {
			return nil, err
		}

		return []*query.Path{path}, nil
	}

	var seq query.PathSeq

	switch req.Params.Operation {
	case query.OperationIterSubjects:
		// Preserve the iterator's wildcard set before the public API projection.
		// This mirrors dispatch receivers and the existing wildcard semantics tests.
		qctx.MarkAsOperation(it, query.OperationIterSubjects)
		qctx.TargetSubjectType = query.NewType(req.Params.SubjectType, req.Params.SubjectRelation)
		seq, err = qctx.IterSubjects(it, req.Resource, query.NewType(req.Params.SubjectType, req.Params.SubjectRelation))
	case query.OperationIterResources:
		seq, err = qctx.IterResources(it, req.Subject, query.NoObjectFilter())
	default:
		return nil, fmt.Errorf("unsupported operation %v", req.Params.Operation)
	}

	if err != nil {
		return nil, err
	}

	var paths []*query.Path

	for path, err := range seq {
		if err != nil {
			return nil, err
		}

		if path == nil {
			return nil, errors.New("nil lookup path")
		}

		paths = append(paths, path)
	}

	return paths, nil
}

// Caveat nodes serialize only their identity, so serialize every node to retain
// the full tree in failure diagnostics rather than hiding caveat children.
func describeOutline(root query.Outline) string {
	var result strings.Builder
	var walk func(query.Outline, int)
	walk = func(node query.Outline, depth int) {
		fmt.Fprintf(&result, "\n%s%s", strings.Repeat("  ", depth), node.Serialize())

		for _, child := range node.SubOutlines {
			walk(child, depth+1)
		}
	}
	walk(root, 0)
	return result.String()
}
