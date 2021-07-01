package services

import (
	"context"
	"errors"
	"regexp"
	"time"

	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/namespace"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

// DevContext holds the various helper types for running the developer calls.
type DevContext struct {
	Datastore     datastore.Datastore
	Revision      decimal.Decimal
	Namespaces    []*v0.NamespaceDefinition
	Dispatcher    graph.Dispatcher
	RequestErrors []*v0.DeveloperError
}

var lineColRegex = regexp.MustCompile(`\(line ([0-9]+):([0-9]+)\): (.+)`)

// NewDevContext creates a new DevContext from the specified request context, parsing and populating
// the datastore as needed.
func NewDevContext(ctx context.Context, requestContext *v0.RequestContext) (*DevContext, bool, error) {
	ds, err := memdb.NewMemdbDatastore(0, 0*time.Second, 0*time.Second, 0*time.Second)
	if err != nil {
		return nil, false, err
	}

	nsm, err := namespace.NewCachingNamespaceManager(ds, 0*time.Second, nil)
	if err != nil {
		return nil, false, err
	}

	dispatcher, err := graph.NewLocalDispatcher(nsm, ds)
	if err != nil {
		return nil, false, err
	}

	empty := ""
	namespaces, err := compiler.Compile([]compiler.InputSchema{
		{
			Source:       input.InputSource("schema"),
			SchemaString: requestContext.Schema,
		},
	}, &empty)

	var contextError compiler.ErrorWithContext
	if errors.As(err, &contextError) {
		line, col, err := contextError.SourceRange.Start().LineAndColumn()
		if err != nil {
			return nil, false, err
		}

		return &DevContext{
			RequestErrors: []*v0.DeveloperError{
				{
					Message: contextError.Error(),
					Kind:    v0.DeveloperError_SCHEMA_ISSUE,
					Source:  v0.DeveloperError_SCHEMA,
					Line:    uint32(line) + 1, // 0-indexed in parser.
					Column:  uint32(col) + 1,  // 0-indexed in parser.
				},
			},
		}, false, nil
	}

	if err != nil {
		return &DevContext{Namespaces: namespaces}, false, err
	}

	requestErrors, err := loadNamespaces(ctx, namespaces, nsm, ds)
	if err != nil {
		return &DevContext{}, false, err
	}

	if len(requestErrors) > 0 {
		return &DevContext{RequestErrors: requestErrors}, false, nil
	}

	revision, requestErrors, err := loadTuples(ctx, requestContext.Relationships, nsm, ds)
	if err != nil {
		return &DevContext{Namespaces: namespaces}, false, err
	}

	return &DevContext{
		Datastore:     ds,
		Namespaces:    namespaces,
		Revision:      revision,
		Dispatcher:    dispatcher,
		RequestErrors: requestErrors,
	}, len(requestErrors) == 0, nil
}

func loadTuples(ctx context.Context, tuples []*v0.RelationTuple, nsm namespace.Manager, ds datastore.Datastore) (decimal.Decimal, []*v0.DeveloperError, error) {
	var errors []*v0.DeveloperError
	var updates []*v0.RelationTupleUpdate
	for _, tpl := range tuples {
		err := validateTupleWrite(ctx, tpl, nsm)
		if err != nil {
			verrs, wireErr := rewriteGraphError(v0.DeveloperError_RELATIONSHIP, 0, 0, tuple.String(tpl), err)
			if wireErr == nil {
				errors = append(errors, verrs...)
				continue
			}

			return decimal.NewFromInt(0), errors, wireErr
		}

		updates = append(updates, tuple.Touch(tpl))
	}

	revision, err := ds.WriteTuples(ctx, []*v0.RelationTuple{}, updates)
	return revision, errors, err
}

func loadNamespaces(ctx context.Context, namespaces []*v0.NamespaceDefinition, nsm namespace.Manager, ds datastore.Datastore) ([]*v0.DeveloperError, error) {
	var errors []*v0.DeveloperError
	for _, nsDef := range namespaces {
		ts, terr := namespace.BuildNamespaceTypeSystem(nsDef, nsm, namespaces...)
		if terr != nil {
			return errors, terr
		}

		tverr := ts.Validate(ctx)
		if tverr == nil {
			_, err := ds.WriteNamespace(ctx, nsDef)
			if err != nil {
				return errors, err
			}
			continue
		}

		errors = append(errors, &v0.DeveloperError{
			Message: tverr.Error(),
			Kind:    v0.DeveloperError_SCHEMA_ISSUE,
			Source:  v0.DeveloperError_SCHEMA,
			Context: nsDef.Name,
		})
	}

	return errors, nil
}
