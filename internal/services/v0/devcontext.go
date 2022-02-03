package v0

import (
	"context"
	"errors"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

// DevContext holds the various helper types for running the developer calls.
type DevContext struct {
	Ctx              context.Context
	Datastore        datastore.Datastore
	Revision         decimal.Decimal
	Namespaces       []*v0.NamespaceDefinition
	Dispatcher       dispatch.Dispatcher
	RequestErrors    []*v0.DeveloperError
	NamespaceManager namespace.Manager
}

// NewDevContext creates a new DevContext from the specified request context, parsing and populating
// the datastore as needed.
func NewDevContext(ctx context.Context, requestContext *v0.RequestContext) (*DevContext, bool, error) {
	ds, err := memdb.NewMemdbDatastore(0, 0*time.Second, 0*time.Second, 0*time.Second)
	if err != nil {
		return nil, false, err
	}

	dctx, ok, err := newDevContext(ctx, requestContext, ds)
	if !ok || err != nil {
		err := dctx.NamespaceManager.Close()
		if err != nil {
			return nil, false, err
		}

		err = ds.Close()
		if err != nil {
			return nil, false, err
		}
	}

	return dctx, ok, err
}

func newDevContext(ctx context.Context, requestContext *v0.RequestContext, ds datastore.Datastore) (*DevContext, bool, error) {
	nsm, err := namespace.NewCachingNamespaceManager(ds, 0*time.Second, nil)
	if err != nil {
		return nil, false, err
	}

	dispatcher := graph.NewLocalOnlyDispatcher(nsm)

	ctx = datastoremw.ContextWithHandle(ctx)
	if err := datastoremw.SetInContext(ctx, ds); err != nil {
		return nil, false, err
	}

	namespaces, devError, err := compile(requestContext.Schema)
	if err != nil {
		return &DevContext{Ctx: ctx, NamespaceManager: nsm}, false, err
	}

	if devError != nil {
		return &DevContext{Ctx: ctx, NamespaceManager: nsm, RequestErrors: []*v0.DeveloperError{devError}}, false, nil
	}

	var currentRevision decimal.Decimal
	var requestErrors []*v0.DeveloperError
	requestErrors, currentRevision, err = loadNamespaces(ctx, namespaces, nsm, ds)
	if err != nil {
		return &DevContext{Ctx: ctx, NamespaceManager: nsm}, false, err
	}

	if len(requestErrors) > 0 {
		return &DevContext{Ctx: ctx, NamespaceManager: nsm, RequestErrors: requestErrors}, false, nil
	}

	if len(requestContext.LegacyNsConfigs) > 0 {
		requestErrors, currentRevision, err = loadNamespaces(ctx, requestContext.LegacyNsConfigs, nsm, ds)
		if err != nil {
			return &DevContext{Ctx: ctx, NamespaceManager: nsm}, false, err
		}

		if len(requestErrors) > 0 {
			return &DevContext{Ctx: ctx, NamespaceManager: nsm, RequestErrors: requestErrors}, false, nil
		}
	}

	revision, requestErrors, err := loadTuples(ctx, requestContext.Relationships, nsm, ds, currentRevision)
	if err != nil {
		return &DevContext{Ctx: ctx, NamespaceManager: nsm, Namespaces: namespaces}, false, err
	}

	if len(requestErrors) == 0 {
		err = requestContext.Validate()
		if err != nil {
			return &DevContext{Ctx: ctx, NamespaceManager: nsm, Namespaces: namespaces}, false, err
		}
	}

	return &DevContext{
		Ctx:              ctx,
		Datastore:        ds,
		Namespaces:       namespaces,
		Revision:         revision,
		Dispatcher:       dispatcher,
		RequestErrors:    requestErrors,
		NamespaceManager: nsm,
	}, len(requestErrors) == 0, nil
}

func (dc *DevContext) dispose() {
	if err := dc.Dispatcher.Close(); err != nil {
		log.Ctx(dc.Ctx).Err(err).Msg("error when disposing of dispatcher in devcontext")
	}
	if dc.Datastore == nil {
		return
	}
	err := dc.NamespaceManager.Close()
	if err != nil {
		log.Ctx(dc.Ctx).Err(err).Msg("error when disposing of namespace manager in devcontext")
	}

	err = dc.Datastore.Close()
	if err != nil {
		log.Ctx(dc.Ctx).Err(err).Msg("error when disposing of datastore in devcontext")
	}
}

func compile(schema string) ([]*v0.NamespaceDefinition, *v0.DeveloperError, error) {
	empty := ""
	namespaces, err := compiler.Compile([]compiler.InputSchema{
		{
			Source:       input.Source("schema"),
			SchemaString: schema,
		},
	}, &empty)

	var contextError compiler.ErrorWithContext
	if errors.As(err, &contextError) {
		line, col, err := contextError.SourceRange.Start().LineAndColumn()
		if err != nil {
			return []*v0.NamespaceDefinition{}, nil, err
		}

		return []*v0.NamespaceDefinition{}, &v0.DeveloperError{
			Message: contextError.Error(),
			Kind:    v0.DeveloperError_SCHEMA_ISSUE,
			Source:  v0.DeveloperError_SCHEMA,
			Line:    uint32(line) + 1, // 0-indexed in parser.
			Column:  uint32(col) + 1,  // 0-indexed in parser.
		}, nil
	}

	if err != nil {
		return []*v0.NamespaceDefinition{}, nil, err
	}

	return namespaces, nil, nil
}

func loadTuples(ctx context.Context, tuples []*v0.RelationTuple, nsm namespace.Manager, ds datastore.Datastore, revision decimal.Decimal) (decimal.Decimal, []*v0.DeveloperError, error) {
	errors := make([]*v0.DeveloperError, 0, len(tuples))
	updates := make([]*v1.RelationshipUpdate, 0, len(tuples))
	for _, tpl := range tuples {
		verr := tpl.Validate()
		if verr != nil {
			errors = append(errors, &v0.DeveloperError{
				Message: verr.Error(),
				Source:  v0.DeveloperError_RELATIONSHIP,
				Kind:    v0.DeveloperError_PARSE_ERROR,
				Context: tuple.String(tpl),
			})
			continue
		}

		err := validateTupleWrite(ctx, tpl, nsm, revision)
		if err != nil {
			verrs, wireErr := rewriteGraphError(ctx, v0.DeveloperError_RELATIONSHIP, 0, 0, tuple.String(tpl), err)
			if wireErr == nil {
				errors = append(errors, verrs...)
				continue
			}

			return decimal.NewFromInt(0), errors, wireErr
		}

		updates = append(updates, &v1.RelationshipUpdate{
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: tuple.MustToRelationship(tpl),
		})
	}

	revision, err := ds.WriteTuples(ctx, nil, updates)
	return revision, errors, err
}

func loadNamespaces(
	ctx context.Context,
	namespaces []*v0.NamespaceDefinition,
	nsm namespace.Manager,
	ds datastore.Datastore,
) ([]*v0.DeveloperError, decimal.Decimal, error) {
	errors := make([]*v0.DeveloperError, 0, len(namespaces))
	var lastRevision decimal.Decimal
	for _, nsDef := range namespaces {
		ts, terr := namespace.BuildNamespaceTypeSystemForDefs(nsDef, namespaces)
		if terr != nil {
			return errors, lastRevision, terr
		}

		tverr := ts.Validate(ctx)
		if tverr == nil {
			var err error
			lastRevision, err = ds.WriteNamespace(ctx, nsDef)
			if err != nil {
				return errors, lastRevision, err
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

	return errors, lastRevision, nil
}
