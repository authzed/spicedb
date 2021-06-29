package services

import (
	"context"
	"regexp"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/encoding/prototext"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/namespace"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
	"github.com/authzed/spicedb/pkg/tuple"
)

// DevContext holds the various helper types for running the developer calls.
type DevContext struct {
	Datastore  datastore.Datastore
	Revision   decimal.Decimal
	Namespaces []*v0.NamespaceInformation
	Dispatcher graph.Dispatcher
	Errors     []*v0.ValidationError
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

	namespaces, ok := parseNamespaces(requestContext.Namespaces)
	if !ok {
		return &DevContext{Namespaces: namespaces}, false, nil
	}

	err = loadNamespaces(ctx, namespaces, nsm, ds)
	if err != nil {
		return &DevContext{Namespaces: namespaces}, false, err
	}

	revision, validationErrors, err := loadTuples(ctx, requestContext.Tuples, nsm, ds)
	if err != nil {
		return &DevContext{Namespaces: namespaces}, false, err
	}

	return &DevContext{
		Datastore:  ds,
		Namespaces: namespaces,
		Revision:   revision,
		Dispatcher: dispatcher,
		Errors:     validationErrors,
	}, len(validationErrors) == 0, nil
}

func loadTuples(ctx context.Context, tuples []*v0.RelationTuple, nsm namespace.Manager, ds datastore.Datastore) (decimal.Decimal, []*v0.ValidationError, error) {
	var validationErrors []*v0.ValidationError
	var updates []*v0.RelationTupleUpdate
	for _, tpl := range tuples {
		err := validateTupleWrite(ctx, tpl, nsm)
		if err != nil {
			verrs, wireErr := rewriteGraphError(v0.ValidationError_VALIDATION_TUPLE, tuple.String(tpl), err)
			if wireErr == nil {
				validationErrors = append(validationErrors, verrs...)
				continue
			}

			return decimal.NewFromInt(0), validationErrors, wireErr
		}

		updates = append(updates, tuple.Touch(tpl))
	}

	revision, err := ds.WriteTuples(ctx, []*v0.RelationTuple{}, updates)
	return revision, validationErrors, err
}

func loadNamespaces(ctx context.Context, namespaces []*v0.NamespaceInformation, nsm namespace.Manager, ds datastore.Datastore) error {
	var nsDefs []*v0.NamespaceDefinition
	for _, nsInfo := range namespaces {
		nsDefs = append(nsDefs, nsInfo.Parsed)
	}

	for _, nsInfo := range namespaces {
		nsDef := nsInfo.Parsed
		ts, terr := namespace.BuildNamespaceTypeSystem(nsDef, nsm, nsDefs...)
		if terr != nil {
			return terr
		}

		tverr := ts.Validate(ctx)
		if tverr == nil {
			_, err := ds.WriteNamespace(ctx, nsDef)
			if err != nil {
				return err
			}
			continue
		}

		nsInfo.Errors = append(nsInfo.Errors, &v0.ValidationError{
			Message: tverr.Error(),
			Kind:    v0.ValidationError_NAMESPACE_CONFIG_ISSUE,
			Source:  v0.ValidationError_NAMESPACE_CONFIG,
		})
	}

	return nil
}

func parseNamespaces(nsContexts []*v0.NamespaceContext) ([]*v0.NamespaceInformation, bool) {
	var namespaces []*v0.NamespaceInformation
	var validationFailed = false
	for _, ns := range nsContexts {
		nsDef := v0.NamespaceDefinition{}
		nerr := prototext.Unmarshal([]byte(ns.Config), &nsDef)
		if nerr == nil {
			namespaces = append(namespaces, &v0.NamespaceInformation{
				Handle: ns.Handle,
				Parsed: &nsDef,
			})
			continue
		}

		var lineNumber uint64 = 0
		var columnNumber uint64 = 0
		var msg = nerr.Error()

		// NOTE: The use of a regex here is quite annoying, but as prototext does not currently
		// return *any* structured debug information, it is the only way to extract the
		// line and column position information.
		pieces := lineColRegex.FindStringSubmatch(nerr.Error())
		if len(pieces) == 4 {
			// We can ignore the errors here because the defaults are 0, which means not found.
			lineNumber, _ = strconv.ParseUint(pieces[1], 10, 0)
			columnNumber, _ = strconv.ParseUint(pieces[2], 10, 0)
			msg = pieces[3]
		}

		namespaces = append(namespaces, &v0.NamespaceInformation{
			Handle: ns.Handle,
			Errors: []*v0.ValidationError{
				&v0.ValidationError{
					Message: msg,
					Kind:    v0.ValidationError_NAMESPACE_CONFIG_ISSUE,
					Source:  v0.ValidationError_NAMESPACE_CONFIG,
					Line:    uint32(lineNumber),
					Column:  uint32(columnNumber),
				},
			},
		})
		validationFailed = true
	}

	return namespaces, !validationFailed
}
