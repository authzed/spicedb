package services

import (
	"context"
	"regexp"
	"strconv"
	"time"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/namespace"
	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/encoding/prototext"
)

// DevContext holds the various helper types for running the developer calls.
type DevContext struct {
	Datastore  datastore.Datastore
	Revision   decimal.Decimal
	Namespaces []*api.NamespaceInformation
	Dispatcher graph.Dispatcher
	Errors     []*api.ValidationError
}

var lineColRegex = regexp.MustCompile(`\(line ([0-9]+):([0-9]+)\): (.+)`)

// NewDevContext creates a new DevContext from the specified request context, parsing and populating
// the datastore as needed.
func NewDevContext(ctx context.Context, requestContext *api.RequestContext) (DevContext, bool, error) {
	ds, err := memdb.NewMemdbDatastore(0, 0*time.Second, 0*time.Second, 0*time.Second)
	if err != nil {
		return DevContext{}, false, err
	}

	// Parse the namespaces.
	namespaces := []*api.NamespaceInformation{}

	var hasError = false
	for _, ns := range requestContext.Namespaces {
		nsDef := api.NamespaceDefinition{}
		nerr := prototext.Unmarshal([]byte(ns.Config), &nsDef)
		if nerr != nil {
			var lineNumber uint64 = 0
			var columnNumber uint64 = 0
			var msg = nerr.Error()

			// NOTE: The use of a regex here is quite annoying,but as prototext does not currently
			// return *any* structured debug information, it is the only way to extract the
			// line and column position information.
			pieces := lineColRegex.FindStringSubmatch(nerr.Error())
			if len(pieces) == 4 {
				lineNumber, _ = strconv.ParseUint(pieces[1], 10, 0)
				columnNumber, _ = strconv.ParseUint(pieces[2], 10, 0)
				msg = pieces[3]
			}

			namespaces = append(namespaces, &api.NamespaceInformation{
				Handle: ns.Handle,
				Errors: []*api.ValidationError{
					&api.ValidationError{
						Message: msg,
						Kind:    api.ValidationError_NAMESPACE_CONFIG_ISSUE,
						Source:  api.ValidationError_NAMESPACE_CONFIG,
						Line:    uint32(lineNumber),
						Column:  uint32(columnNumber),
					},
				},
			})
			hasError = true
			continue
		}

		namespaces = append(namespaces, &api.NamespaceInformation{
			Handle: ns.Handle,
			Parsed: &nsDef,
		})
	}

	if hasError {
		return DevContext{Namespaces: namespaces}, false, nil
	}

	// Load the namespace into the datastore.
	nsm, err := namespace.NewCachingNamespaceManager(ds, 0*time.Second, nil)
	if err != nil {
		return DevContext{Namespaces: namespaces}, false, err
	}

	nsDefs := []*api.NamespaceDefinition{}
	for _, nsInfo := range namespaces {
		nsDefs = append(nsDefs, nsInfo.Parsed)
	}

	for _, nsInfo := range namespaces {
		nsDef := nsInfo.Parsed
		ts, terr := namespace.BuildNamespaceTypeSystem(nsDef, nsm, nsDefs...)
		if terr != nil {
			return DevContext{Namespaces: namespaces}, false, terr
		}

		tverr := ts.Validate(ctx)
		if tverr != nil {
			hasError = true
			nsInfo.Errors = append(nsInfo.Errors, &api.ValidationError{
				Message: tverr.Error(),
				Kind:    api.ValidationError_NAMESPACE_CONFIG_ISSUE,
				Source:  api.ValidationError_NAMESPACE_CONFIG,
			})
			continue
		}

		_, err := ds.WriteNamespace(ctx, nsDef)
		if err != nil {
			return DevContext{Namespaces: namespaces}, false, err
		}
	}

	if hasError {
		return DevContext{Namespaces: namespaces}, false, err
	}

	dispatcher, err := graph.NewLocalDispatcher(nsm, ds)
	if err != nil {
		return DevContext{}, false, err
	}

	// Load tuples.
	errors := []*api.ValidationError{}

	updates := []*api.RelationTupleUpdate{}
	for _, tpl := range requestContext.Tuples {
		err := validateTupleWrite(ctx, tpl, nsm)
		if err != nil {
			verrs, wireErr := rewriteGraphError(api.ValidationError_VALIDATION_TUPLE, tuple.String(tpl), err)
			if wireErr != nil {
				return DevContext{}, false, wireErr
			}

			errors = append(errors, verrs...)
			continue
		}

		updates = append(updates, tuple.Touch(tpl))
	}

	revision, err := ds.WriteTuples(ctx, []*api.RelationTuple{}, updates)
	if err != nil {
		return DevContext{}, false, err
	}

	return DevContext{
		Datastore:  ds,
		Namespaces: namespaces,
		Revision:   revision,
		Dispatcher: dispatcher,
		Errors:     errors,
	}, len(errors) == 0, nil
}
