package relationships

import (
	"context"
	"errors"
	"fmt"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/proto"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// DefaultMaxUpdatesPerWrite is shared by embedded and service writes.
const DefaultMaxUpdatesPerWrite = 1000

// DefaultMaxRelationshipContextSize is the maximum serialized caveat size.
const DefaultMaxRelationshipContextSize = 25_000

// ValidateNativeUpdates applies structural constraints before schema validation.
func ValidateNativeUpdates(updates []tuple.RelationshipUpdate, maxUpdates, maxContext int, expiration bool) error {
	if len(updates) == 0 || len(updates) > maxUpdates {
		return fmt.Errorf("relationship update count must be between 1 and %d", maxUpdates)
	}
	seen := make(map[tuple.RelationshipReference]struct{}, len(updates))
	for _, u := range updates {
		switch u.Operation {
		case tuple.UpdateOperationTouch, tuple.UpdateOperationCreate, tuple.UpdateOperationDelete:
		default:
			return fmt.Errorf("unknown relationship operation: %d", u.Operation)
		}
		converted, err := tuple.UpdateToV1RelationshipUpdate(u)
		if err != nil {
			return err
		}
		if err := protovalidate.Validate(converted); err != nil {
			return err
		}
		if _, ok := seen[u.Relationship.RelationshipReference]; ok {
			return errors.New("duplicate relationship update")
		}
		seen[u.Relationship.RelationshipReference] = struct{}{}
		if proto.Size(converted.Relationship.OptionalCaveat) > maxContext {
			return errors.New("relationship caveat exceeds context size limit")
		}
		if !expiration && u.Relationship.OptionalExpiration != nil {
			return errors.New("expiring relationships are disabled")
		}
	}
	return nil
}

// ValidateFilter validates native filters against schema from the same reader.
func ValidateFilter(ctx context.Context, r datalayer.RevisionedReader, f datastore.RelationshipsFilter) error {
	if err := protovalidate.Validate(&v1.RelationshipFilter{ResourceType: f.OptionalResourceType, OptionalRelation: f.OptionalResourceRelation, OptionalResourceIdPrefix: f.OptionalResourceIDPrefix}); err != nil {
		return err
	}
	if f.OptionalExpirationOption < datastore.ExpirationFilterOptionNone || f.OptionalExpirationOption > datastore.ExpirationFilterOptionNoExpiration {
		return errors.New("invalid expiration filter")
	}
	if f.OptionalCaveatNameFilter.Option < datastore.CaveatFilterOptionNone || f.OptionalCaveatNameFilter.Option > datastore.CaveatFilterOptionNoCaveat {
		return errors.New("invalid caveat filter")
	}
	if len(f.OptionalResourceIds) > 0 && f.OptionalResourceIDPrefix != "" {
		return errors.New("resource IDs and prefix cannot both be specified")
	}
	if f.OptionalResourceType == "" && len(f.OptionalResourceIds) == 0 && f.OptionalResourceIDPrefix == "" && f.OptionalResourceRelation == "" && len(f.OptionalSubjectsSelectors) == 0 {
		return errors.New("relationship filter must not be empty")
	}
	for _, id := range f.OptionalResourceIds {
		if err := tuple.ValidateResourceID(id); err != nil {
			return err
		}
	}
	sr, err := r.ReadSchema(ctx)
	if err != nil {
		return err
	}
	check := func(typ, rel string) error {
		if typ == "" {
			return nil
		}
		allow := rel == ""
		if allow {
			rel = tuple.Ellipsis
		}
		return namespace.CheckNamespaceAndRelation(ctx, typ, rel, allow, sr)
	}
	if err := check(f.OptionalResourceType, f.OptionalResourceRelation); err != nil {
		return err
	}
	for _, selector := range f.OptionalSubjectsSelectors {
		if selector.RelationFilter.OnlyNonEllipsisRelations && (selector.RelationFilter.IncludeEllipsisRelation || selector.RelationFilter.NonEllipsisRelation != "") {
			return errors.New("incompatible subject relation filters")
		}
		sf := &v1.SubjectFilter{SubjectType: selector.OptionalSubjectType}
		if selector.RelationFilter.NonEllipsisRelation != "" {
			sf.OptionalRelation = &v1.SubjectFilter_RelationFilter{Relation: selector.RelationFilter.NonEllipsisRelation}
		}
		if err := protovalidate.Validate(sf); err != nil {
			return err
		}
		for _, id := range selector.OptionalSubjectIds {
			if err := tuple.ValidateSubjectID(id); err != nil {
				return err
			}
		}
		if err := check(selector.OptionalSubjectType, selector.RelationFilter.NonEllipsisRelation); err != nil {
			return err
		}
	}
	return nil
}
