package changebuffer

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/common/pebbleutil"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ParsingFunc is a function that parses a revision string into a datastore.Revision.
// This is defined locally to avoid import cycles with revisionparsing package.
type ParsingFunc func(revisionStr string) (rev datastore.Revision, err error)

var (
	// <prefix><rev> -> <empty>
	revHasChanges = pebbleutil.PrefixFor("r2ce")
	// <prefix><rev><cav_name> -> <cav_proto>
	revToCaveatChanged = pebbleutil.PrefixFor("r2cc")
	// <prefix><rev><cav_name> -> <empty>
	revToCaveatDeleted = pebbleutil.PrefixFor("r2cd")
	// <prefix><rev><ns_name> -> <ns_proto>
	revToNsChanged = pebbleutil.PrefixFor("r2nc")
	// <prefix><rev><ns_name> -> <empty>
	revToNsDeleted = pebbleutil.PrefixFor("r2nd")
	// <prefix><rev><rel_string> -> <empty>
	revToRelDeleted = pebbleutil.PrefixFor("r2rd")
	// <prefix><rev><rel_string> -> <empty>
	revToRelTouch = pebbleutil.PrefixFor("r2rt")
	// <prefix><rev> -> <structpb_proto>
	revToMetadata = pebbleutil.PrefixFor("r2md")
)

// RevisionChanges represents the changes that occurred in a specific revision.
type RevisionChanges struct {
	Revision            datastore.Revision
	RelationshipChanges []tuple.RelationshipUpdate
	ChangedDefinitions  []datastore.SchemaDefinition
	DeletedNamespaces   []string
	DeletedCaveats      []string
	Metadatas           []*structpb.Struct
}

// Store stores relationship update data, uncompressed, from SpiceDB's datastore Watch stream.
// It is designed to be able to ingest unordered and potentially duplicated changes,
// and be capable to return the list of changes visible up to a specific revision.
//
// ⚠️ The assumption is that the upstream SpiceDB's datastore uses datastore.Revision that are sortable.
type Store struct {
	// No close method is provided as the pebble.DB is expected to be closed by the caller.
	db          *pebble.DB
	parsingFunc ParsingFunc
}

// New creates a new Store with the provided revision parsing function and pebble.DB.
// The client is responsible to manage the lifecycle of the pebble.DB.
func New(parsingFunc ParsingFunc, db *pebble.DB) (*Store, error) {
	return &Store{
		db:          db,
		parsingFunc: parsingFunc,
	}, nil
}

// IterateRevisionChangesUpToBoundary returns all changes that are below the provided upper-boundary revision.
//   - It will return one RevisionChanges per revision that will include all the changes that took place in that revision.
//   - The RevisionChanges will be delivered in order by revision.
//   - Any duplicate event will be deduplicated.
//
// The client is responsible to call DeleteRevisionsRange of the ranges consumed once they have been processed.
func (s *Store) IterateRevisionChangesUpToBoundary(ctx context.Context, upperBoundary datastore.Revision) iter.Seq2[RevisionChanges, error] {
	return func(yield func(RevisionChanges, error) bool) {
		// iterate over all revisions that had changes
		for rev, err := range pebbleutil.IterForKey(revHasChanges, s.db, s.byteToRevision) {
			if err != nil {
				yield(RevisionChanges{}, err)
				return
			}

			revString := rev.String()
			// determine if the revision is below the high watermark emitted by the checkpoint
			if rev.LessThan(upperBoundary) || rev.Equal(upperBoundary) {
				revChange := RevisionChanges{Revision: rev}

				touchedRels := make(map[tuple.Relationship]struct{})
				// obtain all changed relationships and add it to RevisionChange to emit
				revToRelTouchWithRev := revToRelTouch.WithSuffix(revString)
				for rel, err := range pebbleutil.IterForKey(revToRelTouchWithRev, s.db, s.byteToRelationship) {
					if err != nil {
						yield(RevisionChanges{}, err)
						return
					}

					// SpiceDB Watch is emitting rels with integrity. To have reliable comparison, it's safer to strip it
					rel = rel.WithoutIntegrity()
					touchedRels[rel] = struct{}{}
					revChange.RelationshipChanges = append(revChange.RelationshipChanges, tuple.Touch(rel))
				}

				revToRelDeletedRev := revToRelDeleted.WithSuffix(revString)
				for rel, err := range pebbleutil.IterForKey(revToRelDeletedRev, s.db, s.byteToRelationship) {
					if err != nil {
						yield(RevisionChanges{}, err)
						return
					}

					rel = rel.WithoutIntegrity()

					// In the original SpiceDB implementation, TOUCH takes precedence over DELETE.
					if _, ok := touchedRels[rel]; !ok {
						revChange.RelationshipChanges = append(revChange.RelationshipChanges, tuple.Delete(rel))
					}
				}

				changedNs := make(map[string]struct{})
				revToNsChangedWithRev := revToNsChanged.WithSuffix(revString)
				for nsDef, err := range pebbleutil.IterForValue(revToNsChangedWithRev, s.db, s.byteToNamespaceDefinition) {
					if err != nil {
						yield(RevisionChanges{}, err)
						return
					}

					changedNs[nsDef.GetName()] = struct{}{}
					revChange.ChangedDefinitions = append(revChange.ChangedDefinitions, nsDef)
				}

				revToNsDeletedWithRev := revToNsDeleted.WithSuffix(revString)
				for nsName, err := range pebbleutil.IterForKey(revToNsDeletedWithRev, s.db, s.byteToString) {
					if err != nil {
						yield(RevisionChanges{}, err)
						return
					}

					// We follow the same semantics as with relationships: changes take precedence to deletions
					if _, ok := changedNs[nsName]; !ok {
						revChange.DeletedNamespaces = append(revChange.DeletedNamespaces, nsName)
					}
				}

				changedCaveat := make(map[string]struct{})
				revToCaveatChangedWithRel := revToCaveatChanged.WithSuffix(revString)
				for caveatDef, err := range pebbleutil.IterForValue(revToCaveatChangedWithRel, s.db, s.byteToCaveatDefinition) {
					if err != nil {
						yield(RevisionChanges{}, err)
						return
					}

					changedCaveat[caveatDef.GetName()] = struct{}{}
					revChange.ChangedDefinitions = append(revChange.ChangedDefinitions, caveatDef)
				}

				revToCaveatDeletedWithRel := revToCaveatDeleted.WithSuffix(revString)
				for caveatName, err := range pebbleutil.IterForKey(revToCaveatDeletedWithRel, s.db, s.byteToString) {
					if err != nil {
						yield(RevisionChanges{}, err)
						return
					}

					// We follow the same semantics as with relationships: changes take precedence to deletions
					if _, ok := changedCaveat[caveatName]; !ok {
						revChange.DeletedCaveats = append(revChange.DeletedCaveats, caveatName)
					}
				}

				// Retrieve metadata for this revision if it exists
				revToMetadataWithRev := revToMetadata.WithSuffix(revString).Prefix()
				metadataBytes, closer, err := s.db.Get(revToMetadataWithRev)
				if err != nil && !errors.Is(err, pebble.ErrNotFound) {
					yield(RevisionChanges{}, err)
					return
				}
				if err == nil {
					metadata := &structpb.Struct{}
					//nolint:protogetter // structpb.Struct from google.golang.org/protobuf doesn't have UnmarshalVT
					if err := proto.Unmarshal(metadataBytes, metadata); err != nil {
						closer.Close()
						yield(RevisionChanges{}, fmt.Errorf("failed to unmarshal metadata: %w", err))
						return
					}
					closer.Close()
					revChange.Metadatas = append(revChange.Metadatas, metadata)
				}

				if !yield(revChange, nil) {
					return
				}
			}
		}
	}
}

// DeleteRevisionsRange deletes all the data related to the revisions in the range [lower, upper].
// Both lower and upper will also be deleted.
func (s *Store) DeleteRevisionsRange(ctx context.Context, lower, upper datastore.Revision) error {
	lowerRevString := lower.String()
	upperRevString := upper.String()

	ranges := make([]pebbleutil.Range, 0, 8)
	for _, f := range []func(lowerSuffix, upperSuffix string) (pebbleutil.Range, error){
		revHasChanges.ToRange,
		revToRelTouch.ToRange,
		revToRelDeleted.ToRange,
		revToNsChanged.ToRange,
		revToNsDeleted.ToRange,
		revToCaveatChanged.ToRange,
		revToCaveatDeleted.ToRange,
		revToMetadata.ToRange,
	} {
		r, err := f(lowerRevString, upperRevString)
		if err != nil {
			return err
		}

		ranges = append(ranges, r)
	}

	return s.deletePrefixRange(ranges...)
}

func (s *Store) deletePrefixRange(prefixRanges ...pebbleutil.Range) error {
	for _, prefixRange := range prefixRanges {
		if err := s.db.DeleteRange(prefixRange.Lower(), prefixRange.NextUpper(), pebble.NoSync); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) markRevisionWithChanges(rev datastore.Revision) error {
	if !rev.ByteSortable() {
		return errors.New("change store does not support non byte-sortable revisions")
	}

	revHasChangesWithRel := revHasChanges.WithSuffix(rev.String()).Prefix()
	if err := s.db.Set(revHasChangesWithRel, nil, pebble.NoSync); err != nil {
		return err
	}

	return nil
}

func (s *Store) AddRelationshipChange(_ context.Context, rev datastore.Revision, rel tuple.Relationship, op tuple.UpdateOperation) error {
	relString, err := tuple.String(rel)
	if err != nil {
		return err
	}

	if err := s.markRevisionWithChanges(rev); err != nil {
		return err
	}

	switch op {
	case tuple.UpdateOperationTouch:
		revToRelTouchWithRevAndRel := revToRelTouch.WithSuffix(rev.String()).WithSuffix(relString).Prefix()
		return s.db.Set(revToRelTouchWithRevAndRel, nil, pebble.NoSync)
	case tuple.UpdateOperationDelete:
		revToRelDeletedWithRevAndRel := revToRelDeleted.WithSuffix(rev.String()).WithSuffix(relString).Prefix()
		return s.db.Set(revToRelDeletedWithRevAndRel, nil, pebble.NoSync)
	case tuple.UpdateOperationCreate:
		// no CREATE come out of the CRDB Watch API
		return errors.New("unexpected update operation = create")
	default:
		return errors.New("unexpected update operation")
	}
}

type vtprotoMessage interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
}

func (s *Store) AddChangedDefinition(_ context.Context, rev datastore.Revision, def datastore.SchemaDefinition) error {
	if err := s.markRevisionWithChanges(rev); err != nil {
		return err
	}

	msg, ok := def.(vtprotoMessage)
	if !ok {
		return errors.New("schema definition must be a vtprotoMessage")
	}

	msgBytes, err := msg.MarshalVT()
	if err != nil {
		return fmt.Errorf("unable to process definition change: %w", err)
	}

	_, isNamespace := def.(*core.NamespaceDefinition)
	if isNamespace {
		revToNsChangedWithRevAndDef := revToNsChanged.WithSuffix(rev.String()).WithSuffix(def.GetName()).Prefix()
		return s.db.Set(revToNsChangedWithRevAndDef, msgBytes, pebble.NoSync)
	}

	_, isCaveat := def.(*core.CaveatDefinition)
	if isCaveat {
		revToCaveatChangedWithRevAndDef := revToCaveatChanged.WithSuffix(rev.String()).WithSuffix(def.GetName()).Prefix()
		return s.db.Set(revToCaveatChangedWithRevAndDef, msgBytes, pebble.NoSync)
	}

	return fmt.Errorf("unexpected definition type %T", def)
}

func (s *Store) AddDeletedNamespace(_ context.Context, rev datastore.Revision, namespaceName string) error {
	if err := s.markRevisionWithChanges(rev); err != nil {
		return err
	}

	revToNsDeletedWithRevWithNs := revToNsDeleted.WithSuffix(rev.String()).WithSuffix(namespaceName).Prefix()
	return s.db.Set(revToNsDeletedWithRevWithNs, nil, pebble.NoSync)
}

func (s *Store) AddDeletedCaveat(_ context.Context, rev datastore.Revision, caveatName string) error {
	if err := s.markRevisionWithChanges(rev); err != nil {
		return err
	}

	revToCaveatDeletedWithRevAndCaveat := revToCaveatDeleted.WithSuffix(rev.String()).WithSuffix(caveatName).Prefix()
	return s.db.Set(revToCaveatDeletedWithRevAndCaveat, nil, pebble.NoSync)
}

func (s *Store) AddRevisionMetadata(_ context.Context, rev datastore.Revision, metadata *structpb.Struct) error {
	if metadata == nil || len(metadata.Fields) == 0 {
		return nil
	}

	if err := s.markRevisionWithChanges(rev); err != nil {
		return err
	}

	// Serialize metadata using protobuf
	//nolint:protogetter // structpb.Struct from google.golang.org/protobuf doesn't have MarshalVT
	metadataBytes, err := proto.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	revToMetadataWithRev := revToMetadata.WithSuffix(rev.String()).Prefix()
	return s.db.Set(revToMetadataWithRev, metadataBytes, pebble.NoSync)
}

func (s *Store) byteToRevision(key []byte) (datastore.Revision, error) {
	return s.parsingFunc(string(key))
}

func (s *Store) byteToRelationship(bytes []byte) (tuple.Relationship, error) {
	return tuple.Parse(string(bytes))
}

func (s *Store) byteToString(bytes []byte) (string, error) {
	return string(bytes), nil
}

func (s *Store) byteToNamespaceDefinition(bytes []byte) (*core.NamespaceDefinition, error) {
	var def core.NamespaceDefinition
	err := def.UnmarshalVT(bytes)
	if err != nil {
		return nil, err
	}

	return &def, nil
}

func (s *Store) byteToCaveatDefinition(bytes []byte) (*core.CaveatDefinition, error) {
	var def core.CaveatDefinition
	err := def.UnmarshalVT(bytes)
	if err != nil {
		return nil, err
	}

	return &def, nil
}
