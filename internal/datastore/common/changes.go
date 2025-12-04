package common

import (
	"context"
	"maps"
	"slices"
	"sort"
	"sync"

	"github.com/ccoveille/go-safecast/v2"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	nsPrefix     = "n$"
	caveatPrefix = "c$"
)

// Changes represents a set of datastore mutations that are kept self-consistent
// across one or more transaction revisions.
// It is thread-safe.
type Changes[R datastore.Revision, K comparable] struct {
	recordsMutex    sync.RWMutex
	records         map[K]changeRecord[R] // GUARDED_BY(recordsMutex)
	keyFunc         func(R) K
	content         datastore.WatchContent
	maxByteSize     uint64
	currentByteSize int64
}

type changeRecord[R datastore.Revision] struct {
	rev                R
	relTouches         map[string]tuple.Relationship
	relDeletes         map[string]tuple.Relationship
	definitionsChanged map[string]datastore.SchemaDefinition
	namespacesDeleted  map[string]struct{}
	caveatsDeleted     map[string]struct{}
	metadatas          []TransactionMetadata
}

// NewChanges creates a new Changes object for change tracking and de-duplication.
func NewChanges[R datastore.Revision, K comparable](keyFunc func(R) K, content datastore.WatchContent, maxByteSize uint64) *Changes[R, K] {
	return &Changes[R, K]{
		records:         make(map[K]changeRecord[R], 0),
		keyFunc:         keyFunc,
		content:         content,
		maxByteSize:     maxByteSize,
		currentByteSize: 0,
	}
}

// IsEmpty returns if the change set is empty.
func (ch *Changes[R, K]) IsEmpty() bool {
	ch.recordsMutex.RLock()
	defer ch.recordsMutex.RUnlock()

	return len(ch.records) == 0
}

// AddRelationshipChange adds a specific change to the complete list of tracked changes
func (ch *Changes[R, K]) AddRelationshipChange(
	ctx context.Context,
	rev R,
	rel tuple.Relationship,
	op tuple.UpdateOperation,
) error {
	if ch.content&datastore.WatchRelationships != datastore.WatchRelationships {
		return nil
	}

	ch.recordsMutex.Lock()
	defer ch.recordsMutex.Unlock()

	record := ch.recordForRevision(rev)

	key := tuple.StringWithoutCaveatOrExpiration(rel)

	switch op {
	case tuple.UpdateOperationTouch:
		// If there was a delete for the same tuple at the same revision, drop it
		existing, ok := record.relDeletes[key]
		if ok {
			delete(record.relDeletes, key)
			if err := ch.adjustByteSize(existing, -1); err != nil {
				return err
			}
		}

		record.relTouches[key] = rel
		if err := ch.adjustByteSize(rel, 1); err != nil {
			return err
		}

	case tuple.UpdateOperationDelete:
		_, alreadyTouched := record.relTouches[key]
		if !alreadyTouched {
			record.relDeletes[key] = rel
			if err := ch.adjustByteSize(rel, 1); err != nil {
				return err
			}
		}

	default:
		return spiceerrors.MustBugf("unknown change operation")
	}

	return nil
}

type sized interface {
	SizeVT() int
}

func (ch *Changes[R, K]) adjustByteSize(item sized, delta int) error {
	if ch.maxByteSize == 0 {
		return nil
	}

	size := item.SizeVT() * delta
	ch.currentByteSize += int64(size)
	if ch.currentByteSize < 0 {
		return spiceerrors.MustBugf("byte size underflow")
	}

	currentByteSize, err := safecast.Convert[uint64](ch.currentByteSize)
	if err != nil {
		return spiceerrors.MustBugf("could not cast currentByteSize to uint64: %v", err)
	}

	if currentByteSize > ch.maxByteSize {
		return datastore.NewMaximumChangesSizeExceededError(ch.maxByteSize)
	}

	return nil
}

// AddRevisionMetadata adds the metadata for the given revision.
func (ch *Changes[R, K]) AddRevisionMetadata(ctx context.Context, rev R, metadata TransactionMetadata) error {
	if metadata.Len() == 0 {
		return nil
	}

	ch.recordsMutex.Lock()
	defer ch.recordsMutex.Unlock()

	record := ch.recordForRevision(rev)

	// Check if the metadata already exists on the record.
	for _, existingMetadata := range record.metadatas {
		if existingMetadata.Equals(metadata) {
			return nil
		}
	}

	record.metadatas = append(record.metadatas, metadata)
	ch.records[ch.keyFunc(rev)] = record
	return nil
}

// NOTE assumes that the mutex has been acquired.
func (ch *Changes[R, K]) recordForRevision(rev R) changeRecord[R] {
	k := ch.keyFunc(rev)
	revisionChanges, ok := ch.records[k]
	if !ok {
		revisionChanges = changeRecord[R]{
			rev,
			make(map[string]tuple.Relationship),
			make(map[string]tuple.Relationship),
			make(map[string]datastore.SchemaDefinition),
			make(map[string]struct{}),
			make(map[string]struct{}),
			make([]TransactionMetadata, 0),
		}
		ch.records[k] = revisionChanges
	}

	return revisionChanges
}

// AddDeletedNamespace adds a change indicating that the namespace with the name was deleted.
func (ch *Changes[R, K]) AddDeletedNamespace(
	_ context.Context,
	rev R,
	namespaceName string,
) error {
	if ch.content&datastore.WatchSchema != datastore.WatchSchema {
		return nil
	}

	ch.recordsMutex.Lock()
	defer ch.recordsMutex.Unlock()

	record := ch.recordForRevision(rev)

	// if a delete happens in the same transaction as a change, we assume it was a change in the first place
	// because that's how namespace changes are implemented in the MVCC
	if _, ok := record.definitionsChanged[nsPrefix+namespaceName]; ok {
		return nil
	}

	delete(record.definitionsChanged, nsPrefix+namespaceName)
	record.namespacesDeleted[namespaceName] = struct{}{}
	return nil
}

// AddDeletedCaveat adds a change indicating that the caveat with the name was deleted.
func (ch *Changes[R, K]) AddDeletedCaveat(
	_ context.Context,
	rev R,
	caveatName string,
) error {
	if ch.content&datastore.WatchSchema != datastore.WatchSchema {
		return nil
	}

	ch.recordsMutex.Lock()
	defer ch.recordsMutex.Unlock()

	record := ch.recordForRevision(rev)

	// if a delete happens in the same transaction as a change, we assume it was a change in the first place
	// because that's how namespace changes are implemented in the MVCC
	if _, ok := record.definitionsChanged[caveatPrefix+caveatName]; ok {
		return nil
	}

	delete(record.definitionsChanged, caveatPrefix+caveatName)
	record.caveatsDeleted[caveatName] = struct{}{}
	return nil
}

// AddChangedDefinition adds a change indicating that the schema definition (namespace or caveat)
// was changed to the definition given.
func (ch *Changes[R, K]) AddChangedDefinition(
	ctx context.Context,
	rev R,
	def datastore.SchemaDefinition,
) error {
	if ch.content&datastore.WatchSchema != datastore.WatchSchema {
		return nil
	}

	ch.recordsMutex.Lock()
	defer ch.recordsMutex.Unlock()

	record := ch.recordForRevision(rev)

	switch t := def.(type) {
	case *core.NamespaceDefinition:
		delete(record.namespacesDeleted, t.Name)

		if existing, ok := record.definitionsChanged[nsPrefix+t.Name]; ok {
			if err := ch.adjustByteSize(existing, -1); err != nil {
				return err
			}
		}

		record.definitionsChanged[nsPrefix+t.Name] = t

		if err := ch.adjustByteSize(t, 1); err != nil {
			return err
		}

	case *core.CaveatDefinition:
		delete(record.caveatsDeleted, t.Name)

		if existing, ok := record.definitionsChanged[nsPrefix+t.Name]; ok {
			if err := ch.adjustByteSize(existing, -1); err != nil {
				return err
			}
		}

		record.definitionsChanged[caveatPrefix+t.Name] = t

		if err := ch.adjustByteSize(t, 1); err != nil {
			return err
		}

	default:
		return spiceerrors.MustBugf("unknown schema definition kind")
	}

	return nil
}

// AsRevisionChanges returns the list of changes processed so far as a datastore watch
// compatible, ordered, changelist.
func (ch *Changes[R, K]) AsRevisionChanges(lessThanFunc func(lhs K, rhs K) bool) ([]datastore.RevisionChanges, error) {
	return ch.revisionChanges(lessThanFunc, *new(R), false)
}

// FilterAndRemoveRevisionChanges filters a list of changes processed up to the bound revision from the changes list, removing them
// and returning the filtered changes.
func (ch *Changes[R, K]) FilterAndRemoveRevisionChanges(lessThanFunc func(lhs, rhs K) bool, boundRev R) ([]datastore.RevisionChanges, error) {
	changes, err := ch.revisionChanges(lessThanFunc, boundRev, true)
	if err != nil {
		return nil, err
	}

	ch.removeAllChangesBefore(boundRev)
	return changes, nil
}

func (ch *Changes[R, K]) revisionChanges(lessThanFunc func(lhs K, rhs K) bool, boundRev R, withBound bool) ([]datastore.RevisionChanges, error) {
	if ch.IsEmpty() {
		return nil, nil
	}

	ch.recordsMutex.RLock()
	revisionsWithChanges := make([]K, 0, len(ch.records))
	for rk, cr := range ch.records {
		if !withBound || boundRev.GreaterThan(cr.rev) {
			revisionsWithChanges = append(revisionsWithChanges, rk)
		}
	}
	ch.recordsMutex.RUnlock()

	if len(revisionsWithChanges) == 0 {
		return nil, nil
	}

	sort.Slice(revisionsWithChanges, func(i int, j int) bool {
		return lessThanFunc(revisionsWithChanges[i], revisionsWithChanges[j])
	})

	changes := make([]datastore.RevisionChanges, len(revisionsWithChanges))

	ch.recordsMutex.RLock()
	defer ch.recordsMutex.RUnlock()

	for i, k := range revisionsWithChanges {
		revisionChangeRecord := ch.records[k]
		changes[i].Revision = revisionChangeRecord.rev
		for _, rel := range revisionChangeRecord.relTouches {
			changes[i].RelationshipChanges = append(changes[i].RelationshipChanges, tuple.Touch(rel))
		}
		for _, rel := range revisionChangeRecord.relDeletes {
			changes[i].RelationshipChanges = append(changes[i].RelationshipChanges, tuple.Delete(rel))
		}
		changes[i].ChangedDefinitions = slices.Collect(maps.Values(revisionChangeRecord.definitionsChanged))
		changes[i].DeletedNamespaces = slices.Collect(maps.Keys(revisionChangeRecord.namespacesDeleted))
		changes[i].DeletedCaveats = slices.Collect(maps.Keys(revisionChangeRecord.caveatsDeleted))

		if len(revisionChangeRecord.metadatas) > 0 {
			metadatas := make([]*structpb.Struct, 0, len(revisionChangeRecord.metadatas))
			for _, metadata := range revisionChangeRecord.metadatas {
				metadatas = append(metadatas, metadata.MustStruct())
			}

			changes[i].Metadatas = metadatas
		}
	}

	return changes, nil
}

func (ch *Changes[R, K]) removeAllChangesBefore(boundRev R) {
	ch.recordsMutex.Lock()
	defer ch.recordsMutex.Unlock()

	for rk, cr := range ch.records {
		if boundRev.GreaterThan(cr.rev) {
			delete(ch.records, rk)
		}
	}
}
