// Package fullrelationcache caches the complete set of relationships for one
// objecttype:objectid#relation at one revision, and serves relationship queries
// from that set. A set never serves a query at a different revision.
package fullrelationcache

import (
	"slices"
	"time"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

// SetKey identifies one object#relation at one revision.
type SetKey string

// KeyString implements the cache key interface.
func (k SetKey) KeyString() string { return string(k) }

// NewSetKey returns the SetKey for an object#relation at a revision.
func NewSetKey(resourceType, objectID, relation, revision string) SetKey {
	return SetKey(resourceType + ":" + objectID + "#" + relation + "@" + revision)
}

// NewTooBigMemoKey returns the revision-independent key for a too-big memo.
// A valid type name or object ID cannot start with "!" or contain "::".
// Thus a memo key never parses as a relationship and never equals a set key.
func NewTooBigMemoKey(resourceType, objectID, relation string) SetKey {
	return SetKey("!toobig::" + resourceType + ":" + objectID + "#" + relation)
}

// CachedSet is the value for a SetKey or a too-big memo key.
// A memo has Complete=false. It sends its object#relation to the datastore at every revision
// until the memo is older than tooBigMemoReprobeInterval or the cache evicts it.
type CachedSet struct {
	// Complete is true if Rels holds every relationship of the object#relation at the revision of the key.
	Complete bool

	// Rels holds the relationships of a complete set. It is nil for a too-big memo.
	Rels []tuple.Relationship

	// MemoizedAt is the creation time of a too-big memo. It is zero for a complete set.
	MemoizedAt time.Time
}

// Cost returns the approximate size of the set in bytes.
func (s *CachedSet) Cost() int64 {
	cost := int64(64)
	for i := range s.Rels {
		cost += int64(s.Rels[i].SizeVT())
	}
	return cost
}

// serveFromSet answers a query from a complete set with the same semantics as the datastore.
// It applies expiry, the filter and the column-skipping options, in that order.
// SkipExpiration disables expiry, as in the SQL query builder.
// rel is a copy, so nulling fields never changes the cached set.
//
// The set key scopes the resource type, ID and relation, so the predicate omits those fields.
// servable rejects a filter with OptionalResourceIDPrefix.
func serveFromSet(set *CachedSet, filter datastore.RelationshipsFilter,
	opts options.QueryOptions, now time.Time,
) []tuple.Relationship {
	// RelationshipsFilter.Test skips the caveat and expiration filters when a subject selector matches,
	// and its selector match differs from the datastores. Thus the code matches the selectors itself,
	// and gives Test only the caveat and expiration filters.
	pred := filter
	pred.OptionalResourceType = ""
	pred.OptionalResourceIds = nil
	pred.OptionalResourceRelation = ""
	pred.OptionalSubjectsSelectors = nil
	selectors := filter.OptionalSubjectsSelectors

	var out []tuple.Relationship
	for _, rel := range set.Rels {
		if !opts.SkipExpiration && rel.OptionalExpiration != nil && !rel.OptionalExpiration.After(now) {
			// Expiry only removes rows, so the set stays complete.
			continue
		}
		if len(selectors) > 0 && !slices.ContainsFunc(selectors, func(sel datastore.SubjectsSelector) bool {
			return sel.Test(rel.Subject)
		}) {
			continue
		}
		if !pred.Test(rel) {
			continue
		}
		if opts.SkipCaveats {
			rel.OptionalCaveat = nil
		}
		if opts.SkipExpiration {
			rel.OptionalExpiration = nil
		}
		out = append(out, rel)
	}
	return out
}
