// Package setcache caches complete relationship sets per object#relation and
// serves relationship queries from them.
package setcache

import (
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
// A SpiceDB type name cannot start with "!", so memo keys never collide with set keys.
func NewTooBigMemoKey(resourceType, objectID, relation string) SetKey {
	return SetKey("!toobig:" + resourceType + ":" + objectID + "#" + relation)
}

// CachedSet is the value for a SetKey. A CachedSet with Complete=false is a too-big memo.
// The datastore serves a too-big object#relation at all revisions.
// This continues until the cache evicts the memo or the memo is older than tooBigMemoReprobeInterval.
type CachedSet struct {
	Complete bool
	Rels     []tuple.Relationship

	// MemoizedAt applies only to a too-big memo.
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
// servable rejects OptionalResourceIDPrefix.
func serveFromSet(set *CachedSet, filter datastore.RelationshipsFilter,
	opts options.QueryOptions, now time.Time,
) []tuple.Relationship {
	pred := filter
	pred.OptionalResourceType = ""
	pred.OptionalResourceIds = nil
	pred.OptionalResourceRelation = ""

	// RelationshipsFilter.Test returns when a subject selector matches and skips the caveat and expiration filters.
	// Thus the code evaluates those filters separately, without the selectors.
	noSelectors := pred
	noSelectors.OptionalSubjectsSelectors = nil
	hasSelectors := len(pred.OptionalSubjectsSelectors) > 0

	var out []tuple.Relationship
	for _, rel := range set.Rels {
		if !opts.SkipExpiration && rel.OptionalExpiration != nil && !rel.OptionalExpiration.After(now) {
			// Expiry only removes rows, so the set stays complete.
			continue
		}
		if hasSelectors && !pred.Test(rel) {
			continue
		}
		if !noSelectors.Test(rel) {
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
