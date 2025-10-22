package query

import (
	"errors"
	"fmt"
	"iter"
	"maps"
	"time"

	"github.com/authzed/spicedb/internal/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// PathSeq is the intermediate iter closure that any of the planning calls return.
type PathSeq iter.Seq2[Path, error]

// EmptyPathSeq returns an empty iterator, that is error-free but empty.
func EmptyPathSeq() PathSeq {
	return func(yield func(Path, error) bool) {}
}

// Path is an abstract notion of an individual relation. While tuple.Relation is what is stored under the hood,
// this represents a virtual relation, one that may either be backed by a real tuple, or one that is constructed from
// a query path, equivalent to a subtree of a query.Plan.
// `permission foo = bar | baz`, for example, is a Path named foo that can be constructed by either the bar path or the baz path
// (which themselves may be other paths, down to individual, stored, relations.)
type Path struct {
	Resource   Object
	Relation   string
	Subject    ObjectAndRelation
	Caveat     *core.CaveatExpression
	Expiration *time.Time
	Integrity  []*core.RelationshipIntegrity

	Metadata map[string]any
}

// ResourceOAR returns the resource as an ObjectAndRelation with the current relation type.
func (p Path) ResourceOAR() ObjectAndRelation {
	return p.Resource.WithRelation(p.Relation)
}

// Key returns a unique string key for this Path based on its resource and subject
func (p Path) Key() string {
	return fmt.Sprintf("%s#%s@%s", p.Resource.Key(), p.Relation, ObjectAndRelationKey(p.Subject))
}

// MergeOr combines the paths, ORing the caveats and expiration and metadata together.
// Returns a new Path with the merged values.
func (p Path) MergeOr(other Path) (Path, error) {
	return p.mergeFrom(other, func(pCaveat, otherCaveat *core.CaveatExpression) *core.CaveatExpression {
		if pCaveat != nil && otherCaveat != nil {
			return caveats.Or(pCaveat, otherCaveat)
		}
		// Since this is ORing together, and at least one caveat is nil,
		// any caveat combined with no caveat is equivalent to no caveat. (Trivially passing)
		return nil
	})
}

// MergeAnd combines the paths, ANDing the caveats and expiration and metadata together.
// Returns a new Path with the merged values.
func (p Path) MergeAnd(other Path) (Path, error) {
	return p.mergeFrom(other, func(pCaveat, otherCaveat *core.CaveatExpression) *core.CaveatExpression {
		if pCaveat != nil {
			if otherCaveat != nil {
				return caveats.And(pCaveat, otherCaveat)
			}
			return pCaveat
		}
		// pCaveat must be nil; so it's equivalent to otherCaveat (which may also be nil)
		return otherCaveat
	})
}

// MergeAndNot combines the paths, subtracting the caveats and expiration and metadata together.
// Returns a new Path with the merged values.
func (p Path) MergeAndNot(other Path) (Path, error) {
	return p.mergeFrom(other, func(pCaveat, otherCaveat *core.CaveatExpression) *core.CaveatExpression {
		if otherCaveat != nil {
			// If pCaveat is nil, this turns it into a negation (Invert() in caveats package)
			// Otherwise it's a subtraction.
			return caveats.Subtract(pCaveat, otherCaveat)
		}
		// If we're subtracting no caveat, then just the original one.
		return pCaveat
	})
}

func (p Path) mergeFrom(other Path, caveatMerger func(pCaveat, otherCaveat *core.CaveatExpression) *core.CaveatExpression) (Path, error) {
	// Check if they have the same Resource and Subject types and IDs
	if !p.Resource.Equals(other.Resource) {
		return Path{}, fmt.Errorf("cannot merge paths with different resources: %v vs %v", p.Resource, other.Resource)
	}

	pSubject := GetObject(p.Subject)
	otherSubject := GetObject(other.Subject)
	if !pSubject.Equals(otherSubject) {
		return Path{}, fmt.Errorf("cannot merge paths with different subjects: %v vs %v", pSubject, otherSubject)
	}

	// Create a new Path with merged values
	result := Path{
		Resource: p.Resource,
		Subject:  p.Subject,
	}

	// Clear Relation unless both have the same Relation string
	if p.Relation == other.Relation {
		result.Relation = p.Relation
	}

	// Combine caveats using the provided merger function
	result.Caveat = caveatMerger(p.Caveat, other.Caveat)

	// Keep any Expiration, and if there are two of them, take the earlier one
	if other.Expiration != nil {
		if p.Expiration == nil || other.Expiration.Before(*p.Expiration) {
			result.Expiration = other.Expiration
		} else {
			result.Expiration = p.Expiration
		}
	} else {
		result.Expiration = p.Expiration
	}

	// Append all integrities together
	result.Integrity = make([]*core.RelationshipIntegrity, 0, len(p.Integrity)+len(other.Integrity))
	result.Integrity = append(result.Integrity, p.Integrity...)
	result.Integrity = append(result.Integrity, other.Integrity...)

	// Merge the metadata by combining both maps
	// WARNING: This is a simple overwrite strategy and may not be appropriate for all use cases.
	// Better is probably to have a more structured Metadata type, with a Merge() function.
	if p.Metadata != nil || other.Metadata != nil {
		result.Metadata = make(map[string]any)
		if p.Metadata != nil {
			maps.Copy(result.Metadata, p.Metadata)
		}
		if other.Metadata != nil {
			maps.Copy(result.Metadata, other.Metadata)
		}
	}

	return result, nil
}

func (p Path) IsExpired() bool {
	if p.Expiration == nil {
		return false
	}
	return time.Now().After(*p.Expiration)
}

// FromRelationship creates a new Path from a tuple.Relationship.
func FromRelationship(rel tuple.Relationship) Path {
	resource := Object{
		ObjectID:   rel.Resource.ObjectID,
		ObjectType: rel.Resource.ObjectType,
	}

	var caveat *core.CaveatExpression
	if rel.OptionalCaveat != nil {
		caveat = caveats.CaveatAsExpr(rel.OptionalCaveat)
	}

	var integrity []*core.RelationshipIntegrity
	if rel.OptionalIntegrity != nil {
		integrity = []*core.RelationshipIntegrity{rel.OptionalIntegrity}
	}

	return Path{
		Resource:   resource,
		Relation:   rel.Resource.Relation,
		Subject:    rel.Subject,
		Caveat:     caveat,
		Expiration: rel.OptionalExpiration,
		Integrity:  integrity,
		Metadata:   make(map[string]any),
	}
}

// ToRelationship converts the Path to a tuple.Relationship.
func (p Path) ToRelationship() (tuple.Relationship, error) {
	if p.Relation == "" {
		return tuple.Relationship{}, errors.New("cannot convert Path with empty Relation to Relationship")
	}

	resourceOAR := ObjectAndRelation{
		ObjectID:   p.Resource.ObjectID,
		ObjectType: p.Resource.ObjectType,
		Relation:   p.Relation,
	}

	var caveat *core.ContextualizedCaveat
	if p.Caveat != nil {
		if p.Caveat.GetCaveat() != nil {
			caveat = p.Caveat.GetCaveat()
		} else {
			// For complex caveat expressions, we cannot directly convert to a single ContextualizedCaveat
			return tuple.Relationship{}, errors.New("cannot convert Path with complex caveat expression to Relationship")
		}
	}

	var integrity *core.RelationshipIntegrity
	if len(p.Integrity) > 0 {
		if len(p.Integrity) > 1 {
			return tuple.Relationship{}, errors.New("cannot convert Path with multiple integrity values to Relationship")
		}
		integrity = p.Integrity[0]
	}

	return tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{
			Resource: resourceOAR,
			Subject:  p.Subject,
		},
		OptionalCaveat:     caveat,
		OptionalExpiration: p.Expiration,
		OptionalIntegrity:  integrity,
	}, nil
}

// MustPathFromString is a helper function for tests that creates a Path from a relationship string.
// It uses tuple.MustParse to parse the string and then converts it to a Path using FromRelationship.
// Example: MustPathFromString("document:doc1#viewer@user:alice")
func MustPathFromString(relationshipStr string) Path {
	rel := tuple.MustParse(relationshipStr)
	return FromRelationship(rel)
}

// EqualsEndpoints checks if two paths have the same Resource and Subject endpoints (types and IDs only)
func (p Path) EqualsEndpoints(other Path) bool {
	return p.Resource.ObjectType == other.Resource.ObjectType &&
		p.Resource.ObjectID == other.Resource.ObjectID &&
		p.Subject.ObjectType == other.Subject.ObjectType &&
		p.Subject.ObjectID == other.Subject.ObjectID &&
		p.Subject.Relation == other.Subject.Relation
}

// Equals checks if two paths are fully equal (all fields match)
func (p Path) Equals(other Path) bool {
	// Check basic fields
	if p.Resource.ObjectType != other.Resource.ObjectType ||
		p.Resource.ObjectID != other.Resource.ObjectID ||
		p.Relation != other.Relation ||
		p.Subject.ObjectType != other.Subject.ObjectType ||
		p.Subject.ObjectID != other.Subject.ObjectID ||
		p.Subject.Relation != other.Subject.Relation {
		return false
	}

	// Check expiration
	if (p.Expiration == nil) != (other.Expiration == nil) {
		return false
	}
	if p.Expiration != nil && other.Expiration != nil && !p.Expiration.Equal(*other.Expiration) {
		return false
	}

	// Check caveat (basic comparison - could be more sophisticated)
	if (p.Caveat == nil) != (other.Caveat == nil) {
		return false
	}
	if p.Caveat != nil && other.Caveat != nil {
		// For now, just compare the string representation
		// A more sophisticated comparison would parse the caveat structure
		if p.Caveat.String() != other.Caveat.String() {
			return false
		}
	}

	// Check metadata maps
	if !maps.Equal(p.Metadata, other.Metadata) {
		return false
	}

	// Check integrity (basic comparison)
	if len(p.Integrity) != len(other.Integrity) {
		return false
	}
	for i, integrity := range p.Integrity {
		if integrity.String() != other.Integrity[i].String() {
			return false
		}
	}

	return true
}

// CollectAll is a helper function to build read a complete PathSeq and turn it into a fully realized slice of Paths.
func CollectAll(seq PathSeq) ([]Path, error) {
	out := make([]Path, 0)
	for x, err := range seq {
		if err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, nil
}
