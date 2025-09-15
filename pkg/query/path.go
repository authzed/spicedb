package query

import (
	"fmt"
	"maps"
	"time"

	"github.com/authzed/spicedb/internal/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Path is an abstract notion of an individual relation. While tuple.Relation is what is stored under the hood,
type Path struct {
	Resource   Object
	Relation   string
	Subject    ObjectAndRelation
	Caveat     *core.CaveatExpression
	Expiration *time.Time
	Integrity  []*core.RelationshipIntegrity

	Metadata map[string]any
}

type pathMergeOp int

const (
	pathMergeOpOr pathMergeOp = iota
	pathMergeOpAnd
	pathMergeOpAndNot
)

func (p *Path) ResourceOAR() ObjectAndRelation {
	return p.Resource.WithRelation(p.Relation)
}

func (p *Path) MergeOr(other *Path) error {
	return p.mergeFrom(other, pathMergeOpOr)
}

func (p *Path) MergeAnd(other *Path) error {
	return p.mergeFrom(other, pathMergeOpAnd)
}

func (p *Path) MergeAndNot(other *Path) error {
	return p.mergeFrom(other, pathMergeOpAndNot)
}

func (p *Path) mergeFrom(other *Path, mergeOp pathMergeOp) error {
	// Check if they have the same Resource and Subject types and IDs
	if !p.Resource.Equals(other.Resource) {
		return fmt.Errorf("cannot merge paths with different resources: %v vs %v", p.Resource, other.Resource)
	}

	pSubject := GetObject(p.Subject)
	otherSubject := GetObject(other.Subject)
	if !pSubject.Equals(otherSubject) {
		return fmt.Errorf("cannot merge paths with different subjects: %v vs %v", pSubject, otherSubject)
	}

	// Clear Relation unless both have the same Relation string
	if p.Relation != other.Relation {
		p.Relation = ""
	}

	// Combine caveats based on merge operation
	switch mergeOp {
	case pathMergeOpOr:
		p.Caveat = caveats.Or(p.Caveat, other.Caveat)
	case pathMergeOpAnd:
		p.Caveat = caveats.And(p.Caveat, other.Caveat)
	case pathMergeOpAndNot:
		p.Caveat = caveats.Subtract(p.Caveat, other.Caveat)
	default:
		return fmt.Errorf("unknown merge operation: %d", mergeOp)
	}

	// Keep any Expiration, and if there are two of them, take the earlier one
	if other.Expiration != nil {
		if p.Expiration == nil || other.Expiration.Before(*p.Expiration) {
			p.Expiration = other.Expiration
		}
	}

	// Append all integrities together
	p.Integrity = append(p.Integrity, other.Integrity...)

	// Merge the metadata by overwriting fields from other into p.
	// WARNING: This is a simple overwrite strategy and may not be appropriate for all use cases.
	// Better is probably to have a more structured Metadata type, with a Merge() function.
	if other.Metadata != nil {
		if p.Metadata == nil {
			p.Metadata = make(map[string]any)
		}
		maps.Copy(p.Metadata, other.Metadata)
	}

	return nil
}

func (p *Path) IsExpired() bool {
	if p.Expiration == nil {
		return false
	}
	return time.Now().After(*p.Expiration)
}

// FromRelationship creates a new Path from a tuple.Relationship.
func FromRelationship(rel tuple.Relationship) *Path {
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

	return &Path{
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
func (p *Path) ToRelationship() (tuple.Relationship, error) {
	if p.Relation == "" {
		return tuple.Relationship{}, fmt.Errorf("cannot convert Path with empty Relation to Relationship")
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
			return tuple.Relationship{}, fmt.Errorf("cannot convert Path with complex caveat expression to Relationship")
		}
	}

	var integrity *core.RelationshipIntegrity
	if len(p.Integrity) > 0 {
		if len(p.Integrity) > 1 {
			return tuple.Relationship{}, fmt.Errorf("cannot convert Path with multiple integrity values to Relationship")
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
