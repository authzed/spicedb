package typesystem

import (
	"fmt"
	"strings"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ResourceCheckHintForRelation returns the resource portion of a check for a specific relation.
func ResourceCheckHintForRelation(resourceType string, resourceID string, relation string) string {
	return tuple.StringONR(&core.ObjectAndRelation{
		Namespace: resourceType,
		ObjectId:  resourceID,
		Relation:  relation,
	})
}

// ResourceCheckHintForArrow returns the resource portion of a check for a specific arrow (tupleset -> userset).
func ResourceCheckHintForArrow(resourceType string, resourceID string, tuplesetRelation string, computedUsersetRelation string) string {
	return ResourceCheckHintForRelation(resourceType, resourceID, tuplesetRelation) + "->" + computedUsersetRelation
}

// CheckHint returns a string representation of a check hint for a resource and subject. This is used in the CheckRequest to
// provide hints to the dispatcher about a subproblem whose result has already been computed, allowing the check to skip
// the evaluation of that subproblem.
func CheckHint(resourceHint string, subject *core.ObjectAndRelation) string {
	return resourceHint + "@" + tuple.StringONR(subject)
}

// CheckHintType is an enum for the type of check hint.
type CheckHintType int

const (
	CheckHintTypeUnknown CheckHintType = iota

	// CheckHintTypeRelation is a hint for a specific relation.
	CheckHintTypeRelation

	// CheckHintTypeArrow is a hint for a specific arrow (tupleset -> userset).
	CheckHintTypeArrow
)

// ParsedCheckHint is a parsed check hint.
type ParsedCheckHint struct {
	// Type is the type of check hint.
	Type CheckHintType

	// Resource is the resource portion of the check hint.
	Resource *core.ObjectAndRelation

	// Subject is the subject portion of the check hint.
	Subject *core.ObjectAndRelation

	// ArrowComputedUsersetRelation is the relation of the computed userset in an arrow hint. Only
	// valid if Type is CheckHintTypeArrow.
	ArrowComputedUsersetRelation string
}

// AsHintString returns the parsed check hint as a string.
func (pch ParsedCheckHint) AsHintString() string {
	if pch.Type == CheckHintTypeArrow {
		return CheckHint(ResourceCheckHintForArrow(pch.Resource.Namespace, pch.Resource.ObjectId, pch.Resource.Relation, pch.ArrowComputedUsersetRelation), pch.Subject)
	}

	return CheckHint(ResourceCheckHintForRelation(pch.Resource.Namespace, pch.Resource.ObjectId, pch.Resource.Relation), pch.Subject)
}

// ParseCheckHint parses a check hint string into a ParsedCheckHint or returns an error if the hint is invalid.
func ParseCheckHint(checkHint string) (*ParsedCheckHint, error) {
	// If the check hint contains an arrow, it is an arrow hint.
	if strings.Contains(checkHint, "->") {
		resourceAndSubject := strings.Split(checkHint, "@")
		if len(resourceAndSubject) != 2 {
			return nil, fmt.Errorf("invalid number of elements in check hint: %q", checkHint)
		}

		resourceAndArrow := resourceAndSubject[0]
		subject := resourceAndSubject[1]

		resourceAndArrowSplit := strings.Split(resourceAndArrow, "->")
		if len(resourceAndArrowSplit) != 2 {
			return nil, fmt.Errorf("invalid number of resources in hint: %q", checkHint)
		}

		resource := tuple.ParseONR(resourceAndArrowSplit[0])
		if resource == nil {
			return nil, fmt.Errorf("could not parse portion %q of check hint: %q", resourceAndArrowSplit[0], checkHint)
		}

		if err := resource.Validate(); err != nil {
			return nil, fmt.Errorf("invalid resource in check hint: %w", err)
		}

		parsedSubject := tuple.ParseSubjectONR(subject)
		if parsedSubject == nil {
			return nil, fmt.Errorf("could not parse portion %q of check hint: %q", subject, checkHint)
		}

		if err := parsedSubject.Validate(); err != nil {
			return nil, fmt.Errorf("invalid subject in check hint: %w", err)
		}

		return &ParsedCheckHint{
			Type:                         CheckHintTypeArrow,
			Resource:                     resource,
			Subject:                      parsedSubject,
			ArrowComputedUsersetRelation: resourceAndArrowSplit[1],
		}, nil
	}

	// Otherwise, it is a relation hint, represented as a single relationship string.
	parsed := tuple.Parse(checkHint)
	if parsed == nil {
		return nil, fmt.Errorf("could not parse check hint: %q", checkHint)
	}

	if err := parsed.Validate(); err != nil {
		return nil, fmt.Errorf("invalid check hint: %w", err)
	}

	return &ParsedCheckHint{
		Type:     CheckHintTypeRelation,
		Resource: parsed.ResourceAndRelation,
		Subject:  parsed.Subject,
	}, nil
}
