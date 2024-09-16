package tuple

import (
	"fmt"
	"slices"
)

// ParseSubjectONR converts a string representation of a Subject ONR to an ObjectAndRelation. Unlike
// ParseONR, this method allows for objects without relations. If an object without a relation
// is given, the relation will be set to ellipsis.
func ParseSubjectONR(subjectOnr string) (ObjectAndRelation, error) {
	groups := subjectRegex.FindStringSubmatch(subjectOnr)

	if len(groups) == 0 {
		return ObjectAndRelation{}, fmt.Errorf("invalid subject ONR: %s", subjectOnr)
	}

	relation := Ellipsis
	subjectRelIndex := slices.Index(subjectRegex.SubexpNames(), "subjectRel")
	if len(groups[subjectRelIndex]) > 0 {
		relation = groups[subjectRelIndex]
	}

	return ObjectAndRelation{
		ObjectType: groups[slices.Index(subjectRegex.SubexpNames(), "subjectType")],
		ObjectID:   groups[slices.Index(subjectRegex.SubexpNames(), "subjectID")],
		Relation:   relation,
	}, nil
}

// ParseONR converts a string representation of an ONR to an ObjectAndRelation object.
func ParseONR(onr string) (ObjectAndRelation, error) {
	groups := onrRegex.FindStringSubmatch(onr)

	if len(groups) == 0 {
		return ObjectAndRelation{}, fmt.Errorf("invalid ONR: %s", onr)
	}

	return ObjectAndRelation{
		ObjectType: groups[slices.Index(onrRegex.SubexpNames(), "resourceType")],
		ObjectID:   groups[slices.Index(onrRegex.SubexpNames(), "resourceID")],
		Relation:   groups[slices.Index(onrRegex.SubexpNames(), "resourceRel")],
	}, nil
}

func MustParseONR(onr string) ObjectAndRelation {
	parsed, err := ParseONR(onr)
	if err != nil {
		panic(err)
	}
	return parsed
}
