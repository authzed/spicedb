package tuple

import (
	"fmt"
	"regexp"
	"slices"
)

var (
	onrRegex     = regexp.MustCompile(fmt.Sprintf("^%s$", onrExpr))
	subjectRegex = regexp.MustCompile(fmt.Sprintf("^%s$", subjectExpr))
)

var onrSubjectRelIndex = slices.Index(subjectRegex.SubexpNames(), "subjectRel")
var onrSubjectTypeIndex = slices.Index(subjectRegex.SubexpNames(), "subjectType")
var onrSubjectIDIndex = slices.Index(subjectRegex.SubexpNames(), "subjectID")
var onrResourceTypeIndex = slices.Index(onrRegex.SubexpNames(), "resourceType")
var onrResourceIDIndex = slices.Index(onrRegex.SubexpNames(), "resourceID")
var onrResourceRelIndex = slices.Index(onrRegex.SubexpNames(), "resourceRel")

// ParseSubjectONR converts a string representation of a Subject ONR to an ObjectAndRelation. Unlike
// ParseONR, this method allows for objects without relations. If an object without a relation
// is given, the relation will be set to ellipsis.
func ParseSubjectONR(subjectOnr string) (ObjectAndRelation, error) {
	groups := subjectRegex.FindStringSubmatch(subjectOnr)
	if len(groups) == 0 {
		return ObjectAndRelation{}, fmt.Errorf("invalid subject ONR: %s", subjectOnr)
	}

	relation := Ellipsis
	if len(groups[onrSubjectRelIndex]) > 0 {
		relation = groups[onrSubjectRelIndex]
	}

	return ObjectAndRelation{
		ObjectType: groups[onrSubjectTypeIndex],
		ObjectID:   groups[onrSubjectIDIndex],
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
		ObjectType: groups[onrResourceTypeIndex],
		ObjectID:   groups[onrResourceIDIndex],
		Relation:   groups[onrResourceRelIndex],
	}, nil
}

func MustParseONR(onr string) ObjectAndRelation {
	parsed, err := ParseONR(onr)
	if err != nil {
		panic(err)
	}
	return parsed
}
