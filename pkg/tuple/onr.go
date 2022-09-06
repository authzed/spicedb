package tuple

import (
	"fmt"
	"sort"

	"github.com/jzelinskie/stringz"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	// Format is the serialized form of the tuple
	formatWithRel            = "%s:%s#%s"
	formatImplicitSubjectRel = "%s:%s"
)

// ObjectAndRelation creates an ONR from string pieces.
func ObjectAndRelation(ns, oid, rel string) *core.ObjectAndRelation {
	return &core.ObjectAndRelation{
		Namespace: ns,
		ObjectId:  oid,
		Relation:  rel,
	}
}

// ParseSubjectONR converts a string representation of a Subject ONR to a proto object. Unlike
// ParseONR, this method allows for objects without relations. If an object without a relation
// is given, the relation will be set to ellipsis.
func ParseSubjectONR(subjectOnr string) *core.ObjectAndRelation {
	groups := subjectRegex.FindStringSubmatch(subjectOnr)

	if len(groups) == 0 {
		return nil
	}

	relation := Ellipsis
	subjectRelIndex := stringz.SliceIndex(subjectRegex.SubexpNames(), "subjectRel")
	if len(groups[subjectRelIndex]) > 0 {
		relation = groups[subjectRelIndex]
	}

	return &core.ObjectAndRelation{
		Namespace: groups[stringz.SliceIndex(subjectRegex.SubexpNames(), "subjectType")],
		ObjectId:  groups[stringz.SliceIndex(subjectRegex.SubexpNames(), "subjectID")],
		Relation:  relation,
	}
}

// ParseONR converts a string representation of an ONR to a proto object.
func ParseONR(onr string) *core.ObjectAndRelation {
	groups := onrRegex.FindStringSubmatch(onr)

	if len(groups) == 0 {
		return nil
	}

	return &core.ObjectAndRelation{
		Namespace: groups[stringz.SliceIndex(onrRegex.SubexpNames(), "resourceType")],
		ObjectId:  groups[stringz.SliceIndex(onrRegex.SubexpNames(), "resourceID")],
		Relation:  groups[stringz.SliceIndex(onrRegex.SubexpNames(), "resourceRel")],
	}
}

// StringRR converts a RR object to a string.
func StringRR(rr *core.RelationReference) string {
	if rr == nil {
		return ""
	}

	return fmt.Sprintf("%s#%s", rr.Namespace, rr.Relation)
}

// StringONR converts an ONR object to a string.
func StringONR(onr *core.ObjectAndRelation) string {
	if onr == nil {
		return ""
	}

	if onr.Relation == Ellipsis {
		return fmt.Sprintf(formatImplicitSubjectRel, onr.Namespace, onr.ObjectId)
	}

	return fmt.Sprintf(formatWithRel, onr.Namespace, onr.ObjectId, onr.Relation)
}

// StringsONRs converts ONR objects to a string slice, sorted.
func StringsONRs(onrs []*core.ObjectAndRelation) []string {
	onrstrings := make([]string, 0, len(onrs))
	for _, onr := range onrs {
		onrstrings = append(onrstrings, StringONR(onr))
	}

	sort.Strings(onrstrings)
	return onrstrings
}
