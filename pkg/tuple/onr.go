package tuple

import (
	"slices"
	"sort"
	"strings"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// ObjectAndRelation creates an ONR from string pieces.
func ObjectAndRelation(ns, oid, rel string) *core.ObjectAndRelation {
	return &core.ObjectAndRelation{
		Namespace: ns,
		ObjectId:  oid,
		Relation:  rel,
	}
}

// RelationReference creates a RelationReference from the string pieces.
func RelationReference(namespaceName string, relationName string) *core.RelationReference {
	return &core.RelationReference{
		Namespace: namespaceName,
		Relation:  relationName,
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
	subjectRelIndex := slices.Index(subjectRegex.SubexpNames(), "subjectRel")
	if len(groups[subjectRelIndex]) > 0 {
		relation = groups[subjectRelIndex]
	}

	return &core.ObjectAndRelation{
		Namespace: groups[slices.Index(subjectRegex.SubexpNames(), "subjectType")],
		ObjectId:  groups[slices.Index(subjectRegex.SubexpNames(), "subjectID")],
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
		Namespace: groups[slices.Index(onrRegex.SubexpNames(), "resourceType")],
		ObjectId:  groups[slices.Index(onrRegex.SubexpNames(), "resourceID")],
		Relation:  groups[slices.Index(onrRegex.SubexpNames(), "resourceRel")],
	}
}

// JoinRelRef joins the namespace and relation together into the same
// format as `StringRR()`.
func JoinRelRef(namespace, relation string) string { return namespace + "#" + relation }

// MustSplitRelRef splits a string produced by `JoinRelRef()` and panics if
// it fails.
func MustSplitRelRef(relRef string) (namespace, relation string) {
	var ok bool
	namespace, relation, ok = strings.Cut(relRef, "#")
	if !ok {
		panic("improperly formatted relation reference")
	}
	return
}

// StringRR converts a RR object to a string.
func StringRR(rr *core.RelationReference) string {
	if rr == nil {
		return ""
	}

	return JoinRelRef(rr.Namespace, rr.Relation)
}

// StringONR converts an ONR object to a string.
func StringONR(onr *core.ObjectAndRelation) string {
	if onr == nil {
		return ""
	}
	if onr.Relation == Ellipsis {
		return JoinObjectRef(onr.Namespace, onr.ObjectId)
	}
	return JoinRelRef(JoinObjectRef(onr.Namespace, onr.ObjectId), onr.Relation)
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

func OnrEqual(lhs, rhs *core.ObjectAndRelation) bool {
	// Properties are sorted by highest to lowest cardinality to optimize for short-circuiting.
	return lhs.ObjectId == rhs.ObjectId && lhs.Relation == rhs.Relation && lhs.Namespace == rhs.Namespace
}

func OnrEqualOrWildcard(tpl, target *core.ObjectAndRelation) bool {
	return OnrEqual(tpl, target) || (tpl.ObjectId == PublicWildcard && tpl.Namespace == target.Namespace)
}
