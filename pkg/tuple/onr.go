package tuple

import (
	"fmt"
	"sort"
	"strings"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jzelinskie/stringz"
)

const (
	// Format is the serialized form of the tuple
	formatWithRel            = "%s:%s#%s"
	formatImplicitSubjectRel = "%s:%s"
)

// ObjectAndRelation creates an ONR from string pieces.
func ObjectAndRelation(ns, oid, rel string) *v0.ObjectAndRelation {
	return &v0.ObjectAndRelation{
		Namespace: ns,
		ObjectId:  oid,
		Relation:  rel,
	}
}

// User creates a user wrapping a userset ONR.
func User(userset *v0.ObjectAndRelation) *v0.User {
	return &v0.User{UserOneof: &v0.User_Userset{Userset: userset}}
}

// ParseSubjectONR converts a string representation of a Subject ONR to a proto object. Unlike
// ParseONR, this method allows for objects without relations. If an object without a relation
// is given, the relation will be set to ellipsis.
func ParseSubjectONR(subjectOnr string) *v0.ObjectAndRelation {
	groups := subjectRegex.FindStringSubmatch(subjectOnr)

	if len(groups) == 0 {
		return nil
	}

	relation := "..."
	subjectRelIndex := stringz.SliceIndex(subjectRegex.SubexpNames(), "subjectRel")
	if len(groups[subjectRelIndex]) > 0 {
		relation = groups[subjectRelIndex]
	}

	return &v0.ObjectAndRelation{
		Namespace: groups[stringz.SliceIndex(subjectRegex.SubexpNames(), "subjectType")],
		ObjectId:  groups[stringz.SliceIndex(subjectRegex.SubexpNames(), "subjectID")],
		Relation:  relation,
	}
}

// ParseONR converts a string representation of an ONR to a proto object.
func ParseONR(onr string) *v0.ObjectAndRelation {
	groups := onrRegex.FindStringSubmatch(onr)

	if len(groups) == 0 {
		return nil
	}

	return &v0.ObjectAndRelation{
		Namespace: groups[stringz.SliceIndex(onrRegex.SubexpNames(), "resourceType")],
		ObjectId:  groups[stringz.SliceIndex(onrRegex.SubexpNames(), "resourceID")],
		Relation:  groups[stringz.SliceIndex(onrRegex.SubexpNames(), "resourceRel")],
	}
}

// StringONR converts an ONR object to a string.
func StringONR(onr *v0.ObjectAndRelation) string {
	if onr == nil {
		return ""
	}

	if onr.Relation == ellipsis {
		return fmt.Sprintf(formatImplicitSubjectRel, onr.Namespace, onr.ObjectId)
	}

	return fmt.Sprintf(formatWithRel, onr.Namespace, onr.ObjectId, onr.Relation)
}

// StringsONRs converts ONR objects to a string slice, sorted.
func StringsONRs(onrs []*v0.ObjectAndRelation) []string {
	var onrstrings []string
	for _, onr := range onrs {
		onrstrings = append(onrstrings, StringONR(onr))
	}

	sort.Strings(onrstrings)
	return onrstrings
}

// StringObjectRef marshals a *v1.ObjectReference into a string.
func StringObjectRef(ref *v1.ObjectReference) string {
	return ref.ObjectType + ":" + ref.ObjectId
}

// StringSubjectRef marshals a *v1.SubjectReference into a string.
func StringSubjectRef(ref *v1.SubjectReference) string {
	var b strings.Builder
	b.WriteString(ref.Object.ObjectType + ":" + ref.Object.ObjectId)
	if ref.OptionalRelation != "" {
		b.WriteString("#" + ref.OptionalRelation)
	}
	return b.String()
}
