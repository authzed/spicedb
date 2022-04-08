package tuple

import (
	"fmt"
	"regexp"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jzelinskie/stringz"
)

const (
	// Ellipsis is the Ellipsis relation in v0 style subjects.
	Ellipsis = "..."

	// PublicWildcard is the wildcard value for subject object IDs that indicates public access
	// for the subject type.
	PublicWildcard = "*"
)

const (
	namespaceNameExpr = "([a-z][a-z0-9_]{1,61}[a-z0-9]/)?[a-z][a-z0-9_]{1,62}[a-z0-9]"
	resourceIDExpr    = "[a-zA-Z0-9_][a-zA-Z0-9/_|-]{0,127}"
	subjectIDExpr     = "([a-zA-Z0-9_][a-zA-Z0-9/_|-]{0,127})|\\*"
	relationExpr      = "[a-z][a-z0-9_]{1,62}[a-z0-9]"
)

var onrExpr = fmt.Sprintf(
	`(?P<resourceType>(%s)):(?P<resourceID>%s)#(?P<resourceRel>%s)`,
	namespaceNameExpr,
	resourceIDExpr,
	relationExpr,
)

var subjectExpr = fmt.Sprintf(
	`(?P<subjectType>(%s)):(?P<subjectID>%s)(#(?P<subjectRel>%s|\.\.\.))?`,
	namespaceNameExpr,
	subjectIDExpr,
	relationExpr,
)

var (
	onrRegex        = regexp.MustCompile(fmt.Sprintf("^%s$", onrExpr))
	subjectRegex    = regexp.MustCompile(fmt.Sprintf("^%s$", subjectExpr))
	resourceIDRegex = regexp.MustCompile(fmt.Sprintf("^%s$", resourceIDExpr))
	subjectIDRegex  = regexp.MustCompile(fmt.Sprintf("^%s$", subjectIDExpr))
)

var parserRegex = regexp.MustCompile(
	fmt.Sprintf(
		`^%s@%s$`,
		onrExpr,
		subjectExpr,
	),
)

// ValidateResourceID ensures that the given resource ID is valid. Returns an error if not.
func ValidateResourceID(objectID string) error {
	if !resourceIDRegex.MatchString(objectID) {
		return fmt.Errorf("invalid resource id; must be alphanumeric and between 1 and 127 characters")
	}

	return nil
}

// ValidateSubjectID ensures that the given object ID (under a subject reference) is valid. Returns an error if not.
func ValidateSubjectID(subjectID string) error {
	if !subjectIDRegex.MatchString(subjectID) {
		return fmt.Errorf("invalid subject id; must be alphanumeric and between 1 and 127 characters or a star for public")
	}

	return nil
}

// String converts a tuple to a string. If the tuple is nil or empty, returns empty string.
func String(tpl *core.RelationTuple) string {
	if tpl == nil || tpl.ObjectAndRelation == nil || tpl.User == nil || tpl.User.GetUserset() == nil {
		return ""
	}

	return fmt.Sprintf("%s@%s", StringONR(tpl.ObjectAndRelation), StringONR(tpl.User.GetUserset()))
}

// MustRelString converts a relationship into a string.  Will panic if
// the Relationship does not validate.
func MustRelString(tpl *v1.Relationship) string {
	return String(MustFromRelationship(tpl))
}

// MustParse wraps Parse such that any failures panic rather than returning
// nil.
func MustParse(tpl string) *core.RelationTuple {
	if parsed := Parse(tpl); parsed != nil {
		return parsed
	}
	panic("failed to parse tuple")
}

// RelString converts a relationship into a string.
func RelString(tpl *v1.Relationship) string {
	return String(FromRelationship(tpl))
}

// Parse unmarshals the string form of a Tuple and returns nil if there is a
// failure.
//
// This function treats both missing and Ellipsis relations equally.
func Parse(tpl string) *core.RelationTuple {
	groups := parserRegex.FindStringSubmatch(tpl)
	if len(groups) == 0 {
		return nil
	}

	subjectRelation := Ellipsis
	subjectRelIndex := stringz.SliceIndex(parserRegex.SubexpNames(), "subjectRel")
	if len(groups[subjectRelIndex]) > 0 {
		subjectRelation = groups[subjectRelIndex]
	}

	return &core.RelationTuple{
		ObjectAndRelation: &core.ObjectAndRelation{
			Namespace: groups[stringz.SliceIndex(parserRegex.SubexpNames(), "resourceType")],
			ObjectId:  groups[stringz.SliceIndex(parserRegex.SubexpNames(), "resourceID")],
			Relation:  groups[stringz.SliceIndex(parserRegex.SubexpNames(), "resourceRel")],
		},
		User: &core.User{UserOneof: &core.User_Userset{Userset: &core.ObjectAndRelation{
			Namespace: groups[stringz.SliceIndex(parserRegex.SubexpNames(), "subjectType")],
			ObjectId:  groups[stringz.SliceIndex(parserRegex.SubexpNames(), "subjectID")],
			Relation:  subjectRelation,
		}}},
	}
}

func ParseRel(rel string) *v1.Relationship {
	tpl := Parse(rel)
	if tpl == nil {
		return nil
	}
	return ToRelationship(tpl)
}

func Create(tpl *core.RelationTuple) *core.RelationTupleUpdate {
	return &core.RelationTupleUpdate{
		Operation: core.RelationTupleUpdate_CREATE,
		Tuple:     tpl,
	}
}

func Touch(tpl *core.RelationTuple) *core.RelationTupleUpdate {
	return &core.RelationTupleUpdate{
		Operation: core.RelationTupleUpdate_TOUCH,
		Tuple:     tpl,
	}
}

func Delete(tpl *core.RelationTuple) *core.RelationTupleUpdate {
	return &core.RelationTupleUpdate{
		Operation: core.RelationTupleUpdate_DELETE,
		Tuple:     tpl,
	}
}

// MustToRelationship converts a RelationTuple into a Relationship. Will panic if
// the RelationTuple does not validate.
func MustToRelationship(tpl *core.RelationTuple) *v1.Relationship {
	if err := tpl.Validate(); err != nil {
		panic(fmt.Sprintf("invalid tuple: %#v %s", tpl, err))
	}

	return ToRelationship(tpl)
}

// ToRelationship converts a RelationTuple into a Relationship.
func ToRelationship(tpl *core.RelationTuple) *v1.Relationship {
	return &v1.Relationship{
		Resource: &v1.ObjectReference{
			ObjectType: tpl.ObjectAndRelation.Namespace,
			ObjectId:   tpl.ObjectAndRelation.ObjectId,
		},
		Relation: tpl.ObjectAndRelation.Relation,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: tpl.User.GetUserset().Namespace,
				ObjectId:   tpl.User.GetUserset().ObjectId,
			},
			OptionalRelation: stringz.Default(tpl.User.GetUserset().Relation, "", Ellipsis),
		},
	}
}

// MustToFilter converts a RelationTuple into a RelationshipFilter. Will panic if
// the RelationTuple does not validate.
func MustToFilter(tpl *core.RelationTuple) *v1.RelationshipFilter {
	if err := tpl.Validate(); err != nil {
		panic(fmt.Sprintf("invalid tuple: %#v %s", tpl, err))
	}

	return ToFilter(tpl)
}

// ToFilter converts a RelationTuple into a RelationshipFilter.
func ToFilter(tpl *core.RelationTuple) *v1.RelationshipFilter {
	return &v1.RelationshipFilter{
		ResourceType:          tpl.ObjectAndRelation.Namespace,
		OptionalResourceId:    tpl.ObjectAndRelation.ObjectId,
		OptionalRelation:      tpl.ObjectAndRelation.Relation,
		OptionalSubjectFilter: UsersetToSubjectFilter(tpl.User.GetUserset()),
	}
}

// UsersetToSubjectFilter converts a userset to the equivalent exact SubjectFilter.
func UsersetToSubjectFilter(userset *core.ObjectAndRelation) *v1.SubjectFilter {
	return &v1.SubjectFilter{
		SubjectType:       userset.Namespace,
		OptionalSubjectId: userset.ObjectId,
		OptionalRelation: &v1.SubjectFilter_RelationFilter{
			Relation: stringz.Default(userset.Relation, "", Ellipsis),
		},
	}
}

// MustRelToFilter converts a Relationship into a RelationshipFilter. Will panic if
// the Relationship does not validate.
func MustRelToFilter(rel *v1.Relationship) *v1.RelationshipFilter {
	if err := rel.Validate(); err != nil {
		panic(fmt.Sprintf("invalid tuple: %#v %s", rel, err))
	}

	return RelToFilter(rel)
}

// RelToFilter converts a Relationship into a RelationshipFilter.
func RelToFilter(rel *v1.Relationship) *v1.RelationshipFilter {
	return &v1.RelationshipFilter{
		ResourceType:       rel.Resource.ObjectType,
		OptionalResourceId: rel.Resource.ObjectId,
		OptionalRelation:   rel.Relation,
		OptionalSubjectFilter: &v1.SubjectFilter{
			SubjectType:       rel.Subject.Object.ObjectType,
			OptionalSubjectId: rel.Subject.Object.ObjectId,
			OptionalRelation: &v1.SubjectFilter_RelationFilter{
				Relation: rel.Subject.OptionalRelation,
			},
		},
	}
}

// UpdatesToRelationshipUpdates converts a slice of RelationTupleUpdate into a
// slice of RelationshipUpdate.
func UpdatesToRelationshipUpdates(updates []*core.RelationTupleUpdate) []*v1.RelationshipUpdate {
	relationshipUpdates := make([]*v1.RelationshipUpdate, 0, len(updates))

	for _, update := range updates {
		relationshipUpdates = append(relationshipUpdates, UpdateToRelationshipUpdate(update))
	}

	return relationshipUpdates
}

// UpdateToRelationshipUpdate converts a RelationTupleUpdate into a
// RelationshipUpdate.
func UpdateToRelationshipUpdate(update *core.RelationTupleUpdate) *v1.RelationshipUpdate {
	var op v1.RelationshipUpdate_Operation
	switch update.Operation {
	case core.RelationTupleUpdate_CREATE:
		op = v1.RelationshipUpdate_OPERATION_CREATE
	case core.RelationTupleUpdate_DELETE:
		op = v1.RelationshipUpdate_OPERATION_DELETE
	case core.RelationTupleUpdate_TOUCH:
		op = v1.RelationshipUpdate_OPERATION_TOUCH
	default:
		panic("unknown tuple mutation")
	}

	return &v1.RelationshipUpdate{
		Operation:    op,
		Relationship: ToRelationship(update.Tuple),
	}
}

// MustFromRelationship converts a Relationship into a RelationTuple.
func MustFromRelationship(r *v1.Relationship) *core.RelationTuple {
	if err := r.Validate(); err != nil {
		panic(fmt.Sprintf("invalid relationship: %#v %s", r, err))
	}
	return FromRelationship(r)
}

// FromRelationship converts a Relationship into a RelationTuple.
func FromRelationship(r *v1.Relationship) *core.RelationTuple {
	return &core.RelationTuple{
		ObjectAndRelation: &core.ObjectAndRelation{
			Namespace: r.Resource.ObjectType,
			ObjectId:  r.Resource.ObjectId,
			Relation:  r.Relation,
		},
		User: &core.User{UserOneof: &core.User_Userset{Userset: &core.ObjectAndRelation{
			Namespace: r.Subject.Object.ObjectType,
			ObjectId:  r.Subject.Object.ObjectId,
			Relation:  stringz.DefaultEmpty(r.Subject.OptionalRelation, Ellipsis),
		}}},
	}
}

// UpdateFromRelationshipUpdate converts a RelationshipUpdate into a
// RelationTupleUpdate.
func UpdateFromRelationshipUpdate(update *v1.RelationshipUpdate) *core.RelationTupleUpdate {
	var op core.RelationTupleUpdate_Operation
	switch update.Operation {
	case v1.RelationshipUpdate_OPERATION_CREATE:
		op = core.RelationTupleUpdate_CREATE
	case v1.RelationshipUpdate_OPERATION_DELETE:
		op = core.RelationTupleUpdate_DELETE
	case v1.RelationshipUpdate_OPERATION_TOUCH:
		op = core.RelationTupleUpdate_TOUCH
	default:
		panic("unknown tuple mutation")
	}

	return &core.RelationTupleUpdate{
		Operation: op,
		Tuple:     FromRelationship(update.Relationship),
	}
}
