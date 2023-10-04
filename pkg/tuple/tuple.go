package tuple

import (
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"slices"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jzelinskie/stringz"
	"google.golang.org/protobuf/types/known/structpb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	// Ellipsis is the Ellipsis relation in v0 style subjects.
	Ellipsis = "..."

	// PublicWildcard is the wildcard value for subject object IDs that indicates public access
	// for the subject type.
	PublicWildcard = "*"
)

const (
	namespaceNameExpr = "([a-z][a-z0-9_]{1,61}[a-z0-9]/)*[a-z][a-z0-9_]{1,62}[a-z0-9]"
	resourceIDExpr    = "([a-zA-Z0-9/_|\\-=+]{1,})"
	subjectIDExpr     = "([a-zA-Z0-9/_|\\-=+]{1,})|\\*"
	relationExpr      = "[a-z][a-z0-9_]{1,62}[a-z0-9]"
	caveatNameExpr    = "([a-z][a-z0-9_]{1,61}[a-z0-9]/)*[a-z][a-z0-9_]{1,62}[a-z0-9]"
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

var caveatExpr = fmt.Sprintf(`\[(?P<caveatName>(%s))(:(?P<caveatContext>(\{(.+)\})))?\]`, caveatNameExpr)

var (
	onrRegex        = regexp.MustCompile(fmt.Sprintf("^%s$", onrExpr))
	subjectRegex    = regexp.MustCompile(fmt.Sprintf("^%s$", subjectExpr))
	resourceIDRegex = regexp.MustCompile(fmt.Sprintf("^%s$", resourceIDExpr))
	subjectIDRegex  = regexp.MustCompile(fmt.Sprintf("^%s$", subjectIDExpr))
)

var parserRegex = regexp.MustCompile(
	fmt.Sprintf(
		`^%s@%s(%s)?$`,
		onrExpr,
		subjectExpr,
		caveatExpr,
	),
)

// ValidateResourceID ensures that the given resource ID is valid. Returns an error if not.
func ValidateResourceID(objectID string) error {
	if !resourceIDRegex.MatchString(objectID) {
		return fmt.Errorf("invalid resource id; must match %s", resourceIDExpr)
	}
	if len(objectID) > 1024 {
		return fmt.Errorf("invalid resource id; must be <= 1024 characters")
	}

	return nil
}

// ValidateSubjectID ensures that the given object ID (under a subject reference) is valid. Returns an error if not.
func ValidateSubjectID(subjectID string) error {
	if !subjectIDRegex.MatchString(subjectID) {
		return fmt.Errorf("invalid subject id; must be alphanumeric and between 1 and 127 characters or a star for public")
	}
	if len(subjectID) > 1024 {
		return fmt.Errorf("invalid resource id; must be <= 1024 characters")
	}

	return nil
}

// MustString converts a tuple to a string. If the tuple is nil or empty, returns empty string.
func MustString(tpl *core.RelationTuple) string {
	tplString, err := String(tpl)
	if err != nil {
		panic(err)
	}
	return tplString
}

// String converts a tuple to a string. If the tuple is nil or empty, returns empty string.
func String(tpl *core.RelationTuple) (string, error) {
	if tpl == nil || tpl.ResourceAndRelation == nil || tpl.Subject == nil {
		return "", nil
	}

	caveatString, err := StringCaveat(tpl.Caveat)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s@%s%s", StringONR(tpl.ResourceAndRelation), StringONR(tpl.Subject), caveatString), nil
}

// StringWithoutCaveat converts a tuple to a string, without its caveat included.
func StringWithoutCaveat(tpl *core.RelationTuple) string {
	if tpl == nil || tpl.ResourceAndRelation == nil || tpl.Subject == nil {
		return ""
	}

	return fmt.Sprintf("%s@%s", StringONR(tpl.ResourceAndRelation), StringONR(tpl.Subject))
}

func MustStringCaveat(caveat *core.ContextualizedCaveat) string {
	caveatString, err := StringCaveat(caveat)
	if err != nil {
		panic(err)
	}
	return caveatString
}

// StringCaveat converts a contextualized caveat to a string. If the caveat is nil or empty, returns empty string.
func StringCaveat(caveat *core.ContextualizedCaveat) (string, error) {
	if caveat == nil || caveat.CaveatName == "" {
		return "", nil
	}

	contextString, err := StringCaveatContext(caveat.Context)
	if err != nil {
		return "", err
	}

	if len(contextString) > 0 {
		contextString = ":" + contextString
	}

	return fmt.Sprintf("[%s%s]", caveat.CaveatName, contextString), nil
}

// StringCaveatContext converts the context of a caveat to a string. If the context is nil or empty, returns an empty string.
func StringCaveatContext(context *structpb.Struct) (string, error) {
	if context == nil || len(context.Fields) == 0 {
		return "", nil
	}

	contextBytes, err := context.MarshalJSON()
	if err != nil {
		return "", err
	}
	return string(contextBytes), nil
}

// MustRelString converts a relationship into a string.  Will panic if
// the Relationship does not validate.
func MustRelString(rel *v1.Relationship) string {
	if err := rel.Validate(); err != nil {
		panic(fmt.Sprintf("invalid relationship: %#v %s", rel, err))
	}
	return MustStringRelationship(rel)
}

// MustParse wraps Parse such that any failures panic rather than returning
// nil.
func MustParse(tpl string) *core.RelationTuple {
	if parsed := Parse(tpl); parsed != nil {
		return parsed
	}
	panic("failed to parse tuple")
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
	subjectRelIndex := slices.Index(parserRegex.SubexpNames(), "subjectRel")
	if len(groups[subjectRelIndex]) > 0 {
		subjectRelation = groups[subjectRelIndex]
	}

	caveatName := groups[slices.Index(parserRegex.SubexpNames(), "caveatName")]
	var optionalCaveat *core.ContextualizedCaveat
	if caveatName != "" {
		optionalCaveat = &core.ContextualizedCaveat{
			CaveatName: caveatName,
		}

		caveatContextString := groups[slices.Index(parserRegex.SubexpNames(), "caveatContext")]
		if len(caveatContextString) > 0 {
			contextMap := make(map[string]any, 1)
			err := json.Unmarshal([]byte(caveatContextString), &contextMap)
			if err != nil {
				return nil
			}

			caveatContext, err := structpb.NewStruct(contextMap)
			if err != nil {
				return nil
			}

			optionalCaveat.Context = caveatContext
		}
	}

	resourceID := groups[slices.Index(parserRegex.SubexpNames(), "resourceID")]
	if err := ValidateResourceID(resourceID); err != nil {
		return nil
	}

	subjectID := groups[slices.Index(parserRegex.SubexpNames(), "subjectID")]
	if err := ValidateSubjectID(subjectID); err != nil {
		return nil
	}

	return &core.RelationTuple{
		ResourceAndRelation: &core.ObjectAndRelation{
			Namespace: groups[slices.Index(parserRegex.SubexpNames(), "resourceType")],
			ObjectId:  resourceID,
			Relation:  groups[slices.Index(parserRegex.SubexpNames(), "resourceRel")],
		},
		Subject: &core.ObjectAndRelation{
			Namespace: groups[slices.Index(parserRegex.SubexpNames(), "subjectType")],
			ObjectId:  subjectID,
			Relation:  subjectRelation,
		},
		Caveat: optionalCaveat,
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

func Equal(lhs, rhs *core.RelationTuple) bool {
	// TODO(jschorr): Use a faster method then string comparison for caveats.
	return OnrEqual(lhs.ResourceAndRelation, rhs.ResourceAndRelation) && OnrEqual(lhs.Subject, rhs.Subject) && MustStringCaveat(lhs.Caveat) == MustStringCaveat(rhs.Caveat)
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
	var caveat *v1.ContextualizedCaveat
	if tpl.Caveat != nil {
		caveat = &v1.ContextualizedCaveat{
			CaveatName: tpl.Caveat.CaveatName,
			Context:    tpl.Caveat.Context,
		}
	}
	return &v1.Relationship{
		Resource: &v1.ObjectReference{
			ObjectType: tpl.ResourceAndRelation.Namespace,
			ObjectId:   tpl.ResourceAndRelation.ObjectId,
		},
		Relation: tpl.ResourceAndRelation.Relation,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: tpl.Subject.Namespace,
				ObjectId:   tpl.Subject.ObjectId,
			},
			OptionalRelation: stringz.Default(tpl.Subject.Relation, "", Ellipsis),
		},
		OptionalCaveat: caveat,
	}
}

// NewRelationship creates a new Relationship value with all its required child structures allocated
func NewRelationship() *v1.Relationship {
	return &v1.Relationship{
		Resource: &v1.ObjectReference{},
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{},
		},
	}
}

// MustToRelationshipMutating sets target relationship to all the values provided in the source tuple.
func MustToRelationshipMutating(source *core.RelationTuple, targetRel *v1.Relationship, targetCaveat *v1.ContextualizedCaveat) {
	targetRel.Resource.ObjectType = source.ResourceAndRelation.Namespace
	targetRel.Resource.ObjectId = source.ResourceAndRelation.ObjectId
	targetRel.Relation = source.ResourceAndRelation.Relation
	targetRel.Subject.Object.ObjectType = source.Subject.Namespace
	targetRel.Subject.Object.ObjectId = source.Subject.ObjectId
	targetRel.Subject.OptionalRelation = stringz.Default(source.Subject.Relation, "", Ellipsis)
	targetRel.OptionalCaveat = nil

	if source.Caveat != nil {
		if targetCaveat == nil {
			panic("expected a provided target caveat")
		}
		targetCaveat.CaveatName = source.Caveat.CaveatName
		targetCaveat.Context = source.Caveat.Context
		targetRel.OptionalCaveat = targetCaveat
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
		ResourceType:          tpl.ResourceAndRelation.Namespace,
		OptionalResourceId:    tpl.ResourceAndRelation.ObjectId,
		OptionalRelation:      tpl.ResourceAndRelation.Relation,
		OptionalSubjectFilter: UsersetToSubjectFilter(tpl.Subject),
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

func UpdateFromRelationshipUpdates(updates []*v1.RelationshipUpdate) []*core.RelationTupleUpdate {
	relationshipUpdates := make([]*core.RelationTupleUpdate, 0, len(updates))

	for _, update := range updates {
		relationshipUpdates = append(relationshipUpdates, UpdateFromRelationshipUpdate(update))
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
func MustFromRelationship[R objectReference, S subjectReference[R], C caveat](r relationship[R, S, C]) *core.RelationTuple {
	if err := r.Validate(); err != nil {
		panic(fmt.Sprintf("invalid relationship: %#v %s", r, err))
	}
	return FromRelationship(r)
}

// MustFromRelationships converts a slice of Relationship's into a slice of RelationTuple's.
func MustFromRelationships[R objectReference, S subjectReference[R], C caveat](rels []relationship[R, S, C]) []*core.RelationTuple {
	tuples := make([]*core.RelationTuple, 0, len(rels))
	for _, rel := range rels {
		tpl := MustFromRelationship(rel)
		tuples = append(tuples, tpl)
	}
	return tuples
}

// FromRelationship converts a Relationship into a RelationTuple.
func FromRelationship[T objectReference, S subjectReference[T], C caveat](r relationship[T, S, C]) *core.RelationTuple {
	rel := &core.RelationTuple{
		ResourceAndRelation: &core.ObjectAndRelation{},
		Subject:             &core.ObjectAndRelation{},
		Caveat:              &core.ContextualizedCaveat{},
	}

	CopyRelationshipToRelationTuple(r, rel)

	return rel
}

func CopyRelationshipToRelationTuple[T objectReference, S subjectReference[T], C caveat](r relationship[T, S, C], dst *core.RelationTuple) {
	if !reflect.ValueOf(r.GetOptionalCaveat()).IsZero() {
		dst.Caveat.CaveatName = r.GetOptionalCaveat().GetCaveatName()
		dst.Caveat.Context = r.GetOptionalCaveat().GetContext()
	} else {
		dst.Caveat = nil
	}

	dst.ResourceAndRelation.Namespace = r.GetResource().GetObjectType()
	dst.ResourceAndRelation.ObjectId = r.GetResource().GetObjectId()
	dst.ResourceAndRelation.Relation = r.GetRelation()
	dst.Subject.Namespace = r.GetSubject().GetObject().GetObjectType()
	dst.Subject.ObjectId = r.GetSubject().GetObject().GetObjectId()
	dst.Subject.Relation = stringz.DefaultEmpty(r.GetSubject().GetOptionalRelation(), Ellipsis)
}

// CopyRelationTupleToRelationship copies a source core.RelationTuple to a
// destination v1.Relationship without allocating new memory. It requires that
// the structure for the destination be pre-allocated for the fixed parts, and
// an optional caveat context be provided for use when the source contains a
// caveat.
func CopyRelationTupleToRelationship(
	src *core.RelationTuple,
	dst *v1.Relationship,
	dstCaveat *v1.ContextualizedCaveat,
) {
	dst.Resource.ObjectType = src.ResourceAndRelation.Namespace
	dst.Resource.ObjectId = src.ResourceAndRelation.ObjectId
	dst.Relation = src.ResourceAndRelation.Relation
	dst.Subject.Object.ObjectType = src.Subject.Namespace
	dst.Subject.Object.ObjectId = src.Subject.ObjectId
	dst.Subject.OptionalRelation = stringz.Default(src.Subject.Relation, "", Ellipsis)

	if src.Caveat != nil {
		dst.OptionalCaveat = dstCaveat
		dst.OptionalCaveat.CaveatName = src.Caveat.CaveatName
		dst.OptionalCaveat.Context = src.Caveat.Context
	} else {
		dst.OptionalCaveat = nil
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
		Tuple:     FromRelationship[*v1.ObjectReference, *v1.SubjectReference, *v1.ContextualizedCaveat](update.Relationship),
	}
}

// MustWithCaveat adds the given caveat name to the tuple. This is for testing only.
func MustWithCaveat(tpl *core.RelationTuple, caveatName string, contexts ...map[string]any) *core.RelationTuple {
	wc, err := WithCaveat(tpl, caveatName, contexts...)
	if err != nil {
		panic(err)
	}
	return wc
}

// WithCaveat adds the given caveat name to the tuple. This is for testing only.
func WithCaveat(tpl *core.RelationTuple, caveatName string, contexts ...map[string]any) (*core.RelationTuple, error) {
	var context *structpb.Struct

	if len(contexts) > 0 {
		combined := map[string]any{}
		for _, current := range contexts {
			maps.Copy(combined, current)
		}

		contextStruct, err := structpb.NewStruct(combined)
		if err != nil {
			return nil, err
		}
		context = contextStruct
	}

	tpl = tpl.CloneVT()
	tpl.Caveat = &core.ContextualizedCaveat{
		CaveatName: caveatName,
		Context:    context,
	}
	return tpl, nil
}
