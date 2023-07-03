package consistencytestutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"

	"github.com/authzed/spicedb/internal/developmentmembership"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/graph/computed"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ObjectAndPermission contains an object ID and whether it is a caveated result.
type ObjectAndPermission struct {
	ObjectID   string
	IsCaveated bool
}

type Accessibility int

const (
	// NotAccessible indicates that the subject is not accessible for the resource+permission.
	NotAccessible Accessibility = 0

	// NotAccessibleDueToPrespecifiedCaveat indicates that the subject is not accessible for the
	// resource+permission due to a caveat whose context is fully prespecified on the relationship.
	NotAccessibleDueToPrespecifiedCaveat Accessibility = 1

	// AccessibleDirectly indicates that the subject is directly accessible for the resource+permission,
	// rather than via a wildcard.
	AccessibleDirectly Accessibility = 2

	// AccessibleViaWildcardOnly indicates that the subject is only granted permission by virtue
	// of a wildcard being present, i.e. the subject is not directly found for a relation used by
	// the permission.
	AccessibleViaWildcardOnly Accessibility = 3

	// AccessibleBecauseTheSame indicates that the resource+permission and subject are exactly
	// the same.
	AccessibleBecauseTheSame Accessibility = 4
)

// AccessibilitySet is a helper for tracking the accessibility, permissions, resources
// and subjects found for consistency testing.
type AccessibilitySet struct {
	// ResourcesByNamespace is a multimap of all defined resources, by resource namespace.
	ResourcesByNamespace *mapz.MultiMap[string, *core.ObjectAndRelation]

	// SubjectsByNamespace is a multimap of all defined subjects, by subject namespace.
	SubjectsByNamespace *mapz.MultiMap[string, *core.ObjectAndRelation]

	// RelationshipsByResourceNamespace is a multimap of all defined relationships, by resource namespace.
	RelationshipsByResourceNamespace *mapz.MultiMap[string, *core.RelationTuple]

	// UncomputedPermissionshipByRelationship is a map from a relationship string of the form
	// "resourceType:resourceObjectID#permission@subjectType:subjectObjectID" to its
	// associated *uncomputed* (i.e. caveats not processed) permissionship state.
	UncomputedPermissionshipByRelationship map[string]dispatchv1.ResourceCheckResult_Membership

	// PermissionshipByRelationship is a map from a relationship string of the form
	// "resourceType:resourceObjectID#permission@subjectType:subjectObjectID" to its
	// associated computed (i.e. caveats processed) permissionship state.
	PermissionshipByRelationship map[string]dispatchv1.ResourceCheckResult_Membership

	// AccessibilityByRelationship is a map from a relationship string of the form
	// "resourceType:resourceObjectID#permission@subjectType:subjectObjectID" to its
	// associated computed accessibility state.
	AccessibilityByRelationship map[string]Accessibility
}

// BuildAccessibilitySet builds and returns an accessibility set for the given consistency
// cluster and data. Note that this function does *a lot* of checks, and should not be used
// outside of testing.
func BuildAccessibilitySet(t *testing.T, ccd ConsistencyClusterAndData) *AccessibilitySet {
	// Compute all relationships and objects by namespace.
	relsByResourceNamespace := mapz.NewMultiMap[string, *core.RelationTuple]()
	resourcesByNamespace := mapz.NewMultiMap[string, *core.ObjectAndRelation]()
	subjectsByNamespace := mapz.NewMultiMap[string, *core.ObjectAndRelation]()
	allObjectIds := mapz.NewSet[string]()

	for _, tpl := range ccd.Populated.Tuples {
		relsByResourceNamespace.Add(tpl.ResourceAndRelation.Namespace, tpl)
		resourcesByNamespace.Add(tpl.ResourceAndRelation.Namespace, tpl.ResourceAndRelation)
		subjectsByNamespace.Add(tpl.Subject.Namespace, tpl.Subject)
		allObjectIds.Add(tpl.ResourceAndRelation.ObjectId)

		if tpl.Subject.ObjectId != tuple.PublicWildcard {
			allObjectIds.Add(tpl.Subject.ObjectId)
		}
	}

	// Run a *dispatched* check for each {resource+permission, defined subject} pair and
	// record the results. Note that we use a dispatched check to ensure that we
	// find caveated subjects. We then run a fully caveated-processed check on the
	// caveated results to see if they are static.
	//
	// NOTE: We only conduct checks here for the *defined* subjects from the relationships,
	// rather than every possible subject, as the latter would make the consistency test suite
	// VERY slow, due to the combinatorial size of all possible subjects.
	headRevision, err := ccd.DataStore.HeadRevision(ccd.Ctx)
	require.NoError(t, err)

	dispatcher := graph.NewLocalOnlyDispatcher(defaultConcurrencyLimit)
	permissionshipByRelationship := map[string]dispatchv1.ResourceCheckResult_Membership{}
	uncomputedPermissionshipByRelationship := map[string]dispatchv1.ResourceCheckResult_Membership{}
	accessibilityByRelationship := map[string]Accessibility{}

	for _, resourceType := range ccd.Populated.NamespaceDefinitions {
		for _, possibleResourceID := range allObjectIds.AsSlice() {
			for _, relationOrPermission := range resourceType.Relation {
				for _, subject := range subjectsByNamespace.Values() {
					if subject.ObjectId == tuple.PublicWildcard {
						continue
					}

					resourceRelation := &core.RelationReference{
						Namespace: resourceType.Name,
						Relation:  relationOrPermission.Name,
					}

					results, err := dispatcher.DispatchCheck(ccd.Ctx, &dispatchv1.DispatchCheckRequest{
						ResourceRelation: resourceRelation,
						ResourceIds:      []string{possibleResourceID},
						Subject:          subject,
						ResultsSetting:   dispatchv1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
						Metadata: &dispatchv1.ResolverMeta{
							AtRevision:     headRevision.String(),
							DepthRemaining: 50,
						},
					})
					require.NoError(t, err)

					resourceAndRelation := &core.ObjectAndRelation{
						Namespace: resourceType.Name,
						ObjectId:  possibleResourceID,
						Relation:  relationOrPermission.Name,
					}
					permString := tuple.MustString(&core.RelationTuple{
						ResourceAndRelation: resourceAndRelation,
						Subject:             subject,
					})

					if result, ok := results.ResultsByResourceId[possibleResourceID]; ok {
						membership := result.Membership
						uncomputedPermissionshipByRelationship[permString] = membership

						// If the subject is caveated, run a computed check to determine if it
						// statically has permission (or not). This can happen if the caveat context
						// is fully specified on the relationship.
						if membership == dispatchv1.ResourceCheckResult_CAVEATED_MEMBER {
							cr, _, err := computed.ComputeCheck(ccd.Ctx, dispatcher,
								computed.CheckParameters{
									ResourceType:  resourceRelation,
									Subject:       subject,
									CaveatContext: nil,
									AtRevision:    headRevision,
									MaximumDepth:  50,
								},
								possibleResourceID,
							)
							require.NoError(t, err)
							membership = cr.Membership
						}

						permissionshipByRelationship[permString] = membership

						switch membership {
						case dispatchv1.ResourceCheckResult_NOT_MEMBER:
							accessibilityByRelationship[permString] = NotAccessibleDueToPrespecifiedCaveat

						case dispatchv1.ResourceCheckResult_CAVEATED_MEMBER:
							fallthrough

						case dispatchv1.ResourceCheckResult_MEMBER:
							if resourceAndRelation.EqualVT(subject) {
								accessibilityByRelationship[permString] = AccessibleBecauseTheSame
							} else {
								if isAccessibleViaWildcardOnly(t, ccd, dispatcher, headRevision, resourceAndRelation, subject) {
									accessibilityByRelationship[permString] = AccessibleViaWildcardOnly
								} else {
									accessibilityByRelationship[permString] = AccessibleDirectly
								}
							}

						default:
							panic("unknown membership result")
						}
					} else {
						uncomputedPermissionshipByRelationship[permString] = dispatchv1.ResourceCheckResult_NOT_MEMBER
						permissionshipByRelationship[permString] = dispatchv1.ResourceCheckResult_NOT_MEMBER
						accessibilityByRelationship[permString] = NotAccessible
					}
				}
			}
		}
	}

	return &AccessibilitySet{
		RelationshipsByResourceNamespace:       relsByResourceNamespace,
		ResourcesByNamespace:                   resourcesByNamespace,
		SubjectsByNamespace:                    subjectsByNamespace,
		PermissionshipByRelationship:           permissionshipByRelationship,
		UncomputedPermissionshipByRelationship: uncomputedPermissionshipByRelationship,
		AccessibilityByRelationship:            accessibilityByRelationship,
	}
}

// UncomputedPermissionshipFor returns the uncomputed permissionship for the given
// resource+permission and subject. If not found, returns false.
func (as *AccessibilitySet) UncomputedPermissionshipFor(resourceAndRelation *core.ObjectAndRelation, subject *core.ObjectAndRelation) (dispatchv1.ResourceCheckResult_Membership, bool) {
	relString := tuple.MustString(&core.RelationTuple{
		ResourceAndRelation: resourceAndRelation,
		Subject:             subject,
	})
	permissionship, ok := as.UncomputedPermissionshipByRelationship[relString]
	return permissionship, ok
}

// AccessibiliyAndPermissionshipFor returns the computed accessibility and permissionship for the
// given resource+permission and subject. If not found, returns false.
func (as *AccessibilitySet) AccessibiliyAndPermissionshipFor(resourceAndRelation *core.ObjectAndRelation, subject *core.ObjectAndRelation) (Accessibility, dispatchv1.ResourceCheckResult_Membership, bool) {
	relString := tuple.MustString(&core.RelationTuple{
		ResourceAndRelation: resourceAndRelation,
		Subject:             subject,
	})
	accessibility, ok := as.AccessibilityByRelationship[relString]
	if !ok {
		return NotAccessible, dispatchv1.ResourceCheckResult_UNKNOWN, false
	}

	permissionship := as.PermissionshipByRelationship[relString]
	return accessibility, permissionship, ok
}

// DirectlyAccessibleDefinedSubjects returns all subjects that have direct access/permission on the
// resource+permission. Direct access is defined as not being granted access via a wildcard.
func (as *AccessibilitySet) DirectlyAccessibleDefinedSubjects(resourceAndRelation *core.ObjectAndRelation) []*core.ObjectAndRelation {
	found := make([]*core.ObjectAndRelation, 0)
	for relString, accessibility := range as.AccessibilityByRelationship {
		if accessibility != AccessibleDirectly {
			continue
		}

		parsed := tuple.MustParse(relString)
		if !parsed.ResourceAndRelation.EqualVT(resourceAndRelation) {
			continue
		}

		found = append(found, parsed.Subject)
	}
	return found
}

// DirectlyAccessibleDefinedSubjectsOfType returns all subjects that have direct access/permission on the
// resource+permission and match the given subject type.
// Direct access is defined as not being granted access via a wildcard.
func (as *AccessibilitySet) DirectlyAccessibleDefinedSubjectsOfType(resourceAndRelation *core.ObjectAndRelation, subjectType *core.RelationReference) map[string]ObjectAndPermission {
	found := map[string]ObjectAndPermission{}
	for relString, accessibility := range as.AccessibilityByRelationship {
		// NOTE: we also ignore subjects granted access by being themselves.
		if accessibility != AccessibleDirectly && accessibility != AccessibleBecauseTheSame {
			continue
		}

		parsed := tuple.MustParse(relString)
		if !parsed.ResourceAndRelation.EqualVT(resourceAndRelation) {
			continue
		}

		if parsed.Subject.Namespace != subjectType.Namespace || parsed.Subject.Relation != subjectType.Relation {
			continue
		}

		permissionship := as.PermissionshipByRelationship[relString]

		found[parsed.Subject.ObjectId] = ObjectAndPermission{
			ObjectID:   parsed.Subject.ObjectId,
			IsCaveated: permissionship == dispatchv1.ResourceCheckResult_CAVEATED_MEMBER,
		}
	}
	return found
}

// SubjectTypes returns all *defined* subject types found.
func (as *AccessibilitySet) SubjectTypes() []*core.RelationReference {
	subjectTypes := map[string]*core.RelationReference{}
	for _, subject := range as.SubjectsByNamespace.Values() {
		rr := &core.RelationReference{
			Namespace: subject.Namespace,
			Relation:  subject.Relation,
		}
		subjectTypes[tuple.StringRR(rr)] = rr
	}
	return maps.Values(subjectTypes)
}

// AllSubjectsNoWildcards returns all *defined*, non-wildcard subjects found.
func (as *AccessibilitySet) AllSubjectsNoWildcards() []*core.ObjectAndRelation {
	subjects := make([]*core.ObjectAndRelation, 0)
	seenSubjects := mapz.NewSet[string]()
	for _, subject := range as.SubjectsByNamespace.Values() {
		if subject.ObjectId == tuple.PublicWildcard {
			continue
		}
		if seenSubjects.Add(tuple.StringONR(subject)) {
			subjects = append(subjects, subject)
		}
	}
	return subjects
}

// LookupAccessibleResources returns all resources of the given type that are accessible to the
// given subject.
func (as *AccessibilitySet) LookupAccessibleResources(resourceType *core.RelationReference, subject *core.ObjectAndRelation) map[string]ObjectAndPermission {
	foundResources := map[string]ObjectAndPermission{}
	for permString, permissionship := range as.PermissionshipByRelationship {
		if permissionship == dispatchv1.ResourceCheckResult_NOT_MEMBER {
			continue
		}

		parsed := tuple.MustParse(permString)
		if parsed.ResourceAndRelation.Namespace != resourceType.Namespace ||
			parsed.ResourceAndRelation.Relation != resourceType.Relation {
			continue
		}

		if parsed.Subject.Namespace != subject.Namespace ||
			parsed.Subject.ObjectId != subject.ObjectId ||
			parsed.Subject.Relation != subject.Relation {
			continue
		}

		foundResources[parsed.ResourceAndRelation.ObjectId] = ObjectAndPermission{
			ObjectID:   parsed.ResourceAndRelation.ObjectId,
			IsCaveated: permissionship == dispatchv1.ResourceCheckResult_CAVEATED_MEMBER,
		}
	}

	return foundResources
}

func isAccessibleViaWildcardOnly(
	t *testing.T,
	ccd ConsistencyClusterAndData,
	dispatcher dispatch.Dispatcher,
	revision datastore.Revision,
	resourceAndPermission *core.ObjectAndRelation,
	subject *core.ObjectAndRelation,
) bool {
	resp, err := dispatcher.DispatchExpand(ccd.Ctx, &dispatchv1.DispatchExpandRequest{
		ResourceAndRelation: resourceAndPermission,
		Metadata: &dispatchv1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 100,
		},
		ExpansionMode: dispatchv1.DispatchExpandRequest_RECURSIVE,
	})
	require.NoError(t, err)

	subjectsFound, err := developmentmembership.AccessibleExpansionSubjects(resp.TreeNode)
	require.NoError(t, err)
	return !subjectsFound.Contains(subject)
}
