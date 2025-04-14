package queryshape

// Shape represents the different ways a query can be shaped.
type Shape string

// Symbol guide:
// *️⃣ - optional
// ✅ - required
// 🆔 - has some sort of filter
// 🅿️ - possibly specified

const (
	// Unspecified indicates that the shape is not specified.
	Unspecified Shape = "unspecified"

	// Varying indicates that the shape can vary. This is used
	// for queries whose shape is not known ahead of time.
	//
	// *️⃣ resource_type, *️⃣ resource_id, *️⃣ resource_relation, *️⃣ subject_type, *️⃣ subject_id, *️⃣ subject_relation, *️⃣ caveat, *️⃣ expiration
	Varying = "varying"

	// CheckPermissionSelectDirectSubjects indicates that the query is a permission check
	// that selects direct subjects.
	//
	// The query shape selects a specific relationship based on filling in *all* of it
	// relationship fields (except the caveat name, context and expiration).
	//
	// ✅ resource_type, ✅ resource_id, ✅ resource_relation, ✅ subject_type, ✅ subject_id, ✅ subject_relation, *️⃣ caveat, *️⃣ expiration
	CheckPermissionSelectDirectSubjects = "check-permission-select-direct-subjects"

	// CheckPermissionSelectIndirectSubjects indicates that the query is a permission check
	// that selects indirect subjects.
	//
	// The query shape selects a specific relationship based on filling in all fields
	// on the resource (except the caveat name, context and expiration) and the relation
	// name. The subject type nor ID is filled in and the optional subject relation is
	// set to match non-`...`.
	//
	// ✅ resource_type, ✅ resource_id, ✅ resource_relation, *️⃣ subject_type, *️⃣ subject_id, 🆔 subject_relation, *️⃣ caveat, *️⃣ expiration
	CheckPermissionSelectIndirectSubjects = "check-permission-select-indirect-subjects"

	// AllSubjectsForResources indicates that the query is selecting all subjects for a
	// given set of resources.
	//
	// The query shape selects all subjects for a given set of resources, which are fully
	// specified by providing the resource type, the resource ID(s) and the relation.
	//
	// ✅ resource_type, ✅ resource_id, ✅ resource_relation, *️⃣ subject_type, *️⃣ subject_id, *️⃣ subject_relation, *️⃣ caveat, *️⃣ expiration
	AllSubjectsForResources = "all-subjects-for-resources"

	// MatchingResourcesForSubject indicates that the query is selecting all resources that
	// match a given subject.
	//
	// The query shape selects all resources that match a given subject, which is specified
	// by providing the subject type, the subject ID and (optionally) the subject relation.
	// The resource type and relation are filled in, but the resource ID is never specified.
	//
	// ✅ resource_type, *️⃣ resource_id, ✅ resource_relation, ✅ subject_type, ✅ subject_id, 🅿️ subject_relation, *️⃣ caveat, *️⃣ expiration
	MatchingResourcesForSubject = "matching-resources-for-subject"

	// FindResourceOfType indicates that the query is selecting a resource of
	// a given type.
	//
	// The query shape selects a resource of a given type, which is specified by
	// providing the resource type. The other fields are never specified.
	//
	// ✅ resource_type, *️⃣ resource_id, *️⃣ resource_relation, *️⃣ subject_type, *️⃣ subject_id, *️⃣ subject_relation, *️⃣ caveat, *️⃣ expiration
	FindResourceOfType = "find-resource-of-type"

	// FindSubjectOfType indicates that the query is selecting a subject of
	// a given type.
	//
	// The query shape selects a subject of a given type, which is specified by
	// providing the subject type. The other fields are never specified.
	//
	// *️⃣ resource_type, *️⃣ resource_id, *️⃣ resource_relation, ✅ subject_type, *️⃣ subject_id, *️⃣ subject_relation, *️⃣ caveat, *️⃣ expiration
	FindSubjectOfType = "find-subject-of-type"

	// FindResourceOfTypeAndRelation indicates that the query is selecting a single
	// resource of a given type and relation.
	//
	// The query shape selects a resource of a given type and relation, which are
	// specified by providing the resource type and relation. The other fields are never
	// specified.
	//
	// ✅ resource_type, *️⃣ resource_id, ✅ resource_relation, *️⃣ subject_type, *️⃣ subject_id, *️⃣ subject_relation, *️⃣ caveat, *️⃣ expiration
	FindResourceOfTypeAndRelation = "find-resource-of-type-and-relation"

	// FindSubjectOfTypeAndRelation indicates that the query is selecting a single
	// subject of a given type and relation.
	//
	// The query shape selects a subject of a given type and relation, which are
	// specified by providing the subject type and relation. The other fields are never
	// specified.
	//
	// *️⃣ resource_type, *️⃣ resource_id, *️⃣ resource_relation, ✅ subject_type, *️⃣ subject_id, ✅ subject_relation, *️⃣ caveat, *️⃣ expiration
	FindSubjectOfTypeAndRelation = "find-subject-of-type-and-relation"

	// FindResourceRelationForSubjectRelation indicates that the query is selecting a single
	// relationship type that matches a given relation type, i.e. `user` or
	// `group#member with somecaveat and expiration`.
	//
	// The query shape selects an allowed subject type for a specific relation on a specific
	// resource type. All fields except resource ID are specified here, with subject ID only
	// specified if a wildcard.
	//
	// ✅ resource_type, *️⃣ resource_id, ✅ resource_relation, ✅ subject_type, 🅿️ subject_id, ✅ subject_relation, *️⃣ caveat, *️⃣ expiration
	FindResourceRelationForSubjectRelation = "find-resource-relation-for-subject-relation"
)
