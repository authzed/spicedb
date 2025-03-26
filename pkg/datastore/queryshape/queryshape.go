package queryshape

// Shape represents the different ways a query can be shaped.
type Shape string

const (
	// Unspecified indicates that the shape is not specified.
	Unspecified Shape = "unspecified"

	// Varying indicates that the shape can vary. This is used
	// for queries whose shape is not known ahead of time.
	Varying = "varying"

	// CheckPermissionSelectDirectSubjects indicates that the query is a permission check
	// that selects direct subjects.
	//
	// The query shape selects a specific relationship based on filling in *all* of it
	// relationship fields (except the caveat name, context and expiration).
	CheckPermissionSelectDirectSubjects = "check-permission-select-direct-subjects"

	// CheckPermissionSelectIndirectSubjects indicates that the query is a permission check
	// that selects indirect subjects.
	//
	// The query shape selects a specific relationship based on filling in all of the fields
	// on the relationship except the subject relation, which is set to match any non-...
	// relations.
	CheckPermissionSelectIndirectSubjects = "check-permission-select-indirect-subjects"
)
