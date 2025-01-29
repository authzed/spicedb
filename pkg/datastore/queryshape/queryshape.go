package queryshape

// Shape represents the different ways a query can be shaped.
type Shape int

const (
	// Unspecified indicates that the shape is not specified.
	Unspecified Shape = iota

	// Varying indicates that the shape can vary. This is used
	// for queries whose shape is not known ahead of time.
	Varying

	// CheckPermissionSelectDirectSubjects indicates that the query is a permission check
	// that selects direct subjects.
	CheckPermissionSelectDirectSubjects

	// CheckPermissionSelectIndirectSubjects indicates that the query is a permission check
	// that selects indirect subjects.
	CheckPermissionSelectIndirectSubjects
)
