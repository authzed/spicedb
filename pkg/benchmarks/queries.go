package benchmarks

// CheckQuery is a positive check query — the subject is expected to have the permission.
type CheckQuery struct {
	ResourceType    string
	ResourceID      string
	Permission      string
	SubjectType     string
	SubjectID       string
	SubjectRelation string
}

// IterResourcesQuery is an IterResources query with the full expected result set.
type IterResourcesQuery struct {
	SubjectType         string
	SubjectID           string
	SubjectRelation     string
	Permission          string
	FilterResourceType  string
	ExpectedResourceIDs []string
}

// IterSubjectsQuery is an IterSubjects query with the full expected result set.
type IterSubjectsQuery struct {
	ResourceType       string
	ResourceID         string
	Permission         string
	FilterSubjectType  string
	ExpectedSubjectIDs []string
}

// QuerySets holds the sets of valid queries returned by a benchmark's Setup.
type QuerySets struct {
	Checks        []CheckQuery
	IterResources []IterResourcesQuery
	IterSubjects  []IterSubjectsQuery

	// MaxRecursionDepth, if > 0, should be passed to the query context.
	// Zero means use the default.
	MaxRecursionDepth int
}
