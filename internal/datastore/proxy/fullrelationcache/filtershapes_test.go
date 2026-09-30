package fullrelationcache

import (
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// FilterShapeRels are the relationships for the filter shape tests.
// The document:foo#viewer set has every subject shape. user:old is expired.
// The bar and baz objects test resource ID filtering and multi-ID queries.
var FilterShapeRels = []string{
	"document:foo#viewer@user:evan",
	"document:foo#viewer@user:tanner",
	"document:foo#viewer@user:*",
	"document:foo#viewer@group:eng#member",
	"document:foo#viewer@group:sales#...",
	"document:foo#viewer@user:cav[somecaveat]",
	"document:foo#viewer@user:exp[expiration:2999-01-01T00:00:00Z]",
	"document:foo#viewer@user:old[expiration:2000-01-01T00:00:00Z]",
	"document:foo#editor@user:evan",
	"document:bar#viewer@user:evan",
	"document:bar#viewer@group:eng#member",
	"document:baz#viewer@user:*",
}

// FilterShapeCase is one filter shape on document:foo#viewer.
// Want lists the subjects that the query returns, sorted.
type FilterShapeCase struct {
	Name             string
	Selectors        []datastore.SubjectsSelector
	CaveatFilter     datastore.CaveatNameFilter
	ExpirationOption datastore.ExpirationFilterOption
	Want             []string
}

// Filter returns the filter of the case for resourceIDs of document#viewer.
func (c FilterShapeCase) Filter(resourceIDs ...string) datastore.RelationshipsFilter {
	return datastore.RelationshipsFilter{
		OptionalResourceType:      "document",
		OptionalResourceIds:       resourceIDs,
		OptionalResourceRelation:  "viewer",
		OptionalSubjectsSelectors: c.Selectors,
		OptionalCaveatNameFilter:  c.CaveatFilter,
		OptionalExpirationOption:  c.ExpirationOption,
	}
}

// FilterShapeCases returns the filter shapes for serveFromSet and the proxy.
func FilterShapeCases() []FilterShapeCase {
	ellipsis := datastore.SubjectRelationFilter{}.WithEllipsisRelation()
	member := datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("member")
	users := datastore.SubjectsSelector{OptionalSubjectType: "user"}
	allLive := []string{"group:eng#member", "group:sales", "user:*", "user:cav", "user:evan", "user:exp", "user:tanner"}

	return []FilterShapeCase{
		{Name: "no subject selector", Want: allLive},
		{
			Name:      "subject type only",
			Selectors: []datastore.SubjectsSelector{users},
			Want:      []string{"user:*", "user:cav", "user:evan", "user:exp", "user:tanner"},
		},
		{
			Name: "subject type and IDs",
			Selectors: []datastore.SubjectsSelector{{
				OptionalSubjectType: "user", OptionalSubjectIds: []string{"evan", "tanner"}, RelationFilter: ellipsis,
			}},
			Want: []string{"user:evan", "user:tanner"},
		},
		{
			Name:      "subject IDs without a type",
			Selectors: []datastore.SubjectsSelector{{OptionalSubjectIds: []string{"evan", "eng"}}},
			Want:      []string{"group:eng#member", "user:evan"},
		},
		{
			Name: "wildcard",
			Selectors: []datastore.SubjectsSelector{{
				OptionalSubjectType: "user", OptionalSubjectIds: []string{tuple.PublicWildcard}, RelationFilter: ellipsis,
			}},
			Want: []string{"user:*"},
		},
		{
			Name: "a concrete ID does not match the wildcard",
			Selectors: []datastore.SubjectsSelector{{
				OptionalSubjectType: "user", OptionalSubjectIds: []string{"nobody"}, RelationFilter: ellipsis,
			}},
		},
		{
			Name:      "ellipsis relation",
			Selectors: []datastore.SubjectsSelector{{RelationFilter: ellipsis}},
			Want:      []string{"group:sales", "user:*", "user:cav", "user:evan", "user:exp", "user:tanner"},
		},
		{
			Name:      "non-ellipsis relation",
			Selectors: []datastore.SubjectsSelector{{OptionalSubjectType: "group", RelationFilter: member}},
			Want:      []string{"group:eng#member"},
		},
		{
			Name: "ellipsis or non-ellipsis relation",
			Selectors: []datastore.SubjectsSelector{{
				OptionalSubjectType: "group", RelationFilter: ellipsis.WithNonEllipsisRelation("member"),
			}},
			Want: []string{"group:eng#member", "group:sales"},
		},
		{
			Name: "only non-ellipsis relations",
			Selectors: []datastore.SubjectsSelector{{
				RelationFilter: datastore.SubjectRelationFilter{}.WithOnlyNonEllipsisRelations(),
			}},
			Want: []string{"group:eng#member"},
		},
		{
			Name: "multiple selectors",
			Selectors: []datastore.SubjectsSelector{
				{OptionalSubjectType: "user", OptionalSubjectIds: []string{"evan"}, RelationFilter: ellipsis},
				{OptionalSubjectType: "group", RelationFilter: member},
			},
			Want: []string{"group:eng#member", "user:evan"},
		},
		{
			Name:         "caveat name",
			CaveatFilter: datastore.WithCaveatName("somecaveat"),
			Want:         []string{"user:cav"},
		},
		{
			Name:         "caveat name with no match",
			CaveatFilter: datastore.WithCaveatName("othercaveat"),
		},
		{
			Name:         "no caveat",
			CaveatFilter: datastore.WithNoCaveat(),
			Want:         []string{"group:eng#member", "group:sales", "user:*", "user:evan", "user:exp", "user:tanner"},
		},
		{
			Name:         "caveat name with a selector",
			Selectors:    []datastore.SubjectsSelector{users},
			CaveatFilter: datastore.WithCaveatName("somecaveat"),
			Want:         []string{"user:cav"},
		},
		{
			Name:             "has expiration",
			ExpirationOption: datastore.ExpirationFilterOptionHasExpiration,
			Want:             []string{"user:exp"},
		},
		{
			Name:             "no expiration",
			ExpirationOption: datastore.ExpirationFilterOptionNoExpiration,
			Want:             []string{"group:eng#member", "group:sales", "user:*", "user:cav", "user:evan", "user:tanner"},
		},
		{
			Name:             "has expiration with a selector",
			Selectors:        []datastore.SubjectsSelector{users},
			ExpirationOption: datastore.ExpirationFilterOptionHasExpiration,
			Want:             []string{"user:exp"},
		},
		{
			Name:             "no expiration with a selector",
			Selectors:        []datastore.SubjectsSelector{users},
			ExpirationOption: datastore.ExpirationFilterOptionNoExpiration,
			Want:             []string{"user:*", "user:cav", "user:evan", "user:tanner"},
		},
		{
			Name:             "no caveat and has expiration",
			CaveatFilter:     datastore.WithNoCaveat(),
			ExpirationOption: datastore.ExpirationFilterOptionHasExpiration,
			Want:             []string{"user:exp"},
		},
	}
}
