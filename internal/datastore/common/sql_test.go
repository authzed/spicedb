package common

import (
	"testing"

	"github.com/authzed/spicedb/pkg/datastore/options"

	"github.com/authzed/spicedb/pkg/tuple"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

func TestSchemaQueryFilterer(t *testing.T) {
	tests := []struct {
		name                 string
		run                  func(filterer SchemaQueryFilterer) SchemaQueryFilterer
		expectedSQL          string
		expectedArgs         []any
		expectedColumnCounts map[string]int
	}{
		{
			"relation filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToRelation("somerelation")
			},
			"SELECT * WHERE relation = ?",
			[]any{"somerelation"},
			map[string]int{
				"relation": 1,
			},
		},
		{
			"resource ID filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceID("someresourceid")
			},
			"SELECT * WHERE object_id = ?",
			[]any{"someresourceid"},
			map[string]int{
				"object_id": 1,
			},
		},
		{
			"resource IDs filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterToResourceIDs([]string{"someresourceid", "anotherresourceid"})
			},
			"SELECT * WHERE object_id IN (?, ?)",
			[]any{"someresourceid", "anotherresourceid"},
			map[string]int{
				"object_id": 2,
			},
		},
		{
			"resource type filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			"SELECT * WHERE ns = ?",
			[]any{"sometype"},
			map[string]int{
				"ns": 1,
			},
		},
		{
			"resource filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj").FilterToRelation("somerel")
			},
			"SELECT * WHERE ns = ? AND object_id = ? AND relation = ?",
			[]any{"sometype", "someobj", "somerel"},
			map[string]int{
				"ns":        1,
				"object_id": 1,
				"relation":  1,
			},
		},
		{
			"relationships filter with no IDs or relations",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					ResourceType: "sometype",
				})
			},
			"SELECT * WHERE ns = ?",
			[]any{"sometype"},
			map[string]int{
				"ns": 1,
			},
		},
		{
			"relationships filter with single ID",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					ResourceType:        "sometype",
					OptionalResourceIds: []string{"someid"},
				})
			},
			"SELECT * WHERE ns = ? AND object_id IN (?)",
			[]any{"sometype", "someid"},
			map[string]int{
				"ns":        1,
				"object_id": 1,
			},
		},
		{
			"relationships filter with no IDs",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					ResourceType:        "sometype",
					OptionalResourceIds: []string{},
				})
			},
			"SELECT * WHERE ns = ?",
			[]any{"sometype"},
			map[string]int{
				"ns": 1,
			},
		},
		{
			"relationships filter with multiple IDs",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					ResourceType:        "sometype",
					OptionalResourceIds: []string{"someid", "anotherid"},
				})
			},
			"SELECT * WHERE ns = ? AND object_id IN (?, ?)",
			[]any{"sometype", "someid", "anotherid"},
			map[string]int{
				"ns":        1,
				"object_id": 2,
			},
		},
		{
			"subjects filter with no IDs or relations",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				})
			},
			"SELECT * WHERE ((subject_ns = ?))",
			[]any{"somesubjectype"},
			map[string]int{
				"subject_ns": 1,
			},
		},
		{
			"multiple subjects filters with just types",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				}, datastore.SubjectsSelector{
					OptionalSubjectType: "anothersubjectype",
				})
			},
			"SELECT * WHERE ((subject_ns = ?) OR (subject_ns = ?))",
			[]any{"somesubjectype", "anothersubjectype"},
			map[string]int{
				"subject_ns": 2,
			},
		},
		{
			"subjects filter with single ID",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"somesubjectid"},
				})
			},
			"SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?)))",
			[]any{"somesubjectype", "somesubjectid"},
			map[string]int{
				"subject_ns":        1,
				"subject_object_id": 1,
			},
		},
		{
			"subjects filter with single ID and no type",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectIds: []string{"somesubjectid"},
				})
			},
			"SELECT * WHERE ((subject_object_id IN (?)))",
			[]any{"somesubjectid"},
			map[string]int{
				"subject_object_id": 1,
			},
		},
		{
			"empty subjects filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{})
			},
			"SELECT * WHERE ((1=1))",
			nil,
			map[string]int{},
		},
		{
			"subjects filter with multiple IDs",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"somesubjectid", "anothersubjectid"},
				})
			},
			"SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?, ?)))",
			[]any{"somesubjectype", "somesubjectid", "anothersubjectid"},
			map[string]int{
				"subject_ns":        1,
				"subject_object_id": 2,
			},
		},
		{
			"subjects filter with single ellipsis relation",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
				})
			},
			"SELECT * WHERE ((subject_ns = ? AND subject_relation = ?))",
			[]any{"somesubjectype", "..."},
			map[string]int{
				"subject_ns":       1,
				"subject_relation": 1,
			},
		},
		{
			"subjects filter with single defined relation",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel"),
				})
			},
			"SELECT * WHERE ((subject_ns = ? AND subject_relation = ?))",
			[]any{"somesubjectype", "somesubrel"},
			map[string]int{
				"subject_ns":       1,
				"subject_relation": 1,
			},
		},
		{
			"subjects filter with only non-ellipsis",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					RelationFilter:      datastore.SubjectRelationFilter{}.WithOnlyNonEllipsisRelations(),
				})
			},
			"SELECT * WHERE ((subject_ns = ? AND subject_relation <> ?))",
			[]any{"somesubjectype", "..."},
			map[string]int{
				"subject_ns":       1,
				"subject_relation": 1,
			},
		},
		{
			"subjects filter with defined relation and ellipsis",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel").WithEllipsisRelation(),
				})
			},
			"SELECT * WHERE ((subject_ns = ? AND (subject_relation = ? OR subject_relation = ?)))",
			[]any{"somesubjectype", "...", "somesubrel"},
			map[string]int{
				"subject_ns":       1,
				"subject_relation": 2,
			},
		},
		{
			"subjects filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"somesubjectid", "anothersubjectid"},
					RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel").WithEllipsisRelation(),
				})
			},
			"SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?, ?) AND (subject_relation = ? OR subject_relation = ?)))",
			[]any{"somesubjectype", "somesubjectid", "anothersubjectid", "...", "somesubrel"},
			map[string]int{
				"subject_ns":        1,
				"subject_object_id": 2,
				"subject_relation":  2,
			},
		},
		{
			"multiple subjects filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(
					datastore.SubjectsSelector{
						OptionalSubjectType: "somesubjectype",
						OptionalSubjectIds:  []string{"a", "b"},
						RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel").WithEllipsisRelation(),
					},
					datastore.SubjectsSelector{
						OptionalSubjectType: "anothersubjecttype",
						OptionalSubjectIds:  []string{"b", "c"},
						RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("anotherrel").WithEllipsisRelation(),
					},
					datastore.SubjectsSelector{
						OptionalSubjectType: "thirdsubjectype",
						RelationFilter:      datastore.SubjectRelationFilter{}.WithOnlyNonEllipsisRelations(),
					},
				)
			},
			"SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?, ?) AND (subject_relation = ? OR subject_relation = ?)) OR (subject_ns = ? AND subject_object_id IN (?, ?) AND (subject_relation = ? OR subject_relation = ?)) OR (subject_ns = ? AND subject_relation <> ?))",
			[]any{"somesubjectype", "a", "b", "...", "somesubrel", "anothersubjecttype", "b", "c", "...", "anotherrel", "thirdsubjectype", "..."},
			map[string]int{
				"subject_ns":        3,
				"subject_object_id": 4,
				"subject_relation":  5,
			},
		},
		{
			"v1 subject filter with namespace",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType: "subns",
				})
			},
			"SELECT * WHERE subject_ns = ?",
			[]any{"subns"},
			map[string]int{
				"subject_ns": 1,
			},
		},
		{
			"v1 subject filter with subject id",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
				})
			},
			"SELECT * WHERE subject_ns = ? AND subject_object_id = ?",
			[]any{"subns", "subid"},
			map[string]int{
				"subject_ns":        1,
				"subject_object_id": 1,
			},
		},
		{
			"v1 subject filter with relation",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType: "subns",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "subrel",
					},
				})
			},
			"SELECT * WHERE subject_ns = ? AND subject_relation = ?",
			[]any{"subns", "subrel"},
			map[string]int{
				"subject_ns":       1,
				"subject_relation": 1,
			},
		},
		{
			"v1 subject filter with empty relation",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType: "subns",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "",
					},
				})
			},
			"SELECT * WHERE subject_ns = ? AND subject_relation = ?",
			[]any{"subns", "..."},
			map[string]int{
				"subject_ns":       1,
				"subject_relation": 1,
			},
		},
		{
			"v1 subject filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "somerel",
					},
				})
			},
			"SELECT * WHERE subject_ns = ? AND subject_object_id = ? AND subject_relation = ?",
			[]any{"subns", "subid", "somerel"},
			map[string]int{
				"subject_ns":        1,
				"subject_object_id": 1,
				"subject_relation":  1,
			},
		},
		{
			"limit",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.limit(100)
			},
			"SELECT * LIMIT 100",
			nil,
			map[string]int{},
		},
		{
			"full resources filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						ResourceType:             "someresourcetype",
						OptionalResourceIds:      []string{"someid", "anotherid"},
						OptionalResourceRelation: "somerelation",
						OptionalSubjectsSelectors: []datastore.SubjectsSelector{
							{
								OptionalSubjectType: "somesubjectype",
								OptionalSubjectIds:  []string{"somesubjectid", "anothersubjectid"},
								RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel").WithEllipsisRelation(),
							},
						},
					},
				)
			},
			"SELECT * WHERE ns = ? AND relation = ? AND object_id IN (?, ?) AND ((subject_ns = ? AND subject_object_id IN (?, ?) AND (subject_relation = ? OR subject_relation = ?)))",
			[]any{"someresourcetype", "somerelation", "someid", "anotherid", "somesubjectype", "somesubjectid", "anothersubjectid", "...", "somesubrel"},
			map[string]int{
				"ns":                1,
				"object_id":         2,
				"relation":          1,
				"subject_ns":        1,
				"subject_object_id": 2,
				"subject_relation":  2,
			},
		},
		{
			"order by",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						ResourceType: "someresourcetype",
					},
				).TupleOrder(options.ByResource)
			},
			"SELECT * WHERE ns = ? ORDER BY ns, object_id, relation, subject_ns, subject_object_id, subject_relation",
			[]any{"someresourcetype"},
			map[string]int{
				"ns": 1,
			},
		},
		{
			"after with just namespace",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						ResourceType: "someresourcetype",
					},
				).After(tuple.MustParse("someresourcetype:foo#viewer@user:bar"), options.ByResource)
			},
			"SELECT * WHERE ns = ? AND (object_id,relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?)",
			[]any{"someresourcetype", "foo", "viewer", "user", "bar", "..."},
			map[string]int{
				"ns": 1,
			},
		},
		{
			"after with namespace and single resource id",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						ResourceType:        "someresourcetype",
						OptionalResourceIds: []string{"one"},
					},
				).After(tuple.MustParse("someresourcetype:foo#viewer@user:bar"), options.ByResource)
			},
			"SELECT * WHERE ns = ? AND object_id IN (?) AND (relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?)",
			[]any{"someresourcetype", "one", "viewer", "user", "bar", "..."},
			map[string]int{
				"ns":        1,
				"object_id": 1,
			},
		},
		{
			"after with namespace and resource ids",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						ResourceType:        "someresourcetype",
						OptionalResourceIds: []string{"one", "two"},
					},
				).After(tuple.MustParse("someresourcetype:foo#viewer@user:bar"), options.ByResource)
			},
			"SELECT * WHERE ns = ? AND object_id IN (?, ?) AND (object_id,relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?)",
			[]any{"someresourcetype", "one", "two", "foo", "viewer", "user", "bar", "..."},
			map[string]int{
				"ns":        1,
				"object_id": 2,
			},
		},
		{
			"after with namespace and relation",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						ResourceType:             "someresourcetype",
						OptionalResourceRelation: "somerelation",
					},
				).After(tuple.MustParse("someresourcetype:foo#viewer@user:bar"), options.ByResource)
			},
			"SELECT * WHERE ns = ? AND relation = ? AND (object_id,subject_ns,subject_object_id,subject_relation) > (?,?,?,?)",
			[]any{"someresourcetype", "somerelation", "foo", "user", "bar", "..."},
			map[string]int{
				"ns":       1,
				"relation": 1,
			},
		},
		{
			"after with subject namespace",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				}).After(tuple.MustParse("someresourcetype:foo#viewer@user:bar"), options.ByResource)
			},
			"SELECT * WHERE ((subject_ns = ?)) AND (ns,object_id,relation,subject_object_id,subject_relation) > (?,?,?,?,?)",
			[]any{"somesubjectype", "someresourcetype", "foo", "viewer", "bar", "..."},
			map[string]int{
				"subject_ns": 1,
			},
		},
		{
			"after with subject namespaces",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				// NOTE: this isn't really valid (it'll return no results), but is a good test to ensure
				// the duplicate subject type results in the subject type being in the ORDER BY.
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				}).MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "anothersubjectype",
				}).After(tuple.MustParse("someresourcetype:foo#viewer@user:bar"), options.ByResource)
			},
			"SELECT * WHERE ((subject_ns = ?)) AND ((subject_ns = ?)) AND (ns,object_id,relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?,?)",
			[]any{"somesubjectype", "anothersubjectype", "someresourcetype", "foo", "viewer", "user", "bar", "..."},
			map[string]int{
				"subject_ns": 2,
			},
		},
		{
			"order by subject",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						ResourceType: "someresourcetype",
					},
				).TupleOrder(options.BySubject)
			},
			"SELECT * WHERE ns = ? ORDER BY subject_ns, subject_object_id, subject_relation, ns, object_id, relation",
			[]any{"someresourcetype"},
			map[string]int{
				"ns": 1,
			},
		},
		{
			"order by subject, after with subject namespace",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				}).After(tuple.MustParse("someresourcetype:foo#viewer@user:bar"), options.BySubject)
			},
			"SELECT * WHERE ((subject_ns = ?)) AND (subject_object_id,subject_relation,ns,object_id,relation) > (?,?,?,?,?)",
			[]any{"somesubjectype", "bar", "...", "someresourcetype", "foo", "viewer"},
			map[string]int{
				"subject_ns": 1,
			},
		},
		{
			"order by subject, after with subject namespace and subject object id",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"foo"},
				}).After(tuple.MustParse("someresourcetype:someresource#viewer@user:bar"), options.BySubject)
			},
			"SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?))) AND (subject_relation,ns,object_id,relation) > (?,?,?,?)",
			[]any{"somesubjectype", "foo", "...", "someresourcetype", "someresource", "viewer"},
			map[string]int{"subject_ns": 1, "subject_object_id": 1},
		},
		{
			"order by subject, after with subject namespace and multiple subject object IDs",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"foo", "bar"},
				}).After(tuple.MustParse("someresourcetype:someresource#viewer@user:next"), options.BySubject)
			},
			"SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?, ?))) AND (subject_object_id,subject_relation,ns,object_id,relation) > (?,?,?,?,?)",
			[]any{"somesubjectype", "foo", "bar", "next", "...", "someresourcetype", "someresource", "viewer"},
			map[string]int{"subject_ns": 1, "subject_object_id": 2},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			base := sq.Select("*")
			schema := NewSchemaInformation(
				"ns",
				"object_id",
				"relation",
				"subject_ns",
				"subject_object_id",
				"subject_relation",
				"caveat",
				TupleComparison,
			)
			filterer := NewSchemaQueryFilterer(schema, base)

			ran := test.run(filterer)
			require.Equal(t, test.expectedColumnCounts, ran.filteringColumnCounts)

			sql, args, err := ran.queryBuilder.ToSql()
			require.NoError(t, err)
			require.Equal(t, test.expectedSQL, sql)
			require.Equal(t, test.expectedArgs, args)
		})
	}
}
