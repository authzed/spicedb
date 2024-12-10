package common

import (
	"context"
	"testing"

	"github.com/authzed/spicedb/pkg/datastore/options"

	"github.com/authzed/spicedb/pkg/tuple"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

var toCursor = options.ToCursor

func TestSchemaQueryFilterer(t *testing.T) {
	tests := []struct {
		name                  string
		run                   func(filterer SchemaQueryFilterer) SchemaQueryFilterer
		expectedSQL           string
		expectedArgs          []any
		expectedStaticColumns []string
	}{
		{
			"relation filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToRelation("somerelation")
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND relation = ?",
			[]any{"somerelation"},
			[]string{"relation"},
		},
		{
			"resource ID filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceID("someresourceid")
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND object_id = ?",
			[]any{"someresourceid"},
			[]string{"object_id"},
		},
		{
			"resource IDs filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithResourceIDPrefix("someprefix")
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND object_id LIKE ?",
			[]any{"someprefix%"},
			[]string{},
		},
		{
			"resource IDs prefix filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterToResourceIDs([]string{"someresourceid", "anotherresourceid"})
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND object_id IN (?,?)",
			[]any{"someresourceid", "anotherresourceid"},
			[]string{},
		},
		{
			"resource type filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ?",
			[]any{"sometype"},
			[]string{"ns"},
		},
		{
			"resource filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj").FilterToRelation("somerel")
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ? AND object_id = ? AND relation = ?",
			[]any{"sometype", "someobj", "somerel"},
			[]string{"ns", "object_id", "relation"},
		},
		{
			"relationships filter with no IDs or relations",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					OptionalResourceType: "sometype",
				})
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ?",
			[]any{"sometype"},
			[]string{"ns"},
		},
		{
			"relationships filter with single ID",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					OptionalResourceType: "sometype",
					OptionalResourceIds:  []string{"someid"},
				})
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ? AND object_id IN (?)",
			[]any{"sometype", "someid"},
			[]string{"ns", "object_id"},
		},
		{
			"relationships filter with no IDs",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					OptionalResourceType: "sometype",
					OptionalResourceIds:  []string{},
				})
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ?",
			[]any{"sometype"},
			[]string{"ns"},
		},
		{
			"relationships filter with multiple IDs",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					OptionalResourceType: "sometype",
					OptionalResourceIds:  []string{"someid", "anotherid"},
				})
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ? AND object_id IN (?,?)",
			[]any{"sometype", "someid", "anotherid"},
			[]string{"ns"},
		},
		{
			"subjects filter with no IDs or relations",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				})
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ((subject_ns = ?))",
			[]any{"somesubjectype"},
			[]string{"subject_ns"},
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
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ((subject_ns = ?) OR (subject_ns = ?))",
			[]any{"somesubjectype", "anothersubjectype"},
			[]string{},
		},
		{
			"subjects filter with single ID",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"somesubjectid"},
				})
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ((subject_ns = ? AND subject_object_id IN (?)))",
			[]any{"somesubjectype", "somesubjectid"},
			[]string{"subject_ns", "subject_object_id"},
		},
		{
			"subjects filter with single ID and no type",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectIds: []string{"somesubjectid"},
				})
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ((subject_object_id IN (?)))",
			[]any{"somesubjectid"},
			[]string{"subject_object_id"},
		},
		{
			"empty subjects filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{})
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ((1=1))",
			nil,
			[]string{},
		},
		{
			"subjects filter with multiple IDs",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"somesubjectid", "anothersubjectid"},
				})
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ((subject_ns = ? AND subject_object_id IN (?,?)))",
			[]any{"somesubjectype", "somesubjectid", "anothersubjectid"},
			[]string{"subject_ns"},
		},
		{
			"subjects filter with single ellipsis relation",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
				})
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ((subject_ns = ? AND subject_relation = ?))",
			[]any{"somesubjectype", "..."},
			[]string{"subject_ns", "subject_relation"},
		},
		{
			"subjects filter with single defined relation",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel"),
				})
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ((subject_ns = ? AND subject_relation = ?))",
			[]any{"somesubjectype", "somesubrel"},
			[]string{"subject_ns", "subject_relation"},
		},
		{
			"subjects filter with only non-ellipsis",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					RelationFilter:      datastore.SubjectRelationFilter{}.WithOnlyNonEllipsisRelations(),
				})
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ((subject_ns = ? AND subject_relation <> ?))",
			[]any{"somesubjectype", "..."},
			[]string{"subject_ns"},
		},
		{
			"subjects filter with defined relation and ellipsis",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel").WithEllipsisRelation(),
				})
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ((subject_ns = ? AND (subject_relation = ? OR subject_relation = ?)))",
			[]any{"somesubjectype", "...", "somesubrel"},
			[]string{"subject_ns"},
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
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ((subject_ns = ? AND subject_object_id IN (?,?) AND (subject_relation = ? OR subject_relation = ?)))",
			[]any{"somesubjectype", "somesubjectid", "anothersubjectid", "...", "somesubrel"},
			[]string{"subject_ns"},
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
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ((subject_ns = ? AND subject_object_id IN (?,?) AND (subject_relation = ? OR subject_relation = ?)) OR (subject_ns = ? AND subject_object_id IN (?,?) AND (subject_relation = ? OR subject_relation = ?)) OR (subject_ns = ? AND subject_relation <> ?))",
			[]any{"somesubjectype", "a", "b", "...", "somesubrel", "anothersubjecttype", "b", "c", "...", "anotherrel", "thirdsubjectype", "..."},
			[]string{},
		},
		{
			"v1 subject filter with namespace",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType: "subns",
				})
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND subject_ns = ?",
			[]any{"subns"},
			[]string{"subject_ns"},
		},
		{
			"v1 subject filter with subject id",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
				})
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND subject_ns = ? AND subject_object_id = ?",
			[]any{"subns", "subid"},
			[]string{"subject_ns", "subject_object_id"},
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
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND subject_ns = ? AND subject_relation = ?",
			[]any{"subns", "subrel"},
			[]string{"subject_ns", "subject_relation"},
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
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND subject_ns = ? AND subject_relation = ?",
			[]any{"subns", "..."},
			[]string{"subject_ns", "subject_relation"},
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
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND subject_ns = ? AND subject_object_id = ? AND subject_relation = ?",
			[]any{"subns", "subid", "somerel"},
			[]string{"subject_ns", "subject_object_id", "subject_relation"},
		},
		{
			"limit",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.limit(100)
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) LIMIT 100",
			nil,
			[]string{},
		},
		{
			"full resources filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType:     "someresourcetype",
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
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ? AND relation = ? AND object_id IN (?,?) AND ((subject_ns = ? AND subject_object_id IN (?,?) AND (subject_relation = ? OR subject_relation = ?)))",
			[]any{"someresourcetype", "somerelation", "someid", "anotherid", "somesubjectype", "somesubjectid", "anothersubjectid", "...", "somesubrel"},
			[]string{"ns", "relation", "subject_ns"},
		},
		{
			"order by",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType: "someresourcetype",
					},
				).TupleOrder(options.ByResource)
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ? ORDER BY ns, object_id, relation, subject_ns, subject_object_id, subject_relation",
			[]any{"someresourcetype"},
			[]string{"ns"},
		},
		{
			"after with just namespace",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType: "someresourcetype",
					},
				).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ? AND (object_id,relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?)",
			[]any{"someresourcetype", "foo", "viewer", "user", "bar", "..."},
			[]string{"ns"},
		},
		{
			"after with just relation",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceRelation: "somerelation",
					},
				).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND relation = ? AND (ns,object_id,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?)",
			[]any{"somerelation", "someresourcetype", "foo", "user", "bar", "..."},
			[]string{"relation"},
		},
		{
			"after with namespace and single resource id",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType: "someresourcetype",
						OptionalResourceIds:  []string{"one"},
					},
				).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ? AND object_id IN (?) AND (relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?)",
			[]any{"someresourcetype", "one", "viewer", "user", "bar", "..."},
			[]string{"ns", "object_id"},
		},
		{
			"after with single resource id",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceIds: []string{"one"},
					},
				).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND object_id IN (?) AND (ns,relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?)",
			[]any{"one", "someresourcetype", "viewer", "user", "bar", "..."},
			[]string{"object_id"},
		},
		{
			"after with namespace and resource ids",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType: "someresourcetype",
						OptionalResourceIds:  []string{"one", "two"},
					},
				).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ? AND object_id IN (?,?) AND (object_id,relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?)",
			[]any{"someresourcetype", "one", "two", "foo", "viewer", "user", "bar", "..."},
			[]string{"ns"},
		},
		{
			"after with namespace and relation",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType:     "someresourcetype",
						OptionalResourceRelation: "somerelation",
					},
				).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ? AND relation = ? AND (object_id,subject_ns,subject_object_id,subject_relation) > (?,?,?,?)",
			[]any{"someresourcetype", "somerelation", "foo", "user", "bar", "..."},
			[]string{"ns", "relation"},
		},
		{
			"after with subject namespace",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				}).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ((subject_ns = ?)) AND (ns,object_id,relation,subject_object_id,subject_relation) > (?,?,?,?,?)",
			[]any{"somesubjectype", "someresourcetype", "foo", "viewer", "bar", "..."},
			[]string{"subject_ns"},
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
				}).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ((subject_ns = ?)) AND ((subject_ns = ?)) AND (ns,object_id,relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?,?)",
			[]any{"somesubjectype", "anothersubjectype", "someresourcetype", "foo", "viewer", "user", "bar", "..."},
			[]string{},
		},
		{
			"after with resource ID prefix",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithResourceIDPrefix("someprefix").After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND object_id LIKE ? AND (ns,object_id,relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?,?)",
			[]any{"someprefix%", "someresourcetype", "foo", "viewer", "user", "bar", "..."},
			[]string{},
		},
		{
			"order by subject",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType: "someresourcetype",
					},
				).TupleOrder(options.BySubject)
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ? ORDER BY subject_ns, subject_object_id, subject_relation, ns, object_id, relation",
			[]any{"someresourcetype"},
			[]string{"ns"},
		},
		{
			"order by subject, after with subject namespace",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				}).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.BySubject)
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ((subject_ns = ?)) AND (subject_object_id,ns,object_id,relation,subject_relation) > (?,?,?,?,?)",
			[]any{"somesubjectype", "bar", "someresourcetype", "foo", "viewer", "..."},
			[]string{"subject_ns"},
		},
		{
			"order by subject, after with subject namespace and subject object id",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"foo"},
				}).After(toCursor(tuple.MustParse("someresourcetype:someresource#viewer@user:bar")), options.BySubject)
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ((subject_ns = ? AND subject_object_id IN (?))) AND (ns,object_id,relation,subject_relation) > (?,?,?,?)",
			[]any{"somesubjectype", "foo", "someresourcetype", "someresource", "viewer", "..."},
			[]string{"subject_ns", "subject_object_id"},
		},
		{
			"order by subject, after with subject namespace and multiple subject object IDs",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"foo", "bar"},
				}).After(toCursor(tuple.MustParse("someresourcetype:someresource#viewer@user:next")), options.BySubject)
			},
			"SELECT * WHERE (expiration IS NULL OR expiration > NOW()) AND ((subject_ns = ? AND subject_object_id IN (?,?))) AND (subject_object_id,ns,object_id,relation,subject_relation) > (?,?,?,?,?)",
			[]any{"somesubjectype", "foo", "bar", "next", "someresourcetype", "someresource", "viewer", "..."},
			[]string{"subject_ns"},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			schema := NewSchemaInformation(
				"relationtuples",
				"ns",
				"object_id",
				"relation",
				"subject_ns",
				"subject_object_id",
				"subject_relation",
				"caveat",
				"caveat_context",
				"expiration",
				TupleComparison,
				sq.Question,
				"NOW",
				ColumnOptimizationOptionStaticValues,
			)
			filterer := NewSchemaQueryFiltererForRelationshipsSelect(schema, 100)

			ran := test.run(filterer)
			foundStaticColumns := []string{}
			for col, tracker := range ran.filteringColumnTracker {
				if tracker.SingleValue != nil {
					foundStaticColumns = append(foundStaticColumns, col)
				}
			}

			require.ElementsMatch(t, test.expectedStaticColumns, foundStaticColumns)

			ran.queryBuilder = ran.queryBuilder.Columns("*")

			sql, args, err := ran.queryBuilder.ToSql()
			require.NoError(t, err)
			require.Equal(t, test.expectedSQL, sql)
			require.Equal(t, test.expectedArgs, args)
		})
	}
}

func TestExecuteQuery(t *testing.T) {
	tcs := []struct {
		name                       string
		run                        func(filterer SchemaQueryFilterer) SchemaQueryFilterer
		options                    []options.QueryOptionsOption
		expectedSQL                string
		expectedArgs               []any
		expectedSelectingNoColumns bool
		expectedSkipCaveats        bool
	}{
		{
			name: "filter by static resource type",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			expectedSQL:  "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ?",
			expectedArgs: []any{"sometype"},
		},
		{
			name: "filter by static resource type and resource ID",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj")
			},
			expectedSQL:  "SELECT relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ? AND object_id = ?",
			expectedArgs: []any{"sometype", "someobj"},
		},
		{
			name: "filter by static resource type and resource ID prefix",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").MustFilterWithResourceIDPrefix("someprefix")
			},
			expectedSQL:  "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ? AND object_id LIKE ?",
			expectedArgs: []any{"sometype", "someprefix%"},
		},
		{
			name: "filter by static resource type and resource IDs",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").MustFilterToResourceIDs([]string{"someobj", "anotherobj"})
			},
			expectedSQL:  "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ? AND object_id IN (?,?)",
			expectedArgs: []any{"sometype", "someobj", "anotherobj"},
		},
		{
			name: "filter by static resource type, resource ID and relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj").FilterToRelation("somerel")
			},
			expectedSQL:  "SELECT subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ? AND object_id = ? AND relation = ?",
			expectedArgs: []any{"sometype", "someobj", "somerel"},
		},
		{
			name: "filter by static resource type, resource ID, relation and subject type",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj").FilterToRelation("somerel").FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType: "subns",
				})
			},
			expectedSQL:  "SELECT subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ? AND object_id = ? AND relation = ? AND subject_ns = ?",
			expectedArgs: []any{"sometype", "someobj", "somerel", "subns"},
		},
		{
			name: "filter by static resource type, resource ID, relation, subject type and subject ID",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj").FilterToRelation("somerel").FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
				})
			},
			expectedSQL:  "SELECT subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ? AND object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ?",
			expectedArgs: []any{"sometype", "someobj", "somerel", "subns", "subid"},
		},
		{
			name: "filter by static resource type, resource ID, relation, subject type, subject ID and subject relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj").FilterToRelation("somerel").FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "subrel",
					},
				})
			},
			expectedSQL:  "SELECT caveat, caveat_context, expiration FROM relationtuples WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ? AND object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation = ?",
			expectedArgs: []any{"sometype", "someobj", "somerel", "subns", "subid", "subrel"},
		},
		{
			name: "filter by static everything without caveats",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj").FilterToRelation("somerel").FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "subrel",
					},
				})
			},
			options: []options.QueryOptionsOption{
				options.WithSkipCaveats(true),
			},
			expectedSkipCaveats:        true,
			expectedSelectingNoColumns: false,
			expectedSQL:                "SELECT expiration FROM relationtuples WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ? AND object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation = ?",
			expectedArgs:               []any{"sometype", "someobj", "somerel", "subns", "subid", "subrel"},
		},
		{
			name: "filter by static everything (except one field) without caveats",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").MustFilterToResourceIDs([]string{"someobj", "anotherobj"}).FilterToRelation("somerel").FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "subrel",
					},
				})
			},
			options: []options.QueryOptionsOption{
				options.WithSkipCaveats(true),
			},
			expectedSkipCaveats: true,
			expectedSQL:         "SELECT object_id, expiration FROM relationtuples WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ? AND object_id IN (?,?) AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation = ?",
			expectedArgs:        []any{"sometype", "someobj", "anotherobj", "somerel", "subns", "subid", "subrel"},
		},
		{
			name: "filter by static resource type with no caveats",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			options: []options.QueryOptionsOption{
				options.WithSkipCaveats(true),
			},
			expectedSkipCaveats: true,
			expectedSQL:         "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, expiration FROM relationtuples WHERE (expiration IS NULL OR expiration > NOW()) AND ns = ?",
			expectedArgs:        []any{"sometype"},
		},
		{
			name: "filter by just subject type",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType: "subns",
				})
			},
			expectedSQL:  "SELECT ns, object_id, relation, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE (expiration IS NULL OR expiration > NOW()) AND subject_ns = ?",
			expectedArgs: []any{"subns"},
		},
		{
			name: "filter by just subject type and subject ID",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
				})
			},
			expectedSQL:  "SELECT ns, object_id, relation, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE (expiration IS NULL OR expiration > NOW()) AND subject_ns = ? AND subject_object_id = ?",
			expectedArgs: []any{"subns", "subid"},
		},
		{
			name: "filter by just subject type and subject relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType: "subns",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "subrel",
					},
				})
			},
			expectedSQL:  "SELECT ns, object_id, relation, subject_object_id, caveat, caveat_context, expiration FROM relationtuples WHERE (expiration IS NULL OR expiration > NOW()) AND subject_ns = ? AND subject_relation = ?",
			expectedArgs: []any{"subns", "subrel"},
		},
		{
			name: "filter by just subject type and subject ID and relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "subrel",
					},
				})
			},
			expectedSQL:  "SELECT ns, object_id, relation, caveat, caveat_context, expiration FROM relationtuples WHERE (expiration IS NULL OR expiration > NOW()) AND subject_ns = ? AND subject_object_id = ? AND subject_relation = ?",
			expectedArgs: []any{"subns", "subid", "subrel"},
		},
		{
			name: "filter by multiple subject types, but static subject ID",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
				}).FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "anothersubns",
					OptionalSubjectId: "subid",
				})
			},
			expectedSQL:  "SELECT ns, object_id, relation, subject_ns, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE (expiration IS NULL OR expiration > NOW()) AND subject_ns = ? AND subject_object_id = ? AND subject_ns = ? AND subject_object_id = ?",
			expectedArgs: []any{"subns", "subid", "anothersubns", "subid"},
		},
		{
			name: "multiple subjects filters with just types",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				}, datastore.SubjectsSelector{
					OptionalSubjectType: "anothersubjectype",
				})
			},
			expectedSQL:  "SELECT ns, object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE (expiration IS NULL OR expiration > NOW()) AND ((subject_ns = ?) OR (subject_ns = ?))",
			expectedArgs: []any{"somesubjectype", "anothersubjectype"},
		},
		{
			name: "multiple subjects filters with just types and static resource type",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				}, datastore.SubjectsSelector{
					OptionalSubjectType: "anothersubjectype",
				}).FilterToResourceType("sometype")
			},
			expectedSQL:  "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE (expiration IS NULL OR expiration > NOW()) AND ((subject_ns = ?) OR (subject_ns = ?)) AND ns = ?",
			expectedArgs: []any{"somesubjectype", "anothersubjectype", "sometype"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			schema := NewSchemaInformation(
				"relationtuples",
				"ns",
				"object_id",
				"relation",
				"subject_ns",
				"subject_object_id",
				"subject_relation",
				"caveat",
				"caveat_context",
				"expiration",
				TupleComparison,
				sq.Question,
				"NOW",
				ColumnOptimizationOptionStaticValues,
			)
			filterer := NewSchemaQueryFiltererForRelationshipsSelect(schema, 100)
			ran := tc.run(filterer)

			var wasRun bool
			fake := QueryExecutor{
				Executor: func(ctx context.Context, queryInfo QueryInfo, sql string, args []any) (datastore.RelationshipIterator, error) {
					wasRun = true
					require.Equal(t, tc.expectedSQL, sql)
					require.Equal(t, tc.expectedArgs, args)
					require.Equal(t, tc.expectedSelectingNoColumns, queryInfo.SelectingNoColumns)
					require.Equal(t, tc.expectedSkipCaveats, queryInfo.SkipCaveats)
					return nil, nil
				},
			}
			_, err := fake.ExecuteQuery(context.Background(), ran, tc.options...)
			require.NoError(t, err)
			require.True(t, wasRun)
		})
	}
}
