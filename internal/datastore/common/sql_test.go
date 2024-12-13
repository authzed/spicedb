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
		name                   string
		run                    func(filterer SchemaQueryFilterer) SchemaQueryFilterer
		expectedSQL            string
		expectedArgs           []any
		expectedStaticColumns  []string
		withExpirationDisabled bool
	}{
		{
			name: "relation filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToRelation("somerelation")
			},
			expectedSQL:           "SELECT * WHERE relation = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"somerelation"},
			expectedStaticColumns: []string{"relation"},
		},
		{
			name: "relation filter without expiration",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToRelation("somerelation")
			},
			expectedSQL:            "SELECT * WHERE relation = ?",
			expectedArgs:           []any{"somerelation"},
			expectedStaticColumns:  []string{"relation"},
			withExpirationDisabled: true,
		},
		{
			name: "resource ID filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceID("someresourceid")
			},
			expectedSQL:           "SELECT * WHERE object_id = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"someresourceid"},
			expectedStaticColumns: []string{"object_id"},
		},
		{
			name: "resource IDs filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithResourceIDPrefix("someprefix")
			},
			expectedSQL:           "SELECT * WHERE object_id LIKE ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"someprefix%"},
			expectedStaticColumns: []string{},
		},
		{
			name: "resource IDs prefix filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterToResourceIDs([]string{"someresourceid", "anotherresourceid"})
			},
			expectedSQL:           "SELECT * WHERE object_id IN (?,?) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"someresourceid", "anotherresourceid"},
			expectedStaticColumns: []string{},
		},
		{
			name: "resource type filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			expectedSQL:           "SELECT * WHERE ns = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"sometype"},
			expectedStaticColumns: []string{"ns"},
		},
		{
			name: "resource filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj").FilterToRelation("somerel")
			},
			expectedSQL:           "SELECT * WHERE ns = ? AND object_id = ? AND relation = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"sometype", "someobj", "somerel"},
			expectedStaticColumns: []string{"ns", "object_id", "relation"},
		},
		{
			name: "relationships filter with no IDs or relations",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					OptionalResourceType: "sometype",
				})
			},
			expectedSQL:           "SELECT * WHERE ns = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"sometype"},
			expectedStaticColumns: []string{"ns"},
		},
		{
			name: "relationships filter with single ID",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					OptionalResourceType: "sometype",
					OptionalResourceIds:  []string{"someid"},
				})
			},
			expectedSQL:           "SELECT * WHERE ns = ? AND object_id IN (?) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"sometype", "someid"},
			expectedStaticColumns: []string{"ns", "object_id"},
		},
		{
			name: "relationships filter with no IDs",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					OptionalResourceType: "sometype",
					OptionalResourceIds:  []string{},
				})
			},
			expectedSQL:           "SELECT * WHERE ns = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"sometype"},
			expectedStaticColumns: []string{"ns"},
		},
		{
			name: "relationships filter with multiple IDs",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					OptionalResourceType: "sometype",
					OptionalResourceIds:  []string{"someid", "anotherid"},
				})
			},
			expectedSQL:           "SELECT * WHERE ns = ? AND object_id IN (?,?) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"sometype", "someid", "anotherid"},
			expectedStaticColumns: []string{"ns"},
		},
		{
			name: "subjects filter with no IDs or relations",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				})
			},
			expectedSQL:           "SELECT * WHERE ((subject_ns = ?)) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"somesubjectype"},
			expectedStaticColumns: []string{"subject_ns"},
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
			expectedSQL:           "SELECT * WHERE ((subject_ns = ?) OR (subject_ns = ?)) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"somesubjectype", "anothersubjectype"},
			expectedStaticColumns: []string{},
		},
		{
			name: "subjects filter with single ID",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"somesubjectid"},
				})
			},
			expectedSQL:           "SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?))) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"somesubjectype", "somesubjectid"},
			expectedStaticColumns: []string{"subject_ns", "subject_object_id"},
		},
		{
			name: "subjects filter with single ID and no type",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectIds: []string{"somesubjectid"},
				})
			},
			expectedSQL:           "SELECT * WHERE ((subject_object_id IN (?))) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"somesubjectid"},
			expectedStaticColumns: []string{"subject_object_id"},
		},
		{
			name: "empty subjects filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{})
			},
			expectedSQL:           "SELECT * WHERE ((1=1)) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          nil,
			expectedStaticColumns: []string{},
		},
		{
			name: "subjects filter with multiple IDs",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"somesubjectid", "anothersubjectid"},
				})
			},
			expectedSQL:           "SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?,?))) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"somesubjectype", "somesubjectid", "anothersubjectid"},
			expectedStaticColumns: []string{"subject_ns"},
		},
		{
			name: "subjects filter with single ellipsis relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
				})
			},
			expectedSQL:           "SELECT * WHERE ((subject_ns = ? AND subject_relation = ?)) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"somesubjectype", "..."},
			expectedStaticColumns: []string{"subject_ns", "subject_relation"},
		},
		{
			name: "subjects filter with single defined relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel"),
				})
			},
			expectedSQL:           "SELECT * WHERE ((subject_ns = ? AND subject_relation = ?)) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"somesubjectype", "somesubrel"},
			expectedStaticColumns: []string{"subject_ns", "subject_relation"},
		},
		{
			name: "subjects filter with only non-ellipsis",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					RelationFilter:      datastore.SubjectRelationFilter{}.WithOnlyNonEllipsisRelations(),
				})
			},
			expectedSQL:           "SELECT * WHERE ((subject_ns = ? AND subject_relation <> ?)) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"somesubjectype", "..."},
			expectedStaticColumns: []string{"subject_ns"},
		},
		{
			name: "subjects filter with defined relation and ellipsis",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel").WithEllipsisRelation(),
				})
			},
			expectedSQL:           "SELECT * WHERE ((subject_ns = ? AND (subject_relation = ? OR subject_relation = ?))) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"somesubjectype", "...", "somesubrel"},
			expectedStaticColumns: []string{"subject_ns"},
		},
		{
			name: "subjects filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"somesubjectid", "anothersubjectid"},
					RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel").WithEllipsisRelation(),
				})
			},
			expectedSQL:           "SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?,?) AND (subject_relation = ? OR subject_relation = ?))) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"somesubjectype", "somesubjectid", "anothersubjectid", "...", "somesubrel"},
			expectedStaticColumns: []string{"subject_ns"},
		},
		{
			name: "multiple subjects filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
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
			expectedSQL:           "SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?,?) AND (subject_relation = ? OR subject_relation = ?)) OR (subject_ns = ? AND subject_object_id IN (?,?) AND (subject_relation = ? OR subject_relation = ?)) OR (subject_ns = ? AND subject_relation <> ?)) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"somesubjectype", "a", "b", "...", "somesubrel", "anothersubjecttype", "b", "c", "...", "anotherrel", "thirdsubjectype", "..."},
			expectedStaticColumns: []string{},
		},
		{
			name: "v1 subject filter with namespace",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType: "subns",
				})
			},
			expectedSQL:           "SELECT * WHERE subject_ns = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"subns"},
			expectedStaticColumns: []string{"subject_ns"},
		},
		{
			name: "v1 subject filter with subject id",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
				})
			},
			expectedSQL:           "SELECT * WHERE subject_ns = ? AND subject_object_id = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"subns", "subid"},
			expectedStaticColumns: []string{"subject_ns", "subject_object_id"},
		},
		{
			name: "v1 subject filter with relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType: "subns",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "subrel",
					},
				})
			},
			expectedSQL:           "SELECT * WHERE subject_ns = ? AND subject_relation = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"subns", "subrel"},
			expectedStaticColumns: []string{"subject_ns", "subject_relation"},
		},
		{
			name: "v1 subject filter with empty relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType: "subns",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "",
					},
				})
			},
			expectedSQL:           "SELECT * WHERE subject_ns = ? AND subject_relation = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"subns", "..."},
			expectedStaticColumns: []string{"subject_ns", "subject_relation"},
		},
		{
			name: "v1 subject filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "somerel",
					},
				})
			},
			expectedSQL:           "SELECT * WHERE subject_ns = ? AND subject_object_id = ? AND subject_relation = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"subns", "subid", "somerel"},
			expectedStaticColumns: []string{"subject_ns", "subject_object_id", "subject_relation"},
		},
		{
			name: "limit",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.limit(100)
			},
			expectedSQL:           "SELECT * WHERE (expiration IS NULL OR expiration > NOW()) LIMIT 100",
			expectedArgs:          nil,
			expectedStaticColumns: []string{},
		},
		{
			name: "full resources filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
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
			expectedSQL:           "SELECT * WHERE ns = ? AND relation = ? AND object_id IN (?,?) AND ((subject_ns = ? AND subject_object_id IN (?,?) AND (subject_relation = ? OR subject_relation = ?))) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"someresourcetype", "somerelation", "someid", "anotherid", "somesubjectype", "somesubjectid", "anothersubjectid", "...", "somesubrel"},
			expectedStaticColumns: []string{"ns", "relation", "subject_ns"},
		},
		{
			name: "full resources filter without expiration",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
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
			expectedSQL:            "SELECT * WHERE ns = ? AND relation = ? AND object_id IN (?,?) AND ((subject_ns = ? AND subject_object_id IN (?,?) AND (subject_relation = ? OR subject_relation = ?)))",
			expectedArgs:           []any{"someresourcetype", "somerelation", "someid", "anotherid", "somesubjectype", "somesubjectid", "anothersubjectid", "...", "somesubrel"},
			expectedStaticColumns:  []string{"ns", "relation", "subject_ns"},
			withExpirationDisabled: true,
		},
		{
			name: "order by",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType: "someresourcetype",
					},
				).TupleOrder(options.ByResource)
			},
			expectedSQL:           "SELECT * WHERE ns = ? AND (expiration IS NULL OR expiration > NOW()) ORDER BY ns, object_id, relation, subject_ns, subject_object_id, subject_relation",
			expectedArgs:          []any{"someresourcetype"},
			expectedStaticColumns: []string{"ns"},
		},
		{
			name: "after with just namespace",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType: "someresourcetype",
					},
				).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			expectedSQL:           "SELECT * WHERE ns = ? AND (object_id,relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"someresourcetype", "foo", "viewer", "user", "bar", "..."},
			expectedStaticColumns: []string{"ns"},
		},
		{
			name: "after with just relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceRelation: "somerelation",
					},
				).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			expectedSQL:           "SELECT * WHERE relation = ? AND (ns,object_id,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"somerelation", "someresourcetype", "foo", "user", "bar", "..."},
			expectedStaticColumns: []string{"relation"},
		},
		{
			name: "after with namespace and single resource id",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType: "someresourcetype",
						OptionalResourceIds:  []string{"one"},
					},
				).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			expectedSQL:           "SELECT * WHERE ns = ? AND object_id IN (?) AND (relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"someresourcetype", "one", "viewer", "user", "bar", "..."},
			expectedStaticColumns: []string{"ns", "object_id"},
		},
		{
			name: "after with single resource id",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceIds: []string{"one"},
					},
				).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			expectedSQL:           "SELECT * WHERE object_id IN (?) AND (ns,relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"one", "someresourcetype", "viewer", "user", "bar", "..."},
			expectedStaticColumns: []string{"object_id"},
		},
		{
			name: "after with namespace and resource ids",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType: "someresourcetype",
						OptionalResourceIds:  []string{"one", "two"},
					},
				).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			expectedSQL:           "SELECT * WHERE ns = ? AND object_id IN (?,?) AND (object_id,relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"someresourcetype", "one", "two", "foo", "viewer", "user", "bar", "..."},
			expectedStaticColumns: []string{"ns"},
		},
		{
			name: "after with namespace and relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType:     "someresourcetype",
						OptionalResourceRelation: "somerelation",
					},
				).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			expectedSQL:           "SELECT * WHERE ns = ? AND relation = ? AND (object_id,subject_ns,subject_object_id,subject_relation) > (?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"someresourcetype", "somerelation", "foo", "user", "bar", "..."},
			expectedStaticColumns: []string{"ns", "relation"},
		},
		{
			name: "after with subject namespace",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				}).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			expectedSQL:           "SELECT * WHERE ((subject_ns = ?)) AND (ns,object_id,relation,subject_object_id,subject_relation) > (?,?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"somesubjectype", "someresourcetype", "foo", "viewer", "bar", "..."},
			expectedStaticColumns: []string{"subject_ns"},
		},
		{
			name: "after with subject namespaces",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				// NOTE: this isn't really valid (it'll return no results), but is a good test to ensure
				// the duplicate subject type results in the subject type being in the ORDER BY.
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				}).MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "anothersubjectype",
				}).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			expectedSQL:           "SELECT * WHERE ((subject_ns = ?)) AND ((subject_ns = ?)) AND (ns,object_id,relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"somesubjectype", "anothersubjectype", "someresourcetype", "foo", "viewer", "user", "bar", "..."},
			expectedStaticColumns: []string{},
		},
		{
			name: "after with resource ID prefix",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithResourceIDPrefix("someprefix").After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			expectedSQL:           "SELECT * WHERE object_id LIKE ? AND (ns,object_id,relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"someprefix%", "someresourcetype", "foo", "viewer", "user", "bar", "..."},
			expectedStaticColumns: []string{},
		},
		{
			name: "order by subject",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType: "someresourcetype",
					},
				).TupleOrder(options.BySubject)
			},
			expectedSQL:           "SELECT * WHERE ns = ? AND (expiration IS NULL OR expiration > NOW()) ORDER BY subject_ns, subject_object_id, subject_relation, ns, object_id, relation",
			expectedArgs:          []any{"someresourcetype"},
			expectedStaticColumns: []string{"ns"},
		},
		{
			name: "order by subject, after with subject namespace",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				}).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.BySubject)
			},
			expectedSQL:           "SELECT * WHERE ((subject_ns = ?)) AND (subject_object_id,ns,object_id,relation,subject_relation) > (?,?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"somesubjectype", "bar", "someresourcetype", "foo", "viewer", "..."},
			expectedStaticColumns: []string{"subject_ns"},
		},
		{
			name: "order by subject, after with subject namespace and subject object id",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"foo"},
				}).After(toCursor(tuple.MustParse("someresourcetype:someresource#viewer@user:bar")), options.BySubject)
			},
			expectedSQL:           "SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?))) AND (ns,object_id,relation,subject_relation) > (?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"somesubjectype", "foo", "someresourcetype", "someresource", "viewer", "..."},
			expectedStaticColumns: []string{"subject_ns", "subject_object_id"},
		},
		{
			name: "order by subject, after with subject namespace and multiple subject object IDs",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"foo", "bar"},
				}).After(toCursor(tuple.MustParse("someresourcetype:someresource#viewer@user:next")), options.BySubject)
			},
			expectedSQL:           "SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?,?))) AND (subject_object_id,ns,object_id,relation,subject_relation) > (?,?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:          []any{"somesubjectype", "foo", "bar", "next", "someresourcetype", "someresource", "viewer", "..."},
			expectedStaticColumns: []string{"subject_ns"},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			schema := NewSchemaInformationWithOptions(
				WithRelationshipTableName("relationtuples"),
				WithColNamespace("ns"),
				WithColObjectID("object_id"),
				WithColRelation("relation"),
				WithColUsersetNamespace("subject_ns"),
				WithColUsersetObjectID("subject_object_id"),
				WithColUsersetRelation("subject_relation"),
				WithColCaveatName("caveat"),
				WithColCaveatContext("caveat_context"),
				WithColExpiration("expiration"),
				WithPlaceholderFormat(sq.Question),
				WithPaginationFilterType(TupleComparison),
				WithColumnOptimization(ColumnOptimizationOptionStaticValues),
				WithNowFunction("NOW"),
			)
			filterer := NewSchemaQueryFiltererForRelationshipsSelect(*schema, 100)

			ran := test.run(filterer)
			foundStaticColumns := []string{}
			for col, tracker := range ran.filteringColumnTracker {
				if tracker.SingleValue != nil {
					foundStaticColumns = append(foundStaticColumns, col)
				}
			}

			require.ElementsMatch(t, test.expectedStaticColumns, foundStaticColumns)

			ran.queryBuilder = ran.queryBuilderWithExpirationFilter(test.withExpirationDisabled).Columns("*")

			sql, args, err := ran.queryBuilder.ToSql()
			require.NoError(t, err)
			require.Equal(t, test.expectedSQL, sql)
			require.Equal(t, test.expectedArgs, args)
		})
	}
}

func TestExecuteQuery(t *testing.T) {
	tcs := []struct {
		name                   string
		run                    func(filterer SchemaQueryFilterer) SchemaQueryFilterer
		options                []options.QueryOptionsOption
		expectedSQL            string
		expectedArgs           []any
		expectedSkipCaveats    bool
		expectedSkipExpiration bool
		withExpirationDisabled bool
	}{
		{
			name: "filter by static resource type",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			expectedSQL:  "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE ns = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs: []any{"sometype"},
		},
		{
			name: "filter by static resource type and resource ID",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj")
			},
			expectedSQL:  "SELECT relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE ns = ? AND object_id = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs: []any{"sometype", "someobj"},
		},
		{
			name: "filter by static resource type and resource ID prefix",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").MustFilterWithResourceIDPrefix("someprefix")
			},
			expectedSQL:  "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE ns = ? AND object_id LIKE ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs: []any{"sometype", "someprefix%"},
		},
		{
			name: "filter by static resource type and resource IDs",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").MustFilterToResourceIDs([]string{"someobj", "anotherobj"})
			},
			expectedSQL:  "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE ns = ? AND object_id IN (?,?) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs: []any{"sometype", "someobj", "anotherobj"},
		},
		{
			name: "filter by static resource type, resource ID and relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj").FilterToRelation("somerel")
			},
			expectedSQL:  "SELECT subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE ns = ? AND object_id = ? AND relation = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs: []any{"sometype", "someobj", "somerel"},
		},
		{
			name: "filter by static resource type, resource ID, relation and subject type",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj").FilterToRelation("somerel").FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType: "subns",
				})
			},
			expectedSQL:  "SELECT subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE ns = ? AND object_id = ? AND relation = ? AND subject_ns = ? AND (expiration IS NULL OR expiration > NOW())",
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
			expectedSQL:  "SELECT subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE ns = ? AND object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND (expiration IS NULL OR expiration > NOW())",
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
			expectedSQL:  "SELECT caveat, caveat_context, expiration FROM relationtuples WHERE ns = ? AND object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation = ? AND (expiration IS NULL OR expiration > NOW())",
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
			expectedSkipCaveats: true,
			expectedSQL:         "SELECT expiration FROM relationtuples WHERE ns = ? AND object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:        []any{"sometype", "someobj", "somerel", "subns", "subid", "subrel"},
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
			expectedSQL:         "SELECT object_id, expiration FROM relationtuples WHERE ns = ? AND object_id IN (?,?) AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation = ? AND (expiration IS NULL OR expiration > NOW())",
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
			expectedSQL:         "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, expiration FROM relationtuples WHERE ns = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:        []any{"sometype"},
		},
		{
			name: "filter by just subject type",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType: "subns",
				})
			},
			expectedSQL:  "SELECT ns, object_id, relation, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE subject_ns = ? AND (expiration IS NULL OR expiration > NOW())",
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
			expectedSQL:  "SELECT ns, object_id, relation, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE subject_ns = ? AND subject_object_id = ? AND (expiration IS NULL OR expiration > NOW())",
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
			expectedSQL:  "SELECT ns, object_id, relation, subject_object_id, caveat, caveat_context, expiration FROM relationtuples WHERE subject_ns = ? AND subject_relation = ? AND (expiration IS NULL OR expiration > NOW())",
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
			expectedSQL:  "SELECT ns, object_id, relation, caveat, caveat_context, expiration FROM relationtuples WHERE subject_ns = ? AND subject_object_id = ? AND subject_relation = ? AND (expiration IS NULL OR expiration > NOW())",
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
			expectedSQL:  "SELECT ns, object_id, relation, subject_ns, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE subject_ns = ? AND subject_object_id = ? AND subject_ns = ? AND subject_object_id = ? AND (expiration IS NULL OR expiration > NOW())",
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
			expectedSQL:  "SELECT ns, object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE ((subject_ns = ?) OR (subject_ns = ?)) AND (expiration IS NULL OR expiration > NOW())",
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
			expectedSQL:  "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE ((subject_ns = ?) OR (subject_ns = ?)) AND ns = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs: []any{"somesubjectype", "anothersubjectype", "sometype"},
		},
		{
			name: "filter by static resource type with expiration disabled",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			expectedSQL:            "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context FROM relationtuples WHERE ns = ?",
			expectedArgs:           []any{"sometype"},
			withExpirationDisabled: true,
		},
		{
			name: "filter by static resource type with expiration skipped",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			expectedSQL:            "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context FROM relationtuples WHERE ns = ?",
			expectedArgs:           []any{"sometype"},
			withExpirationDisabled: false,
			expectedSkipExpiration: true,
			options: []options.QueryOptionsOption{
				options.WithSkipExpiration(true),
			},
		},
		{
			name: "filter by static resource type with expiration skipped and disabled",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			expectedSQL:            "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context FROM relationtuples WHERE ns = ?",
			expectedArgs:           []any{"sometype"},
			withExpirationDisabled: true,
			expectedSkipExpiration: true,
			options: []options.QueryOptionsOption{
				options.WithSkipExpiration(true),
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			schema := NewSchemaInformationWithOptions(
				WithRelationshipTableName("relationtuples"),
				WithColNamespace("ns"),
				WithColObjectID("object_id"),
				WithColRelation("relation"),
				WithColUsersetNamespace("subject_ns"),
				WithColUsersetObjectID("subject_object_id"),
				WithColUsersetRelation("subject_relation"),
				WithColCaveatName("caveat"),
				WithColCaveatContext("caveat_context"),
				WithColExpiration("expiration"),
				WithPlaceholderFormat(sq.Question),
				WithPaginationFilterType(TupleComparison),
				WithColumnOptimization(ColumnOptimizationOptionStaticValues),
				WithNowFunction("NOW"),
				WithExpirationDisabled(tc.withExpirationDisabled),
			)
			filterer := NewSchemaQueryFiltererForRelationshipsSelect(*schema, 100)
			ran := tc.run(filterer)

			var wasRun bool
			fake := QueryRelationshipsExecutor{
				Executor: func(ctx context.Context, builder RelationshipsQueryBuilder) (datastore.RelationshipIterator, error) {
					sql, args, err := builder.SelectSQL()
					require.NoError(t, err)

					wasRun = true
					require.Equal(t, tc.expectedSQL, sql)
					require.Equal(t, tc.expectedArgs, args)
					require.Equal(t, tc.expectedSkipCaveats, builder.SkipCaveats)
					require.Equal(t, tc.expectedSkipExpiration, builder.SkipExpiration)
					return nil, nil
				},
			}
			_, err := fake.ExecuteQuery(context.Background(), ran, tc.options...)
			require.NoError(t, err)
			require.True(t, wasRun)
		})
	}
}
