package common

import (
	"testing"

	"github.com/authzed/spicedb/pkg/tuple"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestSchemaQueryFilterer(t *testing.T) {
	tests := []struct {
		name         string
		run          func(filterer SchemaQueryFilterer) SchemaQueryFilterer
		expectedSQL  string
		expectedArgs []any
	}{
		{
			"relation filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToRelation("somerelation")
			},
			"SELECT * WHERE relation = ?",
			[]any{"somerelation"},
		},
		{
			"resource ID filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceID("someresourceid")
			},
			"SELECT * WHERE object_id = ?",
			[]any{"someresourceid"},
		},
		{
			"resource IDs filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceIDs([]string{"someresourceid", "anotherresourceid"})
			},
			"SELECT * WHERE object_id IN (?, ?)",
			[]any{"someresourceid", "anotherresourceid"},
		},
		{
			"resource type filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			"SELECT * WHERE ns = ?",
			[]any{"sometype"},
		},
		{
			"resource filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj").FilterToRelation("somerel")
			},
			"SELECT * WHERE ns = ? AND object_id = ? AND relation = ?",
			[]any{"sometype", "someobj", "somerel"},
		},
		{
			"relationships filter with no IDs or relations",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					ResourceType: "sometype",
				})
			},
			"SELECT * WHERE ns = ?",
			[]any{"sometype"},
		},
		{
			"relationships filter with single ID",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					ResourceType:        "sometype",
					OptionalResourceIds: []string{"someid"},
				})
			},
			"SELECT * WHERE ns = ? AND object_id IN (?)",
			[]any{"sometype", "someid"},
		},
		{
			"relationships filter with no IDs",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					ResourceType:        "sometype",
					OptionalResourceIds: []string{},
				})
			},
			"SELECT * WHERE ns = ?",
			[]any{"sometype"},
		},
		{
			"relationships filter with multiple IDs",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					ResourceType:        "sometype",
					OptionalResourceIds: []string{"someid", "anotherid"},
				})
			},
			"SELECT * WHERE ns = ? AND object_id IN (?, ?)",
			[]any{"sometype", "someid", "anotherid"},
		},
		{
			"subjects filter with no IDs or relations",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterWithSubjectsFilter(datastore.SubjectsFilter{
					SubjectType: "somesubjectype",
				})
			},
			"SELECT * WHERE subject_ns = ?",
			[]any{"somesubjectype"},
		},
		{
			"subjects filter with single ID",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterWithSubjectsFilter(datastore.SubjectsFilter{
					SubjectType:        "somesubjectype",
					OptionalSubjectIds: []string{"somesubjectid"},
				})
			},
			"SELECT * WHERE subject_ns = ? AND subject_object_id IN (?)",
			[]any{"somesubjectype", "somesubjectid"},
		},
		{
			"subjects filter with multiple IDs",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterWithSubjectsFilter(datastore.SubjectsFilter{
					SubjectType:        "somesubjectype",
					OptionalSubjectIds: []string{"somesubjectid", "anothersubjectid"},
				})
			},
			"SELECT * WHERE subject_ns = ? AND subject_object_id IN (?, ?)",
			[]any{"somesubjectype", "somesubjectid", "anothersubjectid"},
		},
		{
			"subjects filter with single ellipsis relation",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterWithSubjectsFilter(datastore.SubjectsFilter{
					SubjectType:    "somesubjectype",
					RelationFilter: datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
				})
			},
			"SELECT * WHERE subject_ns = ? AND subject_relation = ?",
			[]any{"somesubjectype", "..."},
		},
		{
			"subjects filter with single defined relation",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterWithSubjectsFilter(datastore.SubjectsFilter{
					SubjectType:    "somesubjectype",
					RelationFilter: datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel"),
				})
			},
			"SELECT * WHERE subject_ns = ? AND subject_relation = ?",
			[]any{"somesubjectype", "somesubrel"},
		},
		{
			"subjects filter with defined relation and ellipsis",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterWithSubjectsFilter(datastore.SubjectsFilter{
					SubjectType:    "somesubjectype",
					RelationFilter: datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel").WithEllipsisRelation(),
				})
			},
			"SELECT * WHERE subject_ns = ? AND (subject_relation = ? OR subject_relation = ?)",
			[]any{"somesubjectype", "...", "somesubrel"},
		},
		{
			"subjects filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterWithSubjectsFilter(datastore.SubjectsFilter{
					SubjectType:        "somesubjectype",
					OptionalSubjectIds: []string{"somesubjectid", "anothersubjectid"},
					RelationFilter:     datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel").WithEllipsisRelation(),
				})
			},
			"SELECT * WHERE subject_ns = ? AND subject_object_id IN (?, ?) AND (subject_relation = ? OR subject_relation = ?)",
			[]any{"somesubjectype", "somesubjectid", "anothersubjectid", "...", "somesubrel"},
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
		},
		{
			"empty filterToUsersets",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.filterToUsersets(nil)
			},
			"SELECT *",
			nil,
		},
		{
			"filterToUsersets",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.filterToUsersets([]*core.ObjectAndRelation{
					tuple.ParseONR("document:foo#somerel"),
					tuple.ParseONR("team:bar#member"),
				})
			},
			"SELECT * WHERE (subject_ns = ? AND subject_object_id = ? AND subject_relation = ? OR subject_ns = ? AND subject_object_id = ? AND subject_relation = ?)",
			[]any{"document", "foo", "somerel", "team", "bar", "member"},
		},
		{
			"limit",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.limit(100)
			},
			"SELECT * LIMIT 100",
			nil,
		},
		{
			"full resources filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						ResourceType:             "someresourcetype",
						OptionalResourceIds:      []string{"someid", "anotherid"},
						OptionalResourceRelation: "somerelation",
						OptionalSubjectsFilter: &datastore.SubjectsFilter{
							SubjectType:        "somesubjectype",
							OptionalSubjectIds: []string{"somesubjectid", "anothersubjectid"},
							RelationFilter:     datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel").WithEllipsisRelation(),
						},
					},
				)
			},
			"SELECT * WHERE ns = ? AND relation = ? AND object_id IN (?, ?) AND subject_ns = ? AND subject_object_id IN (?, ?) AND (subject_relation = ? OR subject_relation = ?)",
			[]any{"someresourcetype", "somerelation", "someid", "anotherid", "somesubjectype", "somesubjectid", "anothersubjectid", "...", "somesubrel"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := sq.Select("*")
			filterer := NewSchemaQueryFilterer(SchemaInformation{
				TableTuple:          "tuple",
				ColNamespace:        "ns",
				ColObjectID:         "object_id",
				ColRelation:         "relation",
				ColUsersetNamespace: "subject_ns",
				ColUsersetObjectID:  "subject_object_id",
				ColUsersetRelation:  "subject_relation",
			}, base)

			sql, args, err := test.run(filterer).queryBuilder.ToSql()
			require.NoError(t, err)
			require.Equal(t, test.expectedSQL, sql)
			require.Equal(t, test.expectedArgs, args)
		})
	}
}
