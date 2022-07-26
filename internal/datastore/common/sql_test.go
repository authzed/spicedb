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
		expectedArgs []interface{}
	}{
		{
			"relation filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToRelation("somerelation")
			},
			"SELECT * WHERE relation = ?",
			[]interface{}{"somerelation"},
		},
		{
			"resource ID filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceID("someresourceid")
			},
			"SELECT * WHERE object_id = ?",
			[]interface{}{"someresourceid"},
		},
		{
			"resource type filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			"SELECT * WHERE ns = ?",
			[]interface{}{"sometype"},
		},
		{
			"resource filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj").FilterToRelation("somerel")
			},
			"SELECT * WHERE ns = ? AND object_id = ? AND relation = ?",
			[]interface{}{"sometype", "someobj", "somerel"},
		},
		{
			"subjects filter with no IDs or relations",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterWithSubjectsFilter(datastore.SubjectsFilter{
					SubjectType: "somesubjectype",
				})
			},
			"SELECT * WHERE subject_ns = ?",
			[]interface{}{"somesubjectype"},
		},
		{
			"subjects filter with single ID",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterWithSubjectsFilter(datastore.SubjectsFilter{
					SubjectType: "somesubjectype",
					SubjectIds:  []string{"somesubjectid"},
				})
			},
			"SELECT * WHERE subject_ns = ? AND subject_object_id = ?",
			[]interface{}{"somesubjectype", "somesubjectid"},
		},
		{
			"subjects filter with multiple IDs",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterWithSubjectsFilter(datastore.SubjectsFilter{
					SubjectType: "somesubjectype",
					SubjectIds:  []string{"somesubjectid", "anothersubjectid"},
				})
			},
			"SELECT * WHERE subject_ns = ? AND subject_object_id IN (?, ?)",
			[]interface{}{"somesubjectype", "somesubjectid", "anothersubjectid"},
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
			[]interface{}{"somesubjectype", "..."},
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
			[]interface{}{"somesubjectype", "somesubrel"},
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
			[]interface{}{"somesubjectype", "...", "somesubrel"},
		},
		{
			"subjects filter",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterWithSubjectsFilter(datastore.SubjectsFilter{
					SubjectType:    "somesubjectype",
					SubjectIds:     []string{"somesubjectid", "anothersubjectid"},
					RelationFilter: datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel").WithEllipsisRelation(),
				})
			},
			"SELECT * WHERE subject_ns = ? AND subject_object_id IN (?, ?) AND (subject_relation = ? OR subject_relation = ?)",
			[]interface{}{"somesubjectype", "somesubjectid", "anothersubjectid", "...", "somesubrel"},
		},
		{
			"v1 subject filter with namespace",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType: "subns",
				})
			},
			"SELECT * WHERE subject_ns = ?",
			[]interface{}{"subns"},
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
			[]interface{}{"subns", "subid"},
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
			[]interface{}{"subns", "subrel"},
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
			[]interface{}{"subns", "..."},
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
			[]interface{}{"subns", "subid", "somerel"},
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
			[]interface{}{"document", "foo", "somerel", "team", "bar", "member"},
		},
		{
			"limit",
			func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.limit(100)
			},
			"SELECT * LIMIT 100",
			nil,
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
