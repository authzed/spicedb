package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
)

func TestIndexForFilter(t *testing.T) {
	tests := []struct {
		name     string
		filter   datastore.RelationshipsFilter
		expected string
	}{
		{
			name:     "no filter",
			filter:   datastore.RelationshipsFilter{},
			expected: "",
		},
		{
			name:     "filter by resource type",
			filter:   datastore.RelationshipsFilter{OptionalResourceType: "foo"},
			expected: "pk_relation_tuple",
		},
		{
			name: "filter by resource type and relation",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "foo",
				OptionalResourceRelation: "bar",
			},
			expected: "pk_relation_tuple",
		},
		{
			name: "filter by resource type, resource ID and relation",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "foo",
				OptionalResourceIds:      []string{"baz"},
				OptionalResourceRelation: "bar",
			},
			expected: "pk_relation_tuple",
		},
		{
			name: "filter by subject type, subject ID and relation",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "foo",
						OptionalSubjectIds:  []string{"bar"},
						RelationFilter: datastore.SubjectRelationFilter{
							NonEllipsisRelation: "baz",
						},
					},
				},
			},
			expected: "ix_relation_tuple_by_subject",
		},
		{
			name: "filter by subject type, subject ID",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "foo",
						OptionalSubjectIds:  []string{"bar"},
					},
				},
			},
			expected: "ix_relation_tuple_by_subject",
		},
		{
			name: "filter by subject relation, subject ID",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectIds: []string{"bar"},
						RelationFilter: datastore.SubjectRelationFilter{
							NonEllipsisRelation: "baz",
						},
					},
				},
			},
			expected: "ix_relation_tuple_by_subject",
		},
		{
			name: "filter by subject type",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "foo",
					},
				},
			},
			expected: "",
		},
		{
			name: "filter by resource type and subject type",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "foo",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "foo",
					},
				},
			},
			expected: "pk_relation_tuple",
		},
		{
			name: "filter by resource type and subject object ID",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "foo",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectIds: []string{"bar"},
					},
				},
			},
			expected: "",
		},
	}

	schema := Schema(common.ColumnOptimizationOptionNone, false, false)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index := IndexForFilter(*schema, test.filter)
			if test.expected == "" {
				require.Nil(t, index)
			} else {
				require.NotNil(t, index)
				require.Equal(t, test.expected, index.Name)
			}
		})
	}
}
