package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
)

const letCockroachDBDecide = ""

func TestIndexForFilter(t *testing.T) {
	tests := []struct {
		name                     string
		filter                   datastore.RelationshipsFilter
		expectedWithoutIntegrity string
		expectedWithIntegrity    string
	}{
		{
			name:                     "no filter",
			filter:                   datastore.RelationshipsFilter{},
			expectedWithoutIntegrity: letCockroachDBDecide,
			expectedWithIntegrity:    letCockroachDBDecide,
		},
		{
			name:                     "filter by resource type",
			filter:                   datastore.RelationshipsFilter{OptionalResourceType: "foo"},
			expectedWithoutIntegrity: "pk_relation_tuple",
			expectedWithIntegrity:    "ix_relation_tuple_with_integrity",
		},
		{
			name: "filter by resource type and relation",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "foo",
				OptionalResourceRelation: "bar",
			},
			expectedWithoutIntegrity: letCockroachDBDecide,
			expectedWithIntegrity:    letCockroachDBDecide,
		},
		{
			name: "filter by resource type, resource ID and relation",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "foo",
				OptionalResourceIds:      []string{"baz"},
				OptionalResourceRelation: "bar",
			},
			expectedWithoutIntegrity: "pk_relation_tuple",
			expectedWithIntegrity:    "ix_relation_tuple_with_integrity",
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
			expectedWithoutIntegrity: "ix_relation_tuple_by_subject",
			expectedWithIntegrity:    letCockroachDBDecide,
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
			expectedWithoutIntegrity: "ix_relation_tuple_by_subject",
			expectedWithIntegrity:    letCockroachDBDecide,
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
			expectedWithoutIntegrity: letCockroachDBDecide,
			expectedWithIntegrity:    letCockroachDBDecide,
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
			expectedWithoutIntegrity: "ix_relation_tuple_by_subject_relation",
			expectedWithIntegrity:    letCockroachDBDecide,
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
			expectedWithoutIntegrity: letCockroachDBDecide,
			expectedWithIntegrity:    letCockroachDBDecide,
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
			expectedWithoutIntegrity: letCockroachDBDecide,
			expectedWithIntegrity:    letCockroachDBDecide,
		},
		{
			name: "filter by resource type, relation and subject type and relation",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "foo",
				OptionalResourceRelation: "bar",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "foo",
						RelationFilter: datastore.SubjectRelationFilter{
							NonEllipsisRelation: "baz",
						},
					},
				},
			},
			expectedWithoutIntegrity: "ix_relation_tuple_by_subject_relation",
			expectedWithIntegrity:    letCockroachDBDecide,
		},
		{
			name: "filter by resource type, relation and subject type",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "foo",
				OptionalResourceRelation: "bar",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "foo",
					},
				},
			},
			expectedWithoutIntegrity: letCockroachDBDecide,
			expectedWithIntegrity:    letCockroachDBDecide,
		},
		{
			name: "filter by resource type, relation and subject relation",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "foo",
				OptionalResourceRelation: "bar",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						RelationFilter: datastore.SubjectRelationFilter{
							NonEllipsisRelation: "baz",
						},
					},
				},
			},
			expectedWithoutIntegrity: letCockroachDBDecide,
			expectedWithIntegrity:    letCockroachDBDecide,
		},
		{
			name: "filter by resource relation and subject type and relation",
			filter: datastore.RelationshipsFilter{
				OptionalResourceRelation: "bar",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "foo",
						RelationFilter: datastore.SubjectRelationFilter{
							NonEllipsisRelation: "baz",
						},
					},
				},
			},
			expectedWithoutIntegrity: letCockroachDBDecide,
			expectedWithIntegrity:    letCockroachDBDecide,
		},
		{
			name: "filter by resource type, relation and subject type and relation, include ellipsis",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "foo",
				OptionalResourceRelation: "bar",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "foo",
						RelationFilter: datastore.SubjectRelationFilter{
							IncludeEllipsisRelation: true,
							NonEllipsisRelation:     "baz",
						},
					},
				},
			},
			expectedWithoutIntegrity: "ix_relation_tuple_by_subject_relation",
			expectedWithIntegrity:    letCockroachDBDecide,
		},
		{
			name: "filter by resource type and ID prefix",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "foo",
				OptionalResourceIDPrefix: "prefix",
			},
			expectedWithoutIntegrity: "pk_relation_tuple",
			expectedWithIntegrity:    "ix_relation_tuple_with_integrity",
		},
		{
			name: "filter by resource type, ID prefix and relation",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "foo",
				OptionalResourceIDPrefix: "prefix",
				OptionalResourceRelation: "bar",
			},
			expectedWithoutIntegrity: letCockroachDBDecide,
			expectedWithIntegrity:    letCockroachDBDecide,
		},
		{
			name: "multiple subject selectors with different depths",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "foo",
					},
					{
						OptionalSubjectType: "bar",
						OptionalSubjectIds:  []string{"id1"},
						RelationFilter: datastore.SubjectRelationFilter{
							NonEllipsisRelation: "baz",
						},
					},
				},
			},
			expectedWithoutIntegrity: letCockroachDBDecide,
			expectedWithIntegrity:    letCockroachDBDecide,
		},
		{
			name: "multiple subject selectors with same depth",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "foo",
						OptionalSubjectIds:  []string{"id1"},
					},
					{
						OptionalSubjectType: "bar",
						OptionalSubjectIds:  []string{"id2"},
					},
				},
			},
			expectedWithoutIntegrity: "ix_relation_tuple_by_subject",
			expectedWithIntegrity:    letCockroachDBDecide,
		},
		{
			name: "multiple subject selectors with resource filter",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "resource_type",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "foo",
					},
					{
						OptionalSubjectType: "bar",
						OptionalSubjectIds:  []string{"id1"},
					},
				},
			},
			expectedWithoutIntegrity: letCockroachDBDecide,
			expectedWithIntegrity:    letCockroachDBDecide,
		},
		{
			name: "subject IDs without subject type",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectIds: []string{"id1"},
						RelationFilter: datastore.SubjectRelationFilter{
							NonEllipsisRelation: "relation",
						},
					},
				},
			},
			expectedWithoutIntegrity: letCockroachDBDecide,
			expectedWithIntegrity:    letCockroachDBDecide,
		},
		{
			name: "schema diff index with only non-ellipsis relations",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "foo",
				OptionalResourceRelation: "bar",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "baz",
						RelationFilter: datastore.SubjectRelationFilter{
							NonEllipsisRelation:      "qux",
							OnlyNonEllipsisRelations: true,
							IncludeEllipsisRelation:  false,
						},
					},
				},
			},
			expectedWithoutIntegrity: "ix_relation_tuple_by_subject_relation",
			expectedWithIntegrity:    letCockroachDBDecide,
		},
		{
			name: "empty subject selector",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{},
				},
			},
			expectedWithoutIntegrity: letCockroachDBDecide,
			expectedWithIntegrity:    letCockroachDBDecide,
		},
		{
			name: "IndexRelationshipBySubjectRelation with ellipsis for subject relation",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "foo",
				OptionalResourceRelation: "bar",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "foo",
						RelationFilter: datastore.SubjectRelationFilter{
							IncludeEllipsisRelation: true,
						},
					},
				},
			},
			expectedWithoutIntegrity: "ix_relation_tuple_by_subject_relation",
			expectedWithIntegrity:    letCockroachDBDecide,
		},
		{
			name: "IndexRelationshipBySubjectRelation with ellipsis and other relation for subject relation",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "foo",
				OptionalResourceRelation: "bar",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "foo",
						RelationFilter: datastore.SubjectRelationFilter{
							NonEllipsisRelation:     "baz",
							IncludeEllipsisRelation: true,
						},
					},
				},
			},
			expectedWithoutIntegrity: "ix_relation_tuple_by_subject_relation",
			expectedWithIntegrity:    letCockroachDBDecide,
		},
		{
			name: "IndexRelationshipBySubjectRelation with ellipsis and other relation as distinct filters on subject relation",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "foo",
				OptionalResourceRelation: "bar",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "foo",
						RelationFilter: datastore.SubjectRelationFilter{
							IncludeEllipsisRelation: true,
						},
					},
					{
						OptionalSubjectType: "foo2",
						RelationFilter: datastore.SubjectRelationFilter{
							NonEllipsisRelation: "baz",
						},
					},
				},
			},
			expectedWithoutIntegrity: "ix_relation_tuple_by_subject_relation",
			expectedWithIntegrity:    letCockroachDBDecide,
		},
	}

	for _, withIntegrity := range []bool{false, true} {
		integritySuffix := ""
		if withIntegrity {
			integritySuffix = " with integrity"
		}

		schema := Schema(common.ColumnOptimizationOptionNone, withIntegrity, false)
		for _, test := range tests {
			t.Run(test.name+integritySuffix, func(t *testing.T) {
				index, err := IndexForFilter(*schema, test.filter)
				require.NoError(t, err)

				expected := test.expectedWithoutIntegrity
				if withIntegrity {
					expected = test.expectedWithIntegrity
				}

				if expected == "" {
					require.Nil(t, index)
				} else {
					require.NotNil(t, index)
					require.Equal(t, expected, index.Name)
				}
			})
		}
	}
}

func TestIndexingHintForAllSpecificShapes(t *testing.T) {
	schema := Schema(common.ColumnOptimizationOptionNone, false, false)
	for _, shape := range queryshape.AllSpecificQueryShapes {
		t.Run(string(shape), func(t *testing.T) {
			index, err := IndexingHintForQueryShape(*schema, shape, nil)
			require.NoError(t, err)
			require.NotEqual(t, NoIndexingHint, index, "expected an indexing hint for shape %s", shape)
		})
	}
}

func TestIndexingHintForVaryingShapeWithFilter(t *testing.T) {
	schema := Schema(common.ColumnOptimizationOptionNone, false, false)

	tests := []struct {
		name          string
		filter        datastore.RelationshipsFilter
		expectedIndex string
	}{
		{
			name: "filter by resource type uses primary key",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "foo",
			},
			expectedIndex: "pk_relation_tuple",
		},
		{
			name: "filter by resource type and ID uses primary key",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "foo",
				OptionalResourceIds:  []string{"bar"},
			},
			expectedIndex: "pk_relation_tuple",
		},
		{
			name: "filter by subject type uses subject relation index",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "foo",
					},
				},
			},
			expectedIndex: "ix_relation_tuple_by_subject_relation",
		},
		{
			name: "filter by subject type and ID uses subject index",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "foo",
						OptionalSubjectIds:  []string{"bar"},
					},
				},
			},
			expectedIndex: "ix_relation_tuple_by_subject",
		},
		{
			name: "filter by subject type, ID, and relation uses subject index",
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
			expectedIndex: "ix_relation_tuple_by_subject",
		},
		{
			name: "filter with no forced index returns no hint",
			filter: datastore.RelationshipsFilter{
				OptionalResourceRelation: "bar",
			},
			expectedIndex: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index, err := IndexingHintForQueryShape(*schema, queryshape.Varying, &test.filter)
			require.NoError(t, err)

			if test.expectedIndex == "" {
				require.Equal(t, NoIndexingHint, index, "expected no indexing hint for varying shape with this filter")
			} else {
				require.NotEqual(t, NoIndexingHint, index, "expected an indexing hint for varying shape with this filter")
				forcedIdx, ok := index.(forcedIndex)
				require.True(t, ok, "expected a forcedIndex type")
				require.Equal(t, test.expectedIndex, forcedIdx.index.Name)
			}
		})
	}
}
