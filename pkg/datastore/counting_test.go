package datastore

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestFilterStableName(t *testing.T) {
	tcs := []struct {
		name     string
		filter   *core.RelationshipFilter
		expected string
	}{
		{
			name:     "empty filter",
			filter:   &core.RelationshipFilter{},
			expected: "f$:#",
		},
		{
			name: "filter with resource type",
			filter: &core.RelationshipFilter{
				ResourceType: "something",
			},
			expected: "f$something:#",
		},
		{
			name: "filter with resource ID",
			filter: &core.RelationshipFilter{
				OptionalResourceId: "something",
			},
			expected: "f$:something#",
		},
		{
			name: "filter with resource ID prefix",
			filter: &core.RelationshipFilter{
				OptionalResourceIdPrefix: "something",
			},
			expected: "f$:something%#",
		},
		{
			name: "filter with relation",
			filter: &core.RelationshipFilter{
				OptionalRelation: "something",
			},
			expected: "f$:#something",
		},
		{
			name: "filter with subject type",
			filter: &core.RelationshipFilter{
				OptionalSubjectFilter: &core.SubjectFilter{
					SubjectType: "something",
				},
			},
			expected: "f$:#@something:",
		},
		{
			name: "filter with subject ID",
			filter: &core.RelationshipFilter{
				OptionalSubjectFilter: &core.SubjectFilter{
					OptionalSubjectId: "something",
				},
			},
			expected: "f$:#@:something",
		},
		{
			name: "filter with subject relation",
			filter: &core.RelationshipFilter{
				OptionalSubjectFilter: &core.SubjectFilter{
					OptionalRelation: &core.SubjectFilter_RelationFilter{
						Relation: "something",
					},
				},
			},
			expected: "f$:#@:#something",
		},
		{
			name: "full filter",
			filter: &core.RelationshipFilter{
				ResourceType:       "resource",
				OptionalResourceId: "resource-id",
				OptionalRelation:   "relation",
				OptionalSubjectFilter: &core.SubjectFilter{
					SubjectType:       "subject",
					OptionalSubjectId: "subject-id",
					OptionalRelation: &core.SubjectFilter_RelationFilter{
						Relation: "subject-relation",
					},
				},
			},
			expected: "f$resource:resource-id#relation@subject:subject-id#subject-relation",
		},
		{
			name: "partial filter with no subject relation",
			filter: &core.RelationshipFilter{
				ResourceType:       "resource",
				OptionalResourceId: "resource-id",
				OptionalRelation:   "relation",
				OptionalSubjectFilter: &core.SubjectFilter{
					SubjectType:       "subject",
					OptionalSubjectId: "subject-id",
				},
			},
			expected: "f$resource:resource-id#relation@subject:subject-id",
		},
		{
			name: "partial filter with no resource ID",
			filter: &core.RelationshipFilter{
				ResourceType:     "resource",
				OptionalRelation: "relation",
				OptionalSubjectFilter: &core.SubjectFilter{
					SubjectType:       "subject",
					OptionalSubjectId: "subject-id",
				},
			},
			expected: "f$resource:#relation@subject:subject-id",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			actual, err := FilterStableName(tc.filter)
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}
}
