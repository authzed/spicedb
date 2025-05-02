package v1

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/middleware/perfinsights"
)

func TestLabelsForFilter(t *testing.T) {
	tcs := []struct {
		name     string
		filter   *v1.RelationshipFilter
		expected perfinsights.APIShapeLabels
	}{
		{
			name:     "no filter",
			filter:   nil,
			expected: perfinsights.NoLabels(),
		},
		{
			name:   "empty filter",
			filter: &v1.RelationshipFilter{},
			expected: perfinsights.APIShapeLabels{
				perfinsights.ResourceTypeLabel:     "",
				perfinsights.ResourceRelationLabel: "",
			},
		},
		{
			name: "resource type and relation",
			filter: &v1.RelationshipFilter{
				ResourceType:     "foo",
				OptionalRelation: "bar",
			},
			expected: perfinsights.APIShapeLabels{
				perfinsights.ResourceTypeLabel:     "foo",
				perfinsights.ResourceRelationLabel: "bar",
			},
		},
		{
			name: "resource type only",
			filter: &v1.RelationshipFilter{
				ResourceType: "foo",
			},
			expected: perfinsights.APIShapeLabels{
				perfinsights.ResourceTypeLabel:     "foo",
				perfinsights.ResourceRelationLabel: "",
			},
		},
		{
			name: "relation only",
			filter: &v1.RelationshipFilter{
				OptionalRelation: "bar",
			},
			expected: perfinsights.APIShapeLabels{
				perfinsights.ResourceTypeLabel:     "",
				perfinsights.ResourceRelationLabel: "bar",
			},
		},
		{
			name: "subject type and relation",
			filter: &v1.RelationshipFilter{
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType: "foo",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "bar",
					},
				},
			},
			expected: perfinsights.APIShapeLabels{
				perfinsights.ResourceTypeLabel:     "",
				perfinsights.ResourceRelationLabel: "",
				perfinsights.SubjectTypeLabel:      "foo",
				perfinsights.SubjectRelationLabel:  "bar",
			},
		},
		{
			name: "subject type only",
			filter: &v1.RelationshipFilter{
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType: "foo",
				},
			},
			expected: perfinsights.APIShapeLabels{
				perfinsights.ResourceTypeLabel:     "",
				perfinsights.ResourceRelationLabel: "",
				perfinsights.SubjectTypeLabel:      "foo",
			},
		},
		{
			name: "subject type and empty relation",
			filter: &v1.RelationshipFilter{
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType: "foo",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "",
					},
				},
			},
			expected: perfinsights.APIShapeLabels{
				perfinsights.ResourceTypeLabel:     "",
				perfinsights.ResourceRelationLabel: "",
				perfinsights.SubjectTypeLabel:      "foo",
				perfinsights.SubjectRelationLabel:  "",
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			labels := labelsForFilter(tc.filter)
			require.Equal(t, tc.expected, labels)
		})
	}
}
