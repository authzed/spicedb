package datastore

import (
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"
)

func TestRelationshipsFilterFromPublicFilter(t *testing.T) {
	tests := []struct {
		name     string
		input    *v1.RelationshipFilter
		expected RelationshipsFilter
	}{
		{
			"only resource name",
			&v1.RelationshipFilter{ResourceType: "sometype"},
			RelationshipsFilter{ResourceType: "sometype"},
		},
		{
			"resource name and id",
			&v1.RelationshipFilter{ResourceType: "sometype", OptionalResourceId: "someid"},
			RelationshipsFilter{ResourceType: "sometype", OptionalResourceIds: []string{"someid"}},
		},
		{
			"resource name and relation",
			&v1.RelationshipFilter{ResourceType: "sometype", OptionalRelation: "somerel"},
			RelationshipsFilter{ResourceType: "sometype", OptionalResourceRelation: "somerel"},
		},
		{
			"resource and subject",
			&v1.RelationshipFilter{ResourceType: "sometype", OptionalSubjectFilter: &v1.SubjectFilter{SubjectType: "someothertype"}},
			RelationshipsFilter{ResourceType: "sometype", OptionalSubjectsSelectors: []SubjectsSelector{
				{
					OptionalSubjectType: "someothertype",
				},
			}},
		},
		{
			"resource and subject with optional relation",
			&v1.RelationshipFilter{
				ResourceType: "sometype",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:      "someothertype",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{Relation: "somerel"},
				},
			},
			RelationshipsFilter{ResourceType: "sometype", OptionalSubjectsSelectors: []SubjectsSelector{
				{
					OptionalSubjectType: "someothertype",
					RelationFilter:      SubjectRelationFilter{}.WithNonEllipsisRelation("somerel"),
				},
			}},
		},
		{
			"resource and subject with ellipsis relation",
			&v1.RelationshipFilter{
				ResourceType: "sometype",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:      "someothertype",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{Relation: ""},
				},
			},
			RelationshipsFilter{ResourceType: "sometype", OptionalSubjectsSelectors: []SubjectsSelector{
				{
					OptionalSubjectType: "someothertype",
					RelationFilter:      SubjectRelationFilter{}.WithEllipsisRelation(),
				},
			}},
		},
		{
			"full",
			&v1.RelationshipFilter{
				ResourceType:       "sometype",
				OptionalResourceId: "someid",
				OptionalRelation:   "somerel",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:       "someothertype",
					OptionalSubjectId: "somesubjectid",
					OptionalRelation:  &v1.SubjectFilter_RelationFilter{Relation: ""},
				},
			},
			RelationshipsFilter{
				ResourceType:             "sometype",
				OptionalResourceIds:      []string{"someid"},
				OptionalResourceRelation: "somerel",
				OptionalSubjectsSelectors: []SubjectsSelector{
					{
						OptionalSubjectType: "someothertype",
						OptionalSubjectIds:  []string{"somesubjectid"},
						RelationFilter:      SubjectRelationFilter{}.WithEllipsisRelation(),
					},
				},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			computed := RelationshipsFilterFromPublicFilter(test.input)
			require.Equal(t, test.expected, computed)
		})
	}
}
