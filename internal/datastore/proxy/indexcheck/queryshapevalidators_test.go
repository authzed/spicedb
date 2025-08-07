package indexcheck

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
)

func TestValidateQueryShape(t *testing.T) {
	tests := []struct {
		name        string
		shape       queryshape.Shape
		filter      datastore.RelationshipsFilter
		expectError bool
		errorMsg    string
	}{
		{
			name:  "CheckPermissionSelectDirectSubjects - valid",
			shape: queryshape.CheckPermissionSelectDirectSubjects,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"user1"},
					},
				},
			},
			expectError: false,
		},
		{
			name:  "CheckPermissionSelectDirectSubjects - missing resource type",
			shape: queryshape.CheckPermissionSelectDirectSubjects,
			filter: datastore.RelationshipsFilter{
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"user1"},
					},
				},
			},
			expectError: true,
			errorMsg:    "optional resource type required",
		},
		{
			name:  "CheckPermissionSelectDirectSubjects - missing resource ids",
			shape: queryshape.CheckPermissionSelectDirectSubjects,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"user1"},
					},
				},
			},
			expectError: true,
			errorMsg:    "optional resource ids required",
		},
		{
			name:  "CheckPermissionSelectDirectSubjects - has resource id prefix",
			shape: queryshape.CheckPermissionSelectDirectSubjects,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:      "document",
				OptionalResourceIds:       []string{"doc1"},
				OptionalResourceIDPrefix:  "prefix",
				OptionalResourceRelation:  "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{{}},
			},
			expectError: true,
			errorMsg:    "no optional resource id prefixes allowed",
		},
		{
			name:  "CheckPermissionSelectDirectSubjects - missing resource relation",
			shape: queryshape.CheckPermissionSelectDirectSubjects,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
				OptionalResourceIds:  []string{"doc1"},
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"user1"},
					},
				},
			},
			expectError: true,
			errorMsg:    "optional resource relation required",
		},
		{
			name:  "CheckPermissionSelectDirectSubjects - missing subjects selectors",
			shape: queryshape.CheckPermissionSelectDirectSubjects,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
			},
			expectError: true,
			errorMsg:    "optional subjects selectors required",
		},
		{
			name:  "CheckPermissionSelectDirectSubjects - missing subject type",
			shape: queryshape.CheckPermissionSelectDirectSubjects,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectIds: []string{"user1"},
					},
				},
			},
			expectError: true,
			errorMsg:    "optional subject type required",
		},
		{
			name:  "CheckPermissionSelectDirectSubjects - missing subject ids",
			shape: queryshape.CheckPermissionSelectDirectSubjects,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
					},
				},
			},
			expectError: true,
			errorMsg:    "optional subject ids required",
		},
		{
			name:  "CheckPermissionSelectDirectSubjects - with caveats",
			shape: queryshape.CheckPermissionSelectDirectSubjects,
			filter: datastore.RelationshipsFilter{
				OptionalCaveatNameFilter: datastore.CaveatNameFilter{
					Option: datastore.CaveatFilterOptionNoCaveat,
				},
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"user1"},
					},
				},
			},
			expectError: true,
			errorMsg:    "optional caveats not supported",
		},
		{
			name:  "CheckPermissionSelectIndirectSubjects - valid",
			shape: queryshape.CheckPermissionSelectIndirectSubjects,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
							IncludeEllipsisRelation:  false,
						},
					},
				},
			},
			expectError: false,
		},
		{
			name:  "CheckPermissionSelectIndirectSubjects - with subject type",
			shape: queryshape.CheckPermissionSelectIndirectSubjects,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "no optional subject type allowed",
		},
		{
			name:  "CheckPermissionSelectIndirectSubjects - with subject ids",
			shape: queryshape.CheckPermissionSelectIndirectSubjects,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectIds: []string{"user1"},
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "no optional subject ids allowed",
		},
		{
			name:  "CheckPermissionSelectIndirectSubjects - empty relation filter",
			shape: queryshape.CheckPermissionSelectIndirectSubjects,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						RelationFilter: datastore.SubjectRelationFilter{},
					},
				},
			},
			expectError: true,
			errorMsg:    "relation filter required",
		},
		{
			name:  "CheckPermissionSelectIndirectSubjects - ellipsis relations allowed",
			shape: queryshape.CheckPermissionSelectIndirectSubjects,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: false,
							IncludeEllipsisRelation:  true,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "only non-ellipsis relations allowed",
		},
		{
			name:  "AllSubjectsForResources - valid",
			shape: queryshape.AllSubjectsForResources,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
			},
			expectError: false,
		},
		{
			name:  "AllSubjectsForResources - with subjects selectors",
			shape: queryshape.AllSubjectsForResources,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
					},
				},
			},
			expectError: true,
			errorMsg:    "no optional subjects selectors allowed",
		},
		{
			name:  "FindResourceOfType - valid",
			shape: queryshape.FindResourceOfType,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
			},
			expectError: false,
		},
		{
			name:  "FindResourceOfType - with resource ids",
			shape: queryshape.FindResourceOfType,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
				OptionalResourceIds:  []string{"doc1"},
			},
			expectError: true,
			errorMsg:    "no optional resource ids allowed",
		},
		{
			name:  "FindResourceOfType - with resource relation",
			shape: queryshape.FindResourceOfType,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceRelation: "viewer",
			},
			expectError: true,
			errorMsg:    "no optional resource relation allowed",
		},
		{
			name:  "FindResourceOfType - with subjects selectors",
			shape: queryshape.FindResourceOfType,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
					},
				},
			},
			expectError: true,
			errorMsg:    "no optional subjects selectors allowed",
		},
		{
			name:        "Varying - always valid",
			shape:       queryshape.Varying,
			filter:      datastore.RelationshipsFilter{},
			expectError: false,
		},
		{
			name:  "FindResourceAndSubjectWithRelations - valid",
			shape: queryshape.FindResourceAndSubjectWithRelations,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: false,
		},
		{
			name:  "FindResourceAndSubjectWithRelations - missing resource type",
			shape: queryshape.FindResourceAndSubjectWithRelations,
			filter: datastore.RelationshipsFilter{
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "optional resource type required",
		},
		{
			name:  "FindResourceAndSubjectWithRelations - with resource ids",
			shape: queryshape.FindResourceAndSubjectWithRelations,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "no optional resource ids allowed",
		},
		{
			name:  "FindResourceAndSubjectWithRelations - missing resource relation",
			shape: queryshape.FindResourceAndSubjectWithRelations,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "optional resource relation required",
		},
		{
			name:  "FindResourceAndSubjectWithRelations - missing subjects selectors",
			shape: queryshape.FindResourceAndSubjectWithRelations,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceRelation: "viewer",
			},
			expectError: true,
			errorMsg:    "optional subjects selectors required",
		},
		{
			name:  "FindResourceAndSubjectWithRelations - multiple subjects selectors",
			shape: queryshape.FindResourceAndSubjectWithRelations,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
					{
						OptionalSubjectType: "group",
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "exactly one subjects selector required",
		},
		{
			name:  "FindResourceAndSubjectWithRelations - missing subject type",
			shape: queryshape.FindResourceAndSubjectWithRelations,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "subject type required",
		},
		{
			name:  "FindResourceAndSubjectWithRelations - with subject ids",
			shape: queryshape.FindResourceAndSubjectWithRelations,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"user1"},
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "no optional subject ids allowed",
		},
		{
			name:  "FindResourceAndSubjectWithRelations - missing subject relation",
			shape: queryshape.FindResourceAndSubjectWithRelations,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter:      datastore.SubjectRelationFilter{},
					},
				},
			},
			expectError: true,
			errorMsg:    "subject relation required",
		},
		{
			name:  "FindSubjectOfTypeAndRelation - valid",
			shape: queryshape.FindSubjectOfTypeAndRelation,
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: false,
		},
		{
			name:  "FindSubjectOfTypeAndRelation - with resource type",
			shape: queryshape.FindSubjectOfTypeAndRelation,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "no optional resource type allowed",
		},
		{
			name:  "FindSubjectOfTypeAndRelation - with resource ids",
			shape: queryshape.FindSubjectOfTypeAndRelation,
			filter: datastore.RelationshipsFilter{
				OptionalResourceIds: []string{"doc1"},
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "no optional resource ids allowed",
		},
		{
			name:  "FindSubjectOfTypeAndRelation - with resource relation",
			shape: queryshape.FindSubjectOfTypeAndRelation,
			filter: datastore.RelationshipsFilter{
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "no optional resource relation allowed",
		},
		{
			name:        "FindSubjectOfTypeAndRelation - missing subjects selectors",
			shape:       queryshape.FindSubjectOfTypeAndRelation,
			filter:      datastore.RelationshipsFilter{},
			expectError: true,
			errorMsg:    "optional subjects selectors required",
		},
		{
			name:  "FindSubjectOfTypeAndRelation - multiple subjects selectors",
			shape: queryshape.FindSubjectOfTypeAndRelation,
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
					{
						OptionalSubjectType: "group",
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "exactly one subjects selector required",
		},
		{
			name:  "FindSubjectOfTypeAndRelation - missing subject type",
			shape: queryshape.FindSubjectOfTypeAndRelation,
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "subject type required",
		},
		{
			name:  "FindSubjectOfTypeAndRelation - with subject ids",
			shape: queryshape.FindSubjectOfTypeAndRelation,
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"user1"},
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "no optional subject ids allowed",
		},
		{
			name:  "FindSubjectOfTypeAndRelation - missing subject relation",
			shape: queryshape.FindSubjectOfTypeAndRelation,
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter:      datastore.SubjectRelationFilter{},
					},
				},
			},
			expectError: true,
			errorMsg:    "subject relation required",
		},
		{
			name:  "FindResourceRelationForSubjectRelation - valid",
			shape: queryshape.FindResourceRelationForSubjectRelation,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: false,
		},
		{
			name:  "FindResourceRelationForSubjectRelation - missing resource type",
			shape: queryshape.FindResourceRelationForSubjectRelation,
			filter: datastore.RelationshipsFilter{
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "optional resource type required",
		},
		{
			name:  "FindResourceRelationForSubjectRelation - missing resource relation",
			shape: queryshape.FindResourceRelationForSubjectRelation,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "optional resource relation required",
		},
		{
			name:  "FindResourceRelationForSubjectRelation - missing subjects selectors",
			shape: queryshape.FindResourceRelationForSubjectRelation,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceRelation: "viewer",
			},
			expectError: true,
			errorMsg:    "optional subjects selectors required",
		},
		{
			name:  "FindResourceRelationForSubjectRelation - multiple subjects selectors",
			shape: queryshape.FindResourceRelationForSubjectRelation,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
					{
						OptionalSubjectType: "group",
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "exactly one subjects selector required",
		},
		{
			name:  "FindResourceRelationForSubjectRelation - missing subject type",
			shape: queryshape.FindResourceRelationForSubjectRelation,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						RelationFilter: datastore.SubjectRelationFilter{
							OnlyNonEllipsisRelations: true,
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "subject type required",
		},
		{
			name:  "FindResourceRelationForSubjectRelation - missing subject relation",
			shape: queryshape.FindResourceRelationForSubjectRelation,
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter:      datastore.SubjectRelationFilter{},
					},
				},
			},
			expectError: true,
			errorMsg:    "subject relation required",
		},
		{
			name:        "Unknown shape - error",
			shape:       "unknown-shape",
			filter:      datastore.RelationshipsFilter{},
			expectError: true,
			errorMsg:    "unsupported query shape",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateQueryShape(tt.shape, tt.filter)
			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateReverseQueryShape(t *testing.T) {
	tests := []struct {
		name        string
		shape       queryshape.Shape
		filter      datastore.SubjectsFilter
		opts        *options.ReverseQueryOptions
		expectError bool
		errorMsg    string
	}{
		{
			name:  "MatchingResourcesForSubject - valid",
			shape: queryshape.MatchingResourcesForSubject,
			filter: datastore.SubjectsFilter{
				SubjectType:        "user",
				OptionalSubjectIds: []string{"user1"},
			},
			opts: &options.ReverseQueryOptions{
				ResRelation: &options.ResourceRelation{
					Namespace: "document",
					Relation:  "viewer",
				},
			},
			expectError: false,
		},
		{
			name:  "MatchingResourcesForSubject - missing subject type",
			shape: queryshape.MatchingResourcesForSubject,
			filter: datastore.SubjectsFilter{
				OptionalSubjectIds: []string{"user1"},
			},
			opts: &options.ReverseQueryOptions{
				ResRelation: &options.ResourceRelation{
					Namespace: "document",
					Relation:  "viewer",
				},
			},
			expectError: true,
			errorMsg:    "subject type required",
		},
		{
			name:  "MatchingResourcesForSubject - missing subject ids",
			shape: queryshape.MatchingResourcesForSubject,
			filter: datastore.SubjectsFilter{
				SubjectType: "user",
			},
			opts: &options.ReverseQueryOptions{
				ResRelation: &options.ResourceRelation{
					Namespace: "document",
					Relation:  "viewer",
				},
			},
			expectError: true,
			errorMsg:    "subject ids required",
		},
		{
			name:  "MatchingResourcesForSubject - missing resource relation",
			shape: queryshape.MatchingResourcesForSubject,
			filter: datastore.SubjectsFilter{
				SubjectType:        "user",
				OptionalSubjectIds: []string{"user1"},
			},
			opts:        &options.ReverseQueryOptions{},
			expectError: true,
			errorMsg:    "resource relation required",
		},
		{
			name:  "MatchingResourcesForSubject - missing resource relation namespace",
			shape: queryshape.MatchingResourcesForSubject,
			filter: datastore.SubjectsFilter{
				SubjectType:        "user",
				OptionalSubjectIds: []string{"user1"},
			},
			opts: &options.ReverseQueryOptions{
				ResRelation: &options.ResourceRelation{
					Relation: "viewer",
				},
			},
			expectError: true,
			errorMsg:    "resource relation namespace required",
		},
		{
			name:  "MatchingResourcesForSubject - missing resource relation name",
			shape: queryshape.MatchingResourcesForSubject,
			filter: datastore.SubjectsFilter{
				SubjectType:        "user",
				OptionalSubjectIds: []string{"user1"},
			},
			opts: &options.ReverseQueryOptions{
				ResRelation: &options.ResourceRelation{
					Namespace: "document",
				},
			},
			expectError: true,
			errorMsg:    "resource relation required",
		},
		{
			name:        "Varying - always valid",
			shape:       queryshape.Varying,
			filter:      datastore.SubjectsFilter{},
			opts:        &options.ReverseQueryOptions{},
			expectError: false,
		},
		{
			name:        "Unknown shape - error",
			shape:       "unknown-shape",
			filter:      datastore.SubjectsFilter{},
			opts:        &options.ReverseQueryOptions{},
			expectError: true,
			errorMsg:    "unsupported query shape",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateReverseQueryShape(tt.shape, tt.filter, tt.opts)
			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
