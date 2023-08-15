package v1

import (
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"
)

func TestReadRelationshipsHashStability(t *testing.T) {
	tcs := []struct {
		name         string
		request      *v1.ReadRelationshipsRequest
		expectedHash string
	}{
		{
			"basic read",
			&v1.ReadRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType: "someresourcetype",
				},
			},
			"0a0e721527f9e3b2",
		},
		{
			"full read",
			&v1.ReadRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:       "someresourcetype",
					OptionalResourceId: "foo",
					OptionalRelation:   "somerelation",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       "somesubjectype",
						OptionalSubjectId: "somesubject",
						OptionalRelation: &v1.SubjectFilter_RelationFilter{
							Relation: "subrelation",
						},
					},
				},
				OptionalLimit: 1000,
			},
			"0375e86e0c72f281",
		},
		{
			"different resource type",
			&v1.ReadRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:       "someresourcetype2",
					OptionalResourceId: "foo",
					OptionalRelation:   "somerelation",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       "somesubjectype",
						OptionalSubjectId: "somesubject",
						OptionalRelation: &v1.SubjectFilter_RelationFilter{
							Relation: "subrelation",
						},
					},
				},
				OptionalLimit: 1000,
			},
			"a7d3548f8303f0ba",
		},
		{
			"different resource id",
			&v1.ReadRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:       "someresourcetype",
					OptionalResourceId: "foo2",
					OptionalRelation:   "somerelation",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       "somesubjectype",
						OptionalSubjectId: "somesubject",
						OptionalRelation: &v1.SubjectFilter_RelationFilter{
							Relation: "subrelation",
						},
					},
				},
				OptionalLimit: 1000,
			},
			"77c9a81be9232973",
		},
		{
			"different resource relation",
			&v1.ReadRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:       "someresourcetype",
					OptionalResourceId: "foo",
					OptionalRelation:   "somerelation2",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       "somesubjectype",
						OptionalSubjectId: "somesubject",
						OptionalRelation: &v1.SubjectFilter_RelationFilter{
							Relation: "subrelation",
						},
					},
				},
				OptionalLimit: 1000,
			},
			"765b640f81f98ff5",
		},
		{
			"no subject filter",
			&v1.ReadRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:       "someresourcetype",
					OptionalResourceId: "foo",
					OptionalRelation:   "somerelation",
				},
				OptionalLimit: 1000,
			},
			"42abbaaaf9d6cb12",
		},
		{
			"different subject type",
			&v1.ReadRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:       "someresourcetype",
					OptionalResourceId: "foo",
					OptionalRelation:   "somerelation",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       "somesubjectype2",
						OptionalSubjectId: "somesubject",
						OptionalRelation: &v1.SubjectFilter_RelationFilter{
							Relation: "subrelation",
						},
					},
				},
				OptionalLimit: 1000,
			},
			"55b37fccd04ae2df",
		},
		{
			"different subject id",
			&v1.ReadRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:       "someresourcetype",
					OptionalResourceId: "foo",
					OptionalRelation:   "somerelation",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       "somesubjectype",
						OptionalSubjectId: "somesubject2",
						OptionalRelation: &v1.SubjectFilter_RelationFilter{
							Relation: "subrelation",
						},
					},
				},
				OptionalLimit: 1000,
			},
			"0ba2e49c26fb8ae1",
		},
		{
			"different subject relation",
			&v1.ReadRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:       "someresourcetype",
					OptionalResourceId: "foo",
					OptionalRelation:   "somerelation",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       "somesubjectype",
						OptionalSubjectId: "somesubject",
						OptionalRelation: &v1.SubjectFilter_RelationFilter{
							Relation: "subrelation2",
						},
					},
				},
				OptionalLimit: 1000,
			},
			"73e715ccd9ea2225",
		},
		{
			"different limit",
			&v1.ReadRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:       "someresourcetype",
					OptionalResourceId: "foo",
					OptionalRelation:   "somerelation",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       "somesubjectype",
						OptionalSubjectId: "somesubject",
						OptionalRelation: &v1.SubjectFilter_RelationFilter{
							Relation: "subrelation",
						},
					},
				},
				OptionalLimit: 999,
			},
			"7e7fdc450bf08327",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			verr := tc.request.Validate()
			require.NoError(t, verr)

			hash, err := computeReadRelationshipsRequestHash(tc.request)
			require.NoError(t, err)
			require.Equal(t, tc.expectedHash, hash)
		})
	}
}

func TestLRHashStability(t *testing.T) {
	tcs := []struct {
		name         string
		request      *v1.LookupResourcesRequest
		expectedHash string
	}{
		{
			"basic LR",
			&v1.LookupResourcesRequest{
				ResourceObjectType: "resource",
				Permission:         "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "tom",
					},
				},
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_MinimizeLatency{
						MinimizeLatency: true,
					},
				},
				OptionalLimit: 1000,
			},
			"f5c7ca6296253717",
		},
		{
			"different LR subject",
			&v1.LookupResourcesRequest{
				ResourceObjectType: "resource",
				Permission:         "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "sarah",
					},
				},
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_MinimizeLatency{
						MinimizeLatency: true,
					},
				},
				OptionalLimit: 1000,
			},
			"aa8b67b886ecf3fd",
		},
		{
			"different LR resource",
			&v1.LookupResourcesRequest{
				ResourceObjectType: "resource2",
				Permission:         "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "tom",
					},
				},
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_MinimizeLatency{
						MinimizeLatency: true,
					},
				},
				OptionalLimit: 1000,
			},
			"fb16e4dd9395864a",
		},
		{
			"different LR resource permission",
			&v1.LookupResourcesRequest{
				ResourceObjectType: "resource",
				Permission:         "viewer",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "tom",
					},
				},
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_MinimizeLatency{
						MinimizeLatency: true,
					},
				},
				OptionalLimit: 1000,
			},
			"593e60bf77f8bdb4",
		},
		{
			"different limit LR",
			&v1.LookupResourcesRequest{
				ResourceObjectType: "resource",
				Permission:         "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "sarah",
					},
				},
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_MinimizeLatency{
						MinimizeLatency: true,
					},
				},
				OptionalLimit: 999,
			},
			"0097cf2ee303ec31",
		},
		{
			"LR with different consistency",
			&v1.LookupResourcesRequest{
				ResourceObjectType: "resource",
				Permission:         "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "tom",
					},
				},
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_FullyConsistent{
						FullyConsistent: true,
					},
				},
				OptionalLimit: 1000,
			},
			"d8b707db35cb7043",
		},
		{
			"basic LR with caveat context",
			&v1.LookupResourcesRequest{
				ResourceObjectType: "resource",
				Permission:         "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "tom",
					},
				},
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_MinimizeLatency{
						MinimizeLatency: true,
					},
				},
				OptionalLimit: 1000,
				Context: func() *structpb.Struct {
					s, _ := structpb.NewStruct(map[string]any{
						"somecondition":    42,
						"anothercondition": "hello world",
					})
					return s
				}(),
			},
			"d40193c84ec59e6f",
		},
		{
			"basic LR with different caveat context",
			&v1.LookupResourcesRequest{
				ResourceObjectType: "resource",
				Permission:         "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "tom",
					},
				},
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_MinimizeLatency{
						MinimizeLatency: true,
					},
				},
				OptionalLimit: 1000,
				Context: func() *structpb.Struct {
					s, _ := structpb.NewStruct(map[string]any{
						"somecondition":    43,
						"anothercondition": "hello world",
					})
					return s
				}(),
			},
			"a5af756163998c88",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			verr := tc.request.Validate()
			require.NoError(t, verr)

			hash, err := computeLRRequestHash(tc.request)
			require.NoError(t, err)
			require.Equal(t, tc.expectedHash, hash)
		})
	}
}

func TestBulkCheckPermissionItemWithoutResourceIDHashStability(t *testing.T) {
	tcs := []struct {
		name         string
		request      *v1.BulkCheckPermissionRequestItem
		expectedHash string
	}{
		{
			"basic bulk check item",
			&v1.BulkCheckPermissionRequestItem{
				Resource: &v1.ObjectReference{
					ObjectType: "resource",
					ObjectId:   "someid",
				},
				Permission: "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "tom",
					},
				},
			},
			"4fe0404757f60c7b",
		},
		{
			"different resource ID, should still be the same hash",
			&v1.BulkCheckPermissionRequestItem{
				Resource: &v1.ObjectReference{
					ObjectType: "resource",
					ObjectId:   "someid2",
				},
				Permission: "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "tom",
					},
				},
			},
			"4fe0404757f60c7b",
		},
		{
			"basic bulk check item - transcribed letter",
			&v1.BulkCheckPermissionRequestItem{
				Resource: &v1.ObjectReference{
					ObjectType: "resourc",
					ObjectId:   "esomeid",
				},
				Permission: "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "tom",
					},
				},
			},
			"9aa5ad05571cf7aa",
		},
		{
			"different resource type",
			&v1.BulkCheckPermissionRequestItem{
				Resource: &v1.ObjectReference{
					ObjectType: "resource2",
					ObjectId:   "someid",
				},
				Permission: "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "tom",
					},
				},
			},
			"7e48b1946d209d19",
		},
		{
			"different permission",
			&v1.BulkCheckPermissionRequestItem{
				Resource: &v1.ObjectReference{
					ObjectType: "resource",
					ObjectId:   "someid",
				},
				Permission: "view2",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "tom",
					},
				},
			},
			"55001a108b0e1857",
		},
		{
			"different subject type",
			&v1.BulkCheckPermissionRequestItem{
				Resource: &v1.ObjectReference{
					ObjectType: "resource",
					ObjectId:   "someid",
				},
				Permission: "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user2",
						ObjectId:   "tom",
					},
				},
			},
			"f913ec03a38259ff",
		},
		{
			"different subject id",
			&v1.BulkCheckPermissionRequestItem{
				Resource: &v1.ObjectReference{
					ObjectType: "resource",
					ObjectId:   "someid",
				},
				Permission: "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "tom2",
					},
				},
			},
			"70ff03c2e3f8598c",
		},
		{
			"different subject relation",
			&v1.BulkCheckPermissionRequestItem{
				Resource: &v1.ObjectReference{
					ObjectType: "resource",
					ObjectId:   "someid",
				},
				Permission: "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "tom",
					},
					OptionalRelation: "foo",
				},
			},
			"0bd57e3aa9d48e1e",
		},
		{
			"with context",
			&v1.BulkCheckPermissionRequestItem{
				Resource: &v1.ObjectReference{
					ObjectType: "resource",
					ObjectId:   "someid",
				},
				Permission: "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "tom",
					},
					OptionalRelation: "foo",
				},
				Context: func() *structpb.Struct {
					s, _ := structpb.NewStruct(map[string]any{
						"somecondition":    42,
						"anothercondition": "hello world",
					})
					return s
				}(),
			},
			"f7ae307d940c4984",
		},
		{
			"with different context",
			&v1.BulkCheckPermissionRequestItem{
				Resource: &v1.ObjectReference{
					ObjectType: "resource",
					ObjectId:   "someid",
				},
				Permission: "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "tom",
					},
					OptionalRelation: "foo",
				},
				Context: func() *structpb.Struct {
					s, _ := structpb.NewStruct(map[string]any{
						"somecondition":    42,
						"anothercondition": "hi there",
					})
					return s
				}(),
			},
			"d0ce3e8b8a7b8b5a",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			verr := tc.request.Validate()
			require.NoError(t, verr)

			hash, err := computeBulkCheckPermissionItemHashWithoutResourceID(tc.request)
			require.NoError(t, err)
			require.Equal(t, tc.expectedHash, hash)
		})
	}
}
