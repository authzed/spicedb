package v1

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
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
			"cf1d767007d04edc",
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
			"3aecab574a273b45",
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
			"67a8f4c53250f14a",
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
			"63d0ca03f244fc44",
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
			"b26e38746b7dd856",
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
			"77f27e2d566ebfbe",
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
			"cbaa1004df7bd467",
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
			"1afac87f34a901c4",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			verr := tc.request.Validate()
			require.NoError(t, verr)

			hash, err := computeLRRequestHash(tc.request)
			require.NoError(t, err)
			require.Equal(t, tc.expectedHash, hash)
		})
	}
}

func TestCheckBulkPermissionsItemWithoutResourceIDHashStability(t *testing.T) {
	tcs := []struct {
		name         string
		request      *v1.CheckBulkPermissionsRequestItem
		expectedHash string
	}{
		{
			"basic bulk check item",
			&v1.CheckBulkPermissionsRequestItem{
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
			"1bb4418571995a32",
		},
		{
			"different resource ID, should still be the same hash",
			&v1.CheckBulkPermissionsRequestItem{
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
			"1bb4418571995a32",
		},
		{
			"basic bulk check item - transcribed letter",
			&v1.CheckBulkPermissionsRequestItem{
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
			"aa035599b6525b1d",
		},
		{
			"different resource type",
			&v1.CheckBulkPermissionsRequestItem{
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
			"3cedf7fb7805705d",
		},
		{
			"different permission",
			&v1.CheckBulkPermissionsRequestItem{
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
			"4497a1419274b340",
		},
		{
			"different subject type",
			&v1.CheckBulkPermissionsRequestItem{
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
			"89c6106d825c579a",
		},
		{
			"different subject id",
			&v1.CheckBulkPermissionsRequestItem{
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
			"af0b171b98c325aa",
		},
		{
			"different subject relation",
			&v1.CheckBulkPermissionsRequestItem{
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
			"624b6989fcd41287",
		},
		{
			"with context",
			&v1.CheckBulkPermissionsRequestItem{
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
			"b52787f4dcfda909",
		},
		{
			"with different context",
			&v1.CheckBulkPermissionsRequestItem{
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
			"01890cb311887418",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			verr := tc.request.Validate()
			require.NoError(t, verr)

			hash, err := computeCheckBulkPermissionsItemHashWithoutResourceID(tc.request)
			require.NoError(t, err)
			require.Equal(t, tc.expectedHash, hash)
		})
	}
}

func TestCheckBulkPermissionsItemWIDHashStability(t *testing.T) {
	tcs := []struct {
		name         string
		request      *v1.CheckBulkPermissionsRequestItem
		expectedHash string
	}{
		{
			"basic bulk check item",
			&v1.CheckBulkPermissionsRequestItem{
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
			"afdf81991b9fea82",
		},
		{
			"different resource ID, should be a different hash",
			&v1.CheckBulkPermissionsRequestItem{
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
			"aef33bb5fef05e09",
		},
		{
			"basic bulk check item - transcribed letter",
			&v1.CheckBulkPermissionsRequestItem{
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
			"b46fcb0d87f7878a",
		},
		{
			"different resource type",
			&v1.CheckBulkPermissionsRequestItem{
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
			"eeec3e95226dc10b",
		},
		{
			"different permission",
			&v1.CheckBulkPermissionsRequestItem{
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
			"4470347cbcd50f28",
		},
		{
			"different subject type",
			&v1.CheckBulkPermissionsRequestItem{
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
			"4c8a61948de1f861",
		},
		{
			"different subject id",
			&v1.CheckBulkPermissionsRequestItem{
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
			"15390b3ad12e998a",
		},
		{
			"different subject relation",
			&v1.CheckBulkPermissionsRequestItem{
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
			"beeba065b897035f",
		},
		{
			"with context",
			&v1.CheckBulkPermissionsRequestItem{
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
			"a2379564a6a20756",
		},
		{
			"with different context",
			&v1.CheckBulkPermissionsRequestItem{
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
			"779beae0e00a7427",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			verr := tc.request.Validate()
			require.NoError(t, verr)

			hash, err := computeCheckBulkPermissionsItemHash(tc.request)
			require.NoError(t, err)
			require.Equal(t, tc.expectedHash, hash)
		})
	}
}
