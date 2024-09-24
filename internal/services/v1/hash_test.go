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
			"f518629690bd9dc0",
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
			"f518629690bd9dc0",
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
			"60f1e177297e915e",
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
			"5117abaee3adf638",
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
			"716f7be27e600292",
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
			"7cb5945314ccbdce",
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
			"b24ecacf87fd0bb8",
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
			"ee8c34ab206c80d7",
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
			"7a5b1fec3cbed446",
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
			"f17da513a6207c30",
		},
	}

	for _, tc := range tcs {
		tc := tc
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
			"5edbb3bbb8079754",
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
			"e6711064500e65ba",
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
			"8cda00b7188572b7",
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
			"51df43a69e51d3b0",
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
			"62aaa50b2821130d",
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
			"82a445d3ffc0823a",
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
			"d3d624a310fa7781",
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
			"a9d96f0572caef89",
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
			"94dea3fccff039ed",
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
			"7ffdedbe12d578ee",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			verr := tc.request.Validate()
			require.NoError(t, verr)

			hash, err := computeCheckBulkPermissionsItemHash(tc.request)
			require.NoError(t, err)
			require.Equal(t, tc.expectedHash, hash)
		})
	}
}
