package v1

import (
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"
)

func TestHashStability(t *testing.T) {
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
