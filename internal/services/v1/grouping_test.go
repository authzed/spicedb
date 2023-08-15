package v1

import (
	"context"
	"math"
	"sort"
	"strings"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/graph/computed"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/tuple"
)

type expectedGroupedRequest struct {
	resourceType string
	resourceRel  string
	subject      string
	resourceIDs  []string
}

func TestGroupItems(t *testing.T) {
	testCases := []struct {
		name      string
		requests  []string
		groupings []expectedGroupedRequest
		err       string
	}{
		{
			name: "different subjects cannot be grouped",
			requests: []string{
				"document:1#view@user:1",
				"document:1#view@user:2",
				"document:1#view@user:3",
			},
			groupings: []expectedGroupedRequest{
				{
					resourceType: "document",
					resourceRel:  "view",
					subject:      "user:1",
					resourceIDs:  []string{"1"},
				},
				{
					resourceType: "document",
					resourceRel:  "view",
					subject:      "user:2",
					resourceIDs:  []string{"1"},
				},
				{
					resourceType: "document",
					resourceRel:  "view",
					subject:      "user:3",
					resourceIDs:  []string{"1"},
				},
			},
		},
		{
			name: "different permissions cannot be grouped",
			requests: []string{
				"document:1#view@user:1",
				"document:1#write@user:1",
				"document:1#admin@user:1",
			},
			groupings: []expectedGroupedRequest{
				{
					resourceType: "document",
					resourceRel:  "admin",
					subject:      "user:1",
					resourceIDs:  []string{"1"},
				},
				{
					resourceType: "document",
					resourceRel:  "view",
					subject:      "user:1",
					resourceIDs:  []string{"1"},
				},
				{
					resourceType: "document",
					resourceRel:  "write",
					subject:      "user:1",
					resourceIDs:  []string{"1"},
				},
			},
		},
		{
			name: "different resource types cannot be grouped",
			requests: []string{
				"document:1#view@user:1",
				"folder:1#view@user:1",
				"organization:1#view@user:1",
			},
			groupings: []expectedGroupedRequest{
				{
					resourceType: "document",
					resourceRel:  "view",
					subject:      "user:1",
					resourceIDs:  []string{"1"},
				},
				{
					resourceType: "folder",
					resourceRel:  "view",
					subject:      "user:1",
					resourceIDs:  []string{"1"},
				},
				{
					resourceType: "organization",
					resourceRel:  "view",
					subject:      "user:1",
					resourceIDs:  []string{"1"},
				},
			},
		},
		{
			name: "grouping takes place",
			requests: []string{
				"document:3#view@user:2",
				"document:1#view@user:1",
				"document:1#view@user:2",
				"document:2#view@user:1",
				"document:5#view@user:2",
			},
			groupings: []expectedGroupedRequest{
				{
					resourceType: "document",
					resourceRel:  "view",
					subject:      "user:1",
					resourceIDs:  []string{"1", "2"},
				},
				{
					resourceType: "document",
					resourceRel:  "view",
					subject:      "user:2",
					resourceIDs:  []string{"1", "3", "5"},
				},
			},
		},
		{
			name: "different caveat context cannot be grouped",
			requests: []string{
				`document:1#view@user:1[somecaveat:{"hey": "bud"}]`,
				`document:2#view@user:1[somecaveat:{"hi": "there"}]`,
			},
			groupings: []expectedGroupedRequest{
				{
					resourceType: "document",
					resourceRel:  "view",
					subject:      "user:1",
					resourceIDs:  []string{"1"},
				},
				{
					resourceType: "document",
					resourceRel:  "view",
					subject:      "user:1",
					resourceIDs:  []string{"2"},
				},
			},
		},
		{
			name: "same caveat context can be grouped",
			requests: []string{
				`document:1#view@user:1[somecaveat:{"hey": "bud"}]`,
				`document:2#view@user:1[somecaveat:{"hey": "bud"}]`,
			},
			groupings: []expectedGroupedRequest{
				{
					resourceType: "document",
					resourceRel:  "view",
					subject:      "user:1",
					resourceIDs:  []string{"1", "2"},
				},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			var items []*v1.BulkCheckPermissionRequestItem
			for _, r := range tt.requests {
				rel := tuple.ParseRel(r)
				item := &v1.BulkCheckPermissionRequestItem{
					Resource:   rel.Resource,
					Permission: rel.Relation,
					Subject:    rel.Subject,
				}
				if rel.OptionalCaveat != nil {
					item.Context = rel.OptionalCaveat.Context
				}
				items = append(items, item)
			}

			cp := groupingParameters{
				atRevision:           datastore.NoRevision,
				maxCaveatContextSize: math.MaxInt,
				maximumAPIDepth:      1,
			}

			ccp, err := groupItems(context.Background(), cp, items)
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, len(tt.groupings), len(ccp))

				sort.Slice(tt.groupings, func(first, second int) bool {
					// NOTE: This sorting is solely for testing, so it does not need to be secure
					firstParams := tt.groupings[first]
					secondParams := tt.groupings[second]
					firstKey := firstParams.resourceType + firstParams.resourceRel + firstParams.subject
					secondKey := secondParams.resourceType + secondParams.resourceRel + secondParams.subject
					return firstKey < secondKey
				})

				sort.Slice(ccp, func(first, second int) bool {
					// NOTE: This sorting is solely for testing, so it does not need to be secure
					firstParams := ccp[first].params
					secondParams := ccp[second].params

					firstKey := firstParams.ResourceType.Namespace + firstParams.ResourceType.Relation +
						firstParams.Subject.Namespace + firstParams.Subject.ObjectId + firstParams.Subject.Relation + strings.Join(ccp[first].resourceIDs, ",")
					secondKey := secondParams.ResourceType.Namespace + secondParams.ResourceType.Relation +
						secondParams.Subject.Namespace + secondParams.Subject.ObjectId + secondParams.Subject.Relation + strings.Join(ccp[second].resourceIDs, ",")
					return firstKey < secondKey
				})

				for i, expected := range tt.groupings {
					sort.Strings(expected.resourceIDs)
					sort.Strings(ccp[i].resourceIDs)

					require.Equal(t, expected.resourceIDs, ccp[i].resourceIDs)

					require.Equal(t, cp.maximumAPIDepth, ccp[i].params.MaximumDepth)
					require.Equal(t, cp.atRevision, ccp[i].params.AtRevision)
					require.Equal(t, computed.NoDebugging, ccp[i].params.DebugOption)

					err := testutil.AreProtoEqual(tuple.RelationReference(expected.resourceType, expected.resourceRel), ccp[i].params.ResourceType, "resource type diff")
					require.NoError(t, err)

					err = testutil.AreProtoEqual(tuple.ParseSubjectONR(expected.subject), ccp[i].params.Subject, "resource type diff")
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestCaveatContextSizeLimitIsEnforced(t *testing.T) {
	cp := groupingParameters{
		atRevision:           datastore.NoRevision,
		maxCaveatContextSize: 1,
		maximumAPIDepth:      1,
	}
	rel := tuple.ParseRel(`document:1#view@user:1[somecaveat:{"hey": "bud"}]`)
	items := []*v1.BulkCheckPermissionRequestItem{
		{
			Resource:   rel.Resource,
			Permission: rel.Relation,
			Subject:    rel.Subject,
			Context:    rel.OptionalCaveat.Context,
		},
	}
	_, err := groupItems(context.Background(), cp, items)
	require.ErrorContains(t, err, "request caveat context should have less than 1 bytes but had 14")
}
