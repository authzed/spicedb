package v1

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

func TestConvertCheckTrace_MultipleResourceIds_RequireAllResults(t *testing.T) {
	tests := []struct {
		name           string
		description    string
		results        map[string]*dispatch.ResourceCheckResult
		expectedResult v1.CheckDebugTrace_Permissionship
	}{
		{
			name:        "mixed results",
			description: "fake CheckDebugTrace with multiple resource IDs and mixed results",
			results: map[string]*dispatch.ResourceCheckResult{
				"resource1": {Membership: dispatch.ResourceCheckResult_MEMBER},
				"resource2": {Membership: dispatch.ResourceCheckResult_NOT_MEMBER},
				"resource3": {Membership: dispatch.ResourceCheckResult_MEMBER},
			},
			expectedResult: v1.CheckDebugTrace_PERMISSIONSHIP_UNSPECIFIED,
		},
		{
			name:        "all same results",
			description: "fake CheckDebugTrace with multiple resource IDs where all results are the same",
			results: map[string]*dispatch.ResourceCheckResult{
				"resource1": {Membership: dispatch.ResourceCheckResult_MEMBER},
				"resource2": {Membership: dispatch.ResourceCheckResult_MEMBER},
				"resource3": {Membership: dispatch.ResourceCheckResult_MEMBER},
			},
			expectedResult: v1.CheckDebugTrace_PERMISSIONSHIP_HAS_PERMISSION,
		},
		{
			name:        "missing results",
			description: "fake CheckDebugTrace with multiple resource IDs where some results are missing",
			results: map[string]*dispatch.ResourceCheckResult{
				"resource1": {Membership: dispatch.ResourceCheckResult_MEMBER},
				"resource3": {Membership: dispatch.ResourceCheckResult_MEMBER},
			},
			expectedResult: v1.CheckDebugTrace_PERMISSIONSHIP_UNSPECIFIED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)

			// Create a fake CheckDebugTrace with multiple resource IDs and REQUIRE_ALL_RESULTS
			ct := &dispatch.CheckDebugTrace{
				Request: &dispatch.DispatchCheckRequest{
					ResourceIds:    []string{"resource1", "resource2", "resource3"},
					ResultsSetting: dispatch.DispatchCheckRequest_REQUIRE_ALL_RESULTS,
					ResourceRelation: &core.RelationReference{
						Namespace: "document",
						Relation:  "view",
					},
					Subject: &core.ObjectAndRelation{
						Namespace: "user",
						ObjectId:  "alice",
						Relation:  "...",
					},
				},
				Results:              tt.results,
				ResourceRelationType: dispatch.CheckDebugTrace_PERMISSION,
			}

			// Convert the trace
			converted, err := convertCheckTrace(t.Context(), nil, ct, nil, nil)
			req.NoError(err)

			// Verify the expected result
			req.Equal(tt.expectedResult, converted.Result)
			req.Equal("resource1,resource2,resource3", converted.Resource.ObjectId)
		})
	}
}
