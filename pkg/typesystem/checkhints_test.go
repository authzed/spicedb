package typesystem

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestCheckHints(t *testing.T) {
	tcs := []struct {
		name               string
		hintString         string
		expectedParsedHint *ParsedCheckHint
		expectedError      string
	}{
		{
			name:          "empty",
			hintString:    "",
			expectedError: "could not parse check hint: \"\"",
		},
		{
			name:          "invalid",
			hintString:    "invalid",
			expectedError: "could not parse check hint: \"invalid\"",
		},
		{
			name:          "invalid resource",
			hintString:    "invalid:#viewer@user:fred",
			expectedError: "could not parse check hint: \"invalid:#viewer@user:fred\"",
		},
		{
			name:          "invalid subject",
			hintString:    ResourceCheckHintForRelation("type", "id", "relation") + "@" + "invalid",
			expectedError: "could not parse check hint: \"type:id#relation@invalid\"",
		},
		{
			name:       "valid relation terminal subject",
			hintString: ResourceCheckHintForRelation("type", "id", "relation") + "@user:fred",
			expectedParsedHint: &ParsedCheckHint{
				Type: CheckHintTypeRelation,
				Resource: &core.ObjectAndRelation{
					Namespace: "type",
					ObjectId:  "id",
					Relation:  "relation",
				},
				Subject: &core.ObjectAndRelation{
					Namespace: "user",
					ObjectId:  "fred",
					Relation:  "...",
				},
			},
		},
		{
			name:       "valid relation non terminal subject",
			hintString: ResourceCheckHintForRelation("type", "id", "relation") + "@user:fred#viewer",
			expectedParsedHint: &ParsedCheckHint{
				Type: CheckHintTypeRelation,
				Resource: &core.ObjectAndRelation{
					Namespace: "type",
					ObjectId:  "id",
					Relation:  "relation",
				},
				Subject: &core.ObjectAndRelation{
					Namespace: "user",
					ObjectId:  "fred",
					Relation:  "viewer",
				},
			},
		},
		{
			name:       "valid arrow terminal subject",
			hintString: ResourceCheckHintForArrow("type", "id", "relation", "computed") + "@user:fred",
			expectedParsedHint: &ParsedCheckHint{
				Type: CheckHintTypeArrow,
				Resource: &core.ObjectAndRelation{
					Namespace: "type",
					ObjectId:  "id",
					Relation:  "relation",
				},
				Subject: &core.ObjectAndRelation{
					Namespace: "user",
					ObjectId:  "fred",
					Relation:  "...",
				},
				ArrowComputedUsersetRelation: "computed",
			},
		},
		{
			name:       "valid arrow non terminal subject",
			hintString: ResourceCheckHintForArrow("type", "id", "relation", "computed") + "@user:fred#viewer",
			expectedParsedHint: &ParsedCheckHint{
				Type: CheckHintTypeArrow,
				Resource: &core.ObjectAndRelation{
					Namespace: "type",
					ObjectId:  "id",
					Relation:  "relation",
				},
				Subject: &core.ObjectAndRelation{
					Namespace: "user",
					ObjectId:  "fred",
					Relation:  "viewer",
				},
				ArrowComputedUsersetRelation: "computed",
			},
		},
		{
			name:          "invalid arrow",
			hintString:    ResourceCheckHintForArrow("type", "id", "relation", "computed") + "->bar@user:fred",
			expectedError: "invalid number of resources in hint: \"type:id#relation->computed->bar@user:fred\"",
		},
		{
			name:          "invalid arrow resource",
			hintString:    ResourceCheckHintForArrow("type", "id$", "relation", "computed") + "@user:fred",
			expectedError: "could not parse portion \"type:id$#relation\" of check hint: \"type:id$#relation->computed@user:fred\"",
		},
		{
			name:          "invalid arrow relation",
			hintString:    ResourceCheckHintForArrow("type", "id", "relation$", "computed") + "@user:fred",
			expectedError: "could not parse portion \"type:id#relation$\" of check hint: \"type:id#relation$->computed@user:fred\"",
		},
		{
			name:          "invalid resource ID",
			hintString:    ResourceCheckHintForRelation("type", "id$", "relation") + "@user:fred",
			expectedError: "could not parse check hint: \"type:id$#relation@user:fred\"",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsedHint, err := ParseCheckHint(tc.hintString)
			if tc.expectedError != "" {
				require.EqualError(t, err, tc.expectedError)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectedParsedHint, parsedHint)
			require.Equal(t, tc.hintString, parsedHint.AsHintString())
		})
	}
}
