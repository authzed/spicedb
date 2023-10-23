package remote

import (
	"testing"

	"github.com/stretchr/testify/require"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

func TestParseDispatchExpression(t *testing.T) {
	tcs := []struct {
		name          string
		expr          string
		expectedError string
	}{
		{
			"empty",
			"",
			"mismatched input '<EOF>'",
		},
		{
			"returns string",
			"'somestring'",
			"a list[string] value",
		},
		{
			"invalid expression",
			"a.b.c!d",
			"mismatched input '!'",
		},
		{
			"valid expression",
			"['prewarm']",
			"",
		},
		{
			"valid big expression",
			"request.resource_relation.namespace == 'foo' ? ['prewarm'] : []",
			"",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseDispatchExpression("somemethod", tc.expr)
			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRunCheckDispatchExpr(t *testing.T) {
	tcs := []struct {
		name           string
		expr           string
		request        *dispatchv1.DispatchCheckRequest
		expectedResult []string
		expectedError  string
	}{
		{
			"static",
			"['prewarm']",
			nil,
			[]string{"prewarm"},
			"",
		},
		{
			"basic",
			"request.resource_relation.namespace == 'somenamespace' ? ['prewarm'] : ['other']",
			&dispatchv1.DispatchCheckRequest{
				ResourceRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
			},
			[]string{"prewarm"},
			"",
		},
		{
			"basic other branch",
			"request.resource_relation.namespace == 'somethingelse' ? ['prewarm'] : ['other']",
			&dispatchv1.DispatchCheckRequest{
				ResourceRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
			},
			[]string{"other"},
			"",
		},
		{
			"invalid field",
			"request.resource_relation.invalidfield == 'somethingelse' ? ['prewarm'] : ['other']",
			&dispatchv1.DispatchCheckRequest{
				ResourceRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
			},
			nil,
			"no such field 'invalidfield'",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseDispatchExpression("check", tc.expr)
			require.NoError(t, err)

			resp, err := RunDispatchExpr(parsed, tc.request)
			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
			} else {
				require.Equal(t, tc.expectedResult, resp)
			}
		})
	}
}
