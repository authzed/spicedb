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

type rar struct {
	request        *dispatchv1.DispatchCheckRequest
	expectedResult []string
	expectedError  string
}

func TestRunCheckDispatchExpr(t *testing.T) {
	tcs := []struct {
		name                string
		expr                string
		requestAndResponses []rar
	}{
		{
			"static",
			"['prewarm']",
			[]rar{
				{
					nil,
					[]string{"prewarm"},
					"",
				},
			},
		},
		{
			"basic",
			"request.resource_relation.namespace == 'somenamespace' ? ['prewarm'] : ['other']",
			[]rar{
				{
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
					&dispatchv1.DispatchCheckRequest{
						ResourceRelation: &corev1.RelationReference{
							Namespace: "anothernamespace",
							Relation:  "somerelation",
						},
					},
					[]string{"other"},
					"",
				},
			},
		},
		{
			"basic other branch",
			"request.resource_relation.namespace == 'somethingelse' ? ['prewarm'] : ['other']",
			[]rar{
				{
					&dispatchv1.DispatchCheckRequest{
						ResourceRelation: &corev1.RelationReference{
							Namespace: "somenamespace",
							Relation:  "somerelation",
						},
					},
					[]string{"other"},
					"",
				},
			},
		},
		{
			"invalid field",
			"request.resource_relation.invalidfield == 'somethingelse' ? ['prewarm'] : ['other']",
			[]rar{
				{
					&dispatchv1.DispatchCheckRequest{
						ResourceRelation: &corev1.RelationReference{
							Namespace: "somenamespace",
							Relation:  "somerelation",
						},
					},
					nil,
					"no such field 'invalidfield'",
				},
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseDispatchExpression("check", tc.expr)
			require.NoError(t, err)

			for _, rar := range tc.requestAndResponses {
				resp, err := RunDispatchExpr(parsed, rar.request)
				if rar.expectedError != "" {
					require.ErrorContains(t, err, rar.expectedError)
				} else {
					require.Equal(t, rar.expectedResult, resp)
				}

				// Run again and ensure the same result.
				resp, err = RunDispatchExpr(parsed, rar.request)
				if rar.expectedError != "" {
					require.ErrorContains(t, err, rar.expectedError)
				} else {
					require.Equal(t, rar.expectedResult, resp)
				}
			}
		})
	}
}

func BenchmarkRunDispatchExpression(b *testing.B) {
	req := &dispatchv1.DispatchCheckRequest{
		ResourceRelation: &corev1.RelationReference{
			Namespace: "somenamespace",
			Relation:  "somerelation",
		},
	}

	parsed, err := ParseDispatchExpression("check", "['tiger']")
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = RunDispatchExpr(parsed, req)
	}
}
