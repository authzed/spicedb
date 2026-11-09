package remote

import (
	"testing"
	"time"

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
		{
			"hedged",
			"hedged(['prewarm'])",
			"",
		},
		{
			"hedged with empty list",
			"hedged([])",
			"",
		},
		{
			"hedged with maximum delay",
			"hedged(['prewarm'], duration('5ms'))",
			"",
		},
		{
			"prefer_secondary",
			"prefer_secondary(['prewarm'], duration('50ms'))",
			"",
		},
		{
			"strategies in both branches",
			"request.resource_relation.namespace == 'foo' ? prefer_secondary(['prewarm'], duration('50ms')) : hedged(['prewarm'], duration('2ms'))",
			"",
		},
		{
			"unknown strategy",
			"hedgd(['prewarm'])",
			"undeclared reference to 'hedgd'",
		},
		{
			"strategy in one branch only",
			"request.resource_relation.namespace == 'foo' ? hedged(['prewarm']) : ['prewarm']",
			"no matching overload",
		},
		{
			"hedged with a string",
			"hedged('prewarm')",
			"no matching overload",
		},
		{
			"hedged with a non-duration delay",
			"hedged(['prewarm'], 5)",
			"no matching overload",
		},
		{
			"prefer_secondary without deadline",
			"prefer_secondary(['prewarm'])",
			"no matching overload",
		},
	}

	for _, tc := range tcs {
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
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseDispatchExpression("check", tc.expr)
			require.NoError(t, err)

			for _, rar := range tc.requestAndResponses {
				resp, err := runDispatchExpr(parsed, rar.request)
				if rar.expectedError != "" {
					require.ErrorContains(t, err, rar.expectedError)
				} else {
					require.Equal(t, rar.expectedResult, resp.secondaries)
					require.Equal(t, hedgedStrategy{}, resp.strategy)
				}

				// Run again and ensure the same result.
				resp, err = runDispatchExpr(parsed, rar.request)
				if rar.expectedError != "" {
					require.ErrorContains(t, err, rar.expectedError)
				} else {
					require.Equal(t, rar.expectedResult, resp.secondaries)
					require.Equal(t, hedgedStrategy{}, resp.strategy)
				}
			}
		})
	}
}

func TestRunDispatchExprStrategies(t *testing.T) {
	checkRequest := func(namespace string) *dispatchv1.DispatchCheckRequest {
		return &dispatchv1.DispatchCheckRequest{
			ResourceRelation: &corev1.RelationReference{Namespace: namespace, Relation: "view"},
		}
	}

	for _, tc := range []struct {
		name          string
		expr          string
		request       *dispatchv1.DispatchCheckRequest
		expected      dispatchPlan
		expectedError string
	}{
		{
			name:     "plain list is hedged",
			expr:     "['tiger']",
			expected: dispatchPlan{secondaries: []string{"tiger"}, strategy: hedgedStrategy{}},
		},
		{
			name:     "hedged",
			expr:     "hedged(['tiger'])",
			expected: dispatchPlan{secondaries: []string{"tiger"}, strategy: hedgedStrategy{}},
		},
		{
			name:     "hedged with empty list",
			expr:     "hedged([])",
			expected: dispatchPlan{secondaries: []string{}, strategy: hedgedStrategy{}},
		},
		{
			name:     "hedged with maximum delay",
			expr:     "hedged(['tiger'], duration('5ms'))",
			expected: dispatchPlan{secondaries: []string{"tiger"}, strategy: hedgedStrategy{maxDelay: 5 * time.Millisecond}},
		},
		{
			name:          "hedged with zero delay",
			expr:          "hedged(['tiger'], duration('0s'))",
			expectedError: "maximum delay must be positive",
		},
		{
			name:     "prefer_secondary",
			expr:     "prefer_secondary(['tiger', 'lion'], duration('50ms'))",
			expected: dispatchPlan{secondaries: []string{"tiger", "lion"}, strategy: preferSecondaryStrategy{deadline: 50 * time.Millisecond}},
		},
		{
			name:          "prefer_secondary with zero deadline",
			expr:          "prefer_secondary(['tiger'], duration('0s'))",
			expectedError: "deadline must be positive",
		},
		{
			name:          "prefer_secondary with negative deadline",
			expr:          "prefer_secondary(['tiger'], duration('-1s'))",
			expectedError: "deadline must be positive",
		},
		{
			name:     "strategy chosen by request, first branch",
			expr:     "request.resource_relation.namespace == 'document' ? prefer_secondary(['tiger'], duration('50ms')) : hedged(['tiger'])",
			request:  checkRequest("document"),
			expected: dispatchPlan{secondaries: []string{"tiger"}, strategy: preferSecondaryStrategy{deadline: 50 * time.Millisecond}},
		},
		{
			name:     "strategy chosen by request, second branch",
			expr:     "request.resource_relation.namespace == 'document' ? prefer_secondary(['tiger'], duration('50ms')) : hedged(['tiger'])",
			request:  checkRequest("folder"),
			expected: dispatchPlan{secondaries: []string{"tiger"}, strategy: hedgedStrategy{}},
		},
		{
			// A realistic expression must stay under the expression cost limit.
			name: "realistic expression within cost limit",
			expr: "request.resource_relation.namespace == 'document' && request.resource_relation.relation in ['view', 'edit'] " +
				"? prefer_secondary(['tiger'], duration('50ms')) " +
				": (request.resource_relation.namespace == 'folder' ? hedged(['tiger'], duration('2ms')) : hedged([]))",
			request:  checkRequest("document"),
			expected: dispatchPlan{secondaries: []string{"tiger"}, strategy: preferSecondaryStrategy{deadline: 50 * time.Millisecond}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseDispatchExpression("check", tc.expr)
			require.NoError(t, err)

			plan, err := runDispatchExpr(parsed, tc.request)
			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, plan)
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

	for _, expr := range []string{
		"['tiger']",
		"hedged(['tiger'])",
		"prefer_secondary(['tiger'], duration('50ms'))",
	} {
		b.Run(expr, func(b *testing.B) {
			parsed, err := ParseDispatchExpression("check", expr)
			require.NoError(b, err)

			b.ResetTimer()
			for b.Loop() {
				_, _ = runDispatchExpr(parsed, req)
			}
		})
	}
}
