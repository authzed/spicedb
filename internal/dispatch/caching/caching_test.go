package caching

import (
	"strings"
	"testing"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

func generateCheckResultEntry(directLen int, ttuLen int) checkResultEntry {
	strLen := 23
	entry := checkResultEntry{
		response: &dispatchv1.DispatchCheckResponse{
			Metadata: &dispatchv1.ResponseMeta{
				LookupExcludedDirect: make([]*corev1.RelationReference, directLen),
				LookupExcludedTtu:    make([]*corev1.RelationReference, ttuLen),
			},
		},
	}

	for i := 0; i < directLen; i++ {
		entry.response.Metadata.LookupExcludedDirect[i] = &corev1.RelationReference{
			Namespace: strings.Repeat("a", strLen),
			Relation:  strings.Repeat("b", strLen),
		}
	}

	for i := 0; i < ttuLen; i++ {
		entry.response.Metadata.LookupExcludedTtu[i] = &corev1.RelationReference{
			Namespace: strings.Repeat("c", strLen),
			Relation:  strings.Repeat("d", strLen),
		}
	}

	return entry
}

func benchmarkCheckResultCost(b *testing.B, costCalc func(checkResultEntry) int64) {
	cases := []struct {
		name  string
		entry checkResultEntry
	}{
		{"    0", generateCheckResultEntry(0, 0)},
		{"   10", generateCheckResultEntry(10, 10)},
		{"  100", generateCheckResultEntry(100, 100)},
		{" 1000", generateCheckResultEntry(1000, 1000)},
		{"10000", generateCheckResultEntry(10000, 10000)},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				costCalc(c.entry)
			}
		})
	}
}

func BenchmarkSizeOf(b *testing.B) {
	benchmarkCheckResultCost(b, checkResultCostSizeOf)
}

func BenchmarkEstimateV1(b *testing.B) {
	benchmarkCheckResultCost(b, checkResultCostEstimateV1)
}

func BenchmarkEstimateV2(b *testing.B) {
	benchmarkCheckResultCost(b, checkResultCostEstimateV2)
}

func BenchmarkAllocCost(b *testing.B) {
	cases := []struct {
		name  string
		scale int
	}{
		{"    0", 0},
		{"   10", 10},
		{"  100", 100},
		{" 1000", 1000},
		{"10000", 10000},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				generateCheckResultEntry(c.scale, c.scale)
			}
		})
	}
}

func logCheckResultAccuracy(t *testing.T, costCalc func(checkResultEntry) int64) {
	cases := []struct {
		name  string
		scale int
	}{
		{"    0", 0},
		{"   10", 10},
		{"  100", 100},
		{" 1000", 1000},
		{"10000", 10000},
	}

	for _, c := range cases {
		item := generateCheckResultEntry(c.scale, c.scale)
		cost := costCalc(item)
		t.Logf("Cost of '%v': '%v'", c.scale, cost)
	}
}

func Test_LogSizeOf(t *testing.T) {
	logCheckResultAccuracy(t, checkResultCostSizeOf)
}

func Test_LogEstimateV1(t *testing.T) {
	logCheckResultAccuracy(t, checkResultCostEstimateV1)
}

func Test_LogEstimateV2(t *testing.T) {
	logCheckResultAccuracy(t, checkResultCostEstimateV2)
}
