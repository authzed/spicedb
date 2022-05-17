package caching

import (
	"strings"
	"testing"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

func generateCheckResultEntry(directLen int, ttuLen int, strLen int) checkResultEntry {
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

var (
	emptyEntry    = generateCheckResultEntry(0, 0, 0)
	smallEntry    = generateCheckResultEntry(10, 10, 32)
	mediumEntry   = generateCheckResultEntry(100, 100, 64)
	largeEntry    = generateCheckResultEntry(1000, 1000, 128)
	behemothEntry = generateCheckResultEntry(10000, 10000, 128)
)

func benchmarkCheckResultCost(entry checkResultEntry, b *testing.B) {
	for n := 0; n < b.N; n++ {
		checkResultCost(entry)
	}
}

func BenchmarkEmptyCheckResultEntry(b *testing.B)    { benchmarkCheckResultCost(emptyEntry, b) }
func BenchmarkSmallCheckResultEntry(b *testing.B)    { benchmarkCheckResultCost(smallEntry, b) }
func BenchmarkMediumCheckResultEntry(b *testing.B)   { benchmarkCheckResultCost(smallEntry, b) }
func BenchmarkLargeCheckResultEntry(b *testing.B)    { benchmarkCheckResultCost(smallEntry, b) }
func BenchmarkBehemothCheckResultEntry(b *testing.B) { benchmarkCheckResultCost(smallEntry, b) }
