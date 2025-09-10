package graph

import (
	"fmt"
	"iter"
	"strings"

	cter "github.com/authzed/spicedb/internal/cursorediterator"
	"github.com/authzed/spicedb/pkg/schema"
)

const cdfThreshold = 0.1

// estimatedConcurrencyLimit calculates the estimated concurrency limit based on the
// cumulative distribution function (CDF) of the digest for the given item.
func estimatedConcurrencyLimit[K UniquelyKeyed](
	crr *CursoredLookupResources3,
	refs lr3refs,
	item K,
	fn func(computedConcurrencyLimit uint16) iter.Seq2[result, error],
) iter.Seq2[result, error] {
	// If the optional limit is set to 0, then all results are returned, and therefore
	// use the maximum concurrency limit that is available.
	if refs.req.OptionalLimit == 0 {
		return fn(refs.concurrencyLimit)
	}

	// If the concurrency limit is set to 1, then we always use that limit.
	if refs.concurrencyLimit == 1 {
		return fn(1)
	}

	// Otherwise, we use the digest (if any) to determine the estimated number of results
	// that will be returned from the current item. If the digest is not available, we default
	// to a limit of 2 to avoid overwhelming the system.
	uniqueKey, err := item.UniqueKey()
	if err != nil {
		return cter.YieldsError[result](fmt.Errorf("failed to get unique key for item: %w", err))
	}

	var concurrencyLimit uint16 = 2
	cdf, ok := crr.digestMap.CDF(uniqueKey, float64(refs.req.OptionalLimit))
	if ok {
		// CDF is the fraction in which all samples are less than or equal to the necessary limit
		// of results. Therefore, if the CDF is less than or equal to the threshold (10%), we assume
		// that most invocations will return the full limit of results, and therefore we limit the
		// concurrency to 1 to avoid extra work being performed.
		if cdf <= cdfThreshold {
			concurrencyLimit = 1
		}
	}

	return cter.CountingIterator(fn(concurrencyLimit), func(count int) {
		crr.digestMap.Add(uniqueKey, float64(count))
	})
}

// UniquelyKeyed is an interface that requires implementing a UniqueKey method.
type UniquelyKeyed interface {
	// UniqueKey returns a unique key for the item, which is used to track concurrency limits.
	UniqueKey() (string, error)
}

type keyedEntrypoints []schema.ReachabilityEntrypoint

func (ke keyedEntrypoints) UniqueKey() (string, error) {
	var sb strings.Builder
	sb.WriteString("entrypoints:")
	for _, entrypoint := range ke {
		hashKey, err := entrypoint.HashKey()
		if err != nil {
			return "", fmt.Errorf("failed to get hash key for entrypoint: %w", err)
		}
		sb.WriteString(hashKey)
		sb.WriteString(";")
	}
	return sb.String(), nil
}

type keyedEntrypoint schema.ReachabilityEntrypoint

func (ke keyedEntrypoint) UniqueKey() (string, error) {
	return schema.ReachabilityEntrypoint(ke).HashKey()
}
