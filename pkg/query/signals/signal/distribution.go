package signal

import (
	"math"

	"github.com/caio/go-tdigest/v4"
)

// Distribution exposes immutable approximate numeric distribution queries.
type Distribution struct {
	digest   *tdigest.TDigest
	parts    []WeightedDistribution
	min, max float64
}

// WeightedDistribution associates an immutable distribution with a mixture weight.
type WeightedDistribution struct {
	// Distribution is the immutable approximate path-count distribution.
	Distribution Distribution

	// Weight is the distribution's effective observation weight.
	Weight float64
}

// DigestDistribution clones a t-digest into an immutable distribution with observed bounds.
func DigestDistribution(d *tdigest.TDigest, minimum, maximum float64) Distribution {
	return Distribution{digest: d.Clone(), min: minimum, max: maximum}
}

// Mixture constructs an immutable weighted mixture of distributions.
func Mixture(parts []WeightedDistribution, minimum, maximum float64) Distribution {
	return Distribution{parts: append([]WeightedDistribution(nil), parts...), min: minimum, max: maximum}
}

// CDF returns the approximate fraction of samples at or below x.
func (d Distribution) CDF(x float64) float64 {
	if x < d.min {
		return 0
	}

	if x >= d.max {
		return 1
	}

	if d.digest != nil {
		return d.digest.CDF(x)
	}

	total, sum := 0.0, 0.0

	for _, p := range d.parts {
		total += p.Weight
		sum += p.Weight * p.Distribution.CDF(x)
	}

	if total == 0 {
		return math.NaN()
	}

	return sum / total
}

// Quantile returns the approximate value at q, or NaN for an invalid quantile.
func (d Distribution) Quantile(q float64) float64 {
	if q < 0 || q > 1 || math.IsNaN(q) {
		return math.NaN()
	}

	if q == 0 {
		return d.min
	}

	if q == 1 {
		return d.max
	}

	if d.digest != nil {
		return d.digest.Quantile(q)
	}

	low, high := d.min, d.max

	for range 64 {
		mid := low + (high-low)/2
		if d.CDF(mid) < q {
			low = mid
		} else {
			high = mid
		}
	}

	return low + (high-low)/2
}
