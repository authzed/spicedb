// Package contextfield predicts request-context field stability at one revision.
package contextfield

import (
	"time"

	"github.com/ccoveille/go-safecast/v2"

	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

// Name identifies the provider for request-context field stability.
const Name signal.Name = "context-field-stability"

var (

	// Ref provides typed access to request-context field stability.
	Ref = signal.Reference[string, Estimate]{Name: Name}

	// Input provides typed publication of observations for request-context field stability.
	Input = signal.Input[string, Observation]{Name: Name}
)

// Observation records whether a context field is present and its value fingerprint.
type Observation struct {
	// Present reports whether the context field was supplied.
	Present bool

	// Fingerprint identifies the canonical value without retaining its contents.
	Fingerprint string
}

// Estimate summarizes observed value stability for one context field.
type Estimate struct {
	// Samples counts observations represented by the estimate.
	Samples uint64

	// Missing counts observations where the field was absent.
	Missing uint64

	// Overflow counts present values omitted after the fingerprint budget was exhausted.
	Overflow uint64

	// AllEqual reports whether at least one present value was seen and all were identical.
	AllEqual bool

	// DominantShareLowerBound bounds the most frequent value's share among present observations.
	DominantShareLowerBound float64
}

type accumulator struct {
	samples, missing, overflow uint64
	frequencies                map[string]uint64
	first                      string
	different                  bool
}

// New returns the revision-bound provider descriptor for request-context field stability.
func New() signal.Descriptor {
	return signal.NewSource(Name, signal.SourceConfig[string, Observation, Estimate]{
		Policy:      signal.RevisionBound,
		Description: "Request-context value stability by field name",
		New: func() signal.Accumulator[Observation, Estimate] {
			return &accumulator{frequencies: map[string]uint64{}}
		},
	})
}

// Add incorporates an observation into the accumulator.
func (a *accumulator) Add(o Observation) error {
	a.samples++

	if !o.Present {
		a.missing++
		return nil
	}

	present := a.samples - a.missing
	if present == 1 {
		a.first = o.Fingerprint
	} else if a.first != o.Fingerprint {
		a.different = true
	}

	if _, ok := a.frequencies[o.Fingerprint]; ok || len(a.frequencies) < 16 {
		a.frequencies[o.Fingerprint]++
	} else {
		a.overflow++
		a.different = true
	}

	return nil
}

// Value returns an independent estimate snapshot at the supplied time.
func (a *accumulator) Value(time.Time) (Estimate, signal.Availability) {
	present := a.samples - a.missing
	maxCount := uint64(0)

	for _, n := range a.frequencies {
		maxCount = max(maxCount, n)
	}

	share := 0.0

	if present > 0 {
		share = float64(maxCount) / float64(present)
	}

	return Estimate{
		Samples:                 a.samples,
		Missing:                 a.missing,
		Overflow:                a.overflow,
		AllEqual:                present > 0 && !a.different,
		DominantShareLowerBound: share,
	}, signal.Available
}

// Size returns the estimated resident size of the accumulator in bytes.
func (a *accumulator) Size() int64 {
	size := int64(64)

	for k := range a.frequencies {
		size += 64 + safecast.MustConvert[int64](len(k))
	}

	return size
}

// Clone returns an independent copy of the accumulator state.
func (a *accumulator) Clone() signal.Accumulator[Observation, Estimate] {
	b := *a
	b.frequencies = map[string]uint64{}

	for k, n := range a.frequencies {
		b.frequencies[k] = n
	}

	return &b
}
