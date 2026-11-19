// Package resultsize estimates completed iterator path counts at one revision.
package resultsize

import (
	"errors"
	"math/rand/v2"
	"time"

	"github.com/caio/go-tdigest/v4"

	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

// Name identifies the provider for completed raw path-count distributions.
const Name signal.Name = "result-size-at-revision"

var (

	// Ref provides typed access to completed raw path-count distributions.
	Ref = signal.Reference[signal.ExecutionKey, Estimate]{Name: Name}

	// Input provides typed publication of observations for completed raw path-count distributions.
	Input = signal.Input[signal.ExecutionKey, Observation]{Name: Name}
)

// Observation records a raw path count and invocation completion status.
type Observation struct {
	// Update identifies the accepted source increment for derived-provider deduplication.
	Update signal.UpdateID

	// Count counts raw paths produced before API deduplication or projection.
	Count uint64

	// Status describes how the observed invocation ended.
	Status signal.Terminal

	// Started is the invocation's original start time.
	Started time.Time
}

// Estimate summarizes completed raw path counts, optionally with aged weights.
type Estimate struct {
	// Samples counts observations represented by the estimate.
	Samples uint64

	// Mean is the weighted mean raw path count.
	Mean float64

	// ZeroFraction is the weighted fraction of completed invocations producing no paths.
	ZeroFraction float64

	// Min is the smallest represented raw path count.
	Min float64

	// Max is the largest represented raw path count.
	Max float64

	// Weight is the effective sample weight after aging or discounting.
	Weight float64

	// Distribution is the immutable approximate path-count distribution.
	Distribution signal.Distribution
}

// SignalSamples returns the number of observations represented by the estimate.
func (e Estimate) SignalSamples() uint64 { return e.Samples }

// SignalWeight returns the effective observation weight of the estimate.
func (e Estimate) SignalWeight() float64 { return e.Weight }

type accumulator struct {
	digest        *tdigest.TDigest
	n, zeros      uint64
	sum, min, max float64
}

// NewAccumulator creates an empty result-size accumulator.
func NewAccumulator() signal.Accumulator[Observation, Estimate] {
	return &accumulator{digest: mustDigest()}
}

const digestCompression = 100

// Digest clones share their RNG. The package-level generator is safe when
// independent schema accumulators continue learning on different goroutines.
type sharedRNG struct{}

// Float32 returns a random value from the concurrent package-level generator.
func (sharedRNG) Float32() float32 {
	return rand.Float32() //nolint:gosec // Digest sampling does not require cryptographic randomness.
}

// Intn returns a random index from the concurrent package-level generator.
func (sharedRNG) Intn(n int) int {
	return rand.IntN(n) //nolint:gosec // Digest sampling does not require cryptographic randomness.
}

func mustDigest() *tdigest.TDigest {
	d, err := tdigest.New(
		tdigest.Compression(digestCompression),
		tdigest.RandomNumberGenerator(sharedRNG{}),
	)
	if err != nil {
		panic(err)
	}

	return d
}

// New returns the revision-bound provider descriptor for completed raw path-count distributions.
func New() signal.Descriptor {
	return signal.NewSource(Name, signal.SourceConfig[signal.ExecutionKey, Observation, Estimate]{
		Policy:      signal.RevisionBound,
		Description: "Completed iterator path-count distribution",
		New:         NewAccumulator,
		Stamp: func(o Observation, id signal.UpdateID, at time.Time) Observation {
			o.Update = id

			if o.Started.IsZero() {
				o.Started = at
			}

			return o
		},
		Accept: func(k signal.ExecutionKey, o Observation) bool {
			return k.Operation != signal.Check && o.Status == signal.Complete
		},
		ObservationTime: func(o Observation) time.Time {
			return o.Started
		},
		Footprint: func(k signal.ExecutionKey) signal.Footprint {
			return k.Footprint()
		},
	})
}

// Add incorporates an observation into the accumulator.
func (a *accumulator) Add(o Observation) error {
	if o.Status != signal.Complete {
		return nil
	}

	if o.Count > 1<<53 {
		return errors.New("path count exceeds exact numeric range")
	}

	x := float64(o.Count)
	if err := a.digest.Add(x); err != nil {
		return err
	}

	if a.n == 0 {
		a.min, a.max = x, x
	} else {
		a.min = min(a.min, x)
		a.max = max(a.max, x)
	}

	a.n++

	if o.Count == 0 {
		a.zeros++
	}

	a.sum += x
	return nil
}

// Value returns an independent estimate snapshot at the supplied time.
func (a *accumulator) Value(time.Time) (Estimate, signal.Availability) {
	if a.n == 0 {
		return Estimate{}, signal.Missing
	}

	return Estimate{
		Samples:      a.n,
		Mean:         a.sum / float64(a.n),
		ZeroFraction: float64(a.zeros) / float64(a.n),
		Min:          a.min,
		Max:          a.max,
		Weight:       float64(a.n),
		Distribution: signal.DigestDistribution(a.digest, a.min, a.max),
	}, signal.Available
}

// Size returns the estimated resident size of the accumulator in bytes.
func (a *accumulator) Size() int64 {
	n := int64(0)
	a.digest.ForEachCentroid(func(float64, uint64) bool { n++; return true })
	// go-tdigest v4 reserves two compression*10 slices of eight-byte elements.
	// Centroid growth can retain capacity beyond the live length. Reserve twice
	// that length once it exceeds the initial allocation; compression replaces
	// both buffers with a fresh initial allocation. Clones may need less, so this
	// deliberately remains a conservative bound rather than reporting live data.
	return 256 + 16*max(digestCompression*10, 2*n)
}

// Clone returns an independent copy of the accumulator state.
func (a *accumulator) Clone() signal.Accumulator[Observation, Estimate] {
	b := *a
	b.digest = a.digest.Clone()
	return &b
}
