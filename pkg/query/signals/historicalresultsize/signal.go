// Package historicalresultsize derives aged path-count estimates across revisions.
package historicalresultsize

import (
	"errors"
	"math"
	"time"

	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

// Name identifies the provider for aged raw path-count distributions across revisions.
const Name signal.Name = "historical-result-size"

// Ref provides typed access to aged raw path-count distributions across revisions.
var Ref = signal.Reference[signal.ExecutionKey, resultsize.Estimate]{Name: Name}

const (
	width     = 10 * time.Second
	retention = 5 * time.Minute
	halfLife  = time.Minute
)

type bucketKey struct {
	window     int64
	generation uint64
}

type bucket struct {
	state  signal.Accumulator[resultsize.Observation, resultsize.Estimate]
	factor float64
}

type watermark struct {
	sequence uint64
	at       time.Time
}

// Options configures the historical provider's bucketing and aging.
type Options struct {
	// BucketWidth is the duration of each observation-time bucket.
	BucketWidth time.Duration

	// Retention is the maximum age of retained observations.
	Retention time.Duration

	// HalfLife is the duration over which evidence weight halves.
	HalfLife time.Duration
}

func (o Options) defaults() (Options, error) {
	if o.BucketWidth == 0 {
		o.BucketWidth = width
	}

	if o.Retention == 0 {
		o.Retention = retention
	}

	if o.HalfLife == 0 {
		o.HalfLife = halfLife
	}

	if o.BucketWidth < 0 || o.Retention < o.BucketWidth || o.HalfLife < 0 || o.Retention/o.BucketWidth > 1024 {
		return o, errors.New("invalid historical result-size options")
	}

	return o, nil
}

type accumulator struct {
	options    Options
	generation uint64
	now        func() time.Time
	buckets    map[bucketKey]bucket
	seen       map[uint64]watermark
}

// NewAccumulator creates an empty historical accumulator using the supplied clock.
func NewAccumulator(now func() time.Time) signal.Accumulator[resultsize.Observation, resultsize.Estimate] {
	opts, _ := Options{}.defaults()
	return &accumulator{options: opts, now: now, buckets: map[bucketKey]bucket{}, seen: map[uint64]watermark{}}
}

// New returns the cross-revision provider descriptor with default aging options.
func New() signal.Descriptor { return mustDefaultDescriptor() }

func mustDefaultDescriptor() signal.Descriptor {
	d, err := NewWithOptions(Options{})
	if err != nil {
		panic(err)
	}

	return d
}

// NewWithOptions creates a historical result-size provider with validated aging options.
func NewWithOptions(opts Options) (signal.Descriptor, error) {
	opts, err := opts.defaults()
	if err != nil {
		return signal.Descriptor{}, err
	}

	build := func(now func() time.Time) signal.Accumulator[resultsize.Observation, resultsize.Estimate] {
		return &accumulator{options: opts, now: now, buckets: map[bucketKey]bucket{}, seen: map[uint64]watermark{}}
	}
	return signal.NewDerived(Name, resultsize.Input, signal.SourceConfig[signal.ExecutionKey, resultsize.Observation, resultsize.Estimate]{
		Policy:      signal.CrossRevision,
		Description: "Aged completed path-count evidence across revisions",
		New: func() signal.Accumulator[resultsize.Observation, resultsize.Estimate] {
			return build(time.Now)
		},
		Build: build,
		ObservationTime: func(o resultsize.Observation) time.Time {
			return o.Started
		},
		Footprint: func(k signal.ExecutionKey) signal.Footprint {
			return k.Footprint()
		},
		Transfer: signal.Unchanged,
	}), nil
}

func (a *accumulator) prune(now time.Time) {
	for k := range a.buckets {
		if now.Sub(time.Unix(0, k.window*int64(a.options.BucketWidth))) >= a.options.Retention {
			delete(a.buckets, k)
		}
	}

	for k, v := range a.seen {
		if now.Sub(v.at) >= a.options.Retention {
			delete(a.seen, k)
		}
	}
}

// Add incorporates an observation into the accumulator.
func (a *accumulator) Add(o resultsize.Observation) error {
	now := a.now()
	a.prune(now)
	at := o.Started
	if at.IsZero() {
		at = now
	}

	if at.After(now) {
		at = now
	}

	if now.Sub(at) >= a.options.Retention {
		return nil
	}

	if o.Update.Epoch != 0 {
		if w, ok := a.seen[o.Update.Epoch]; ok && o.Update.Sequence <= w.sequence {
			return nil
		}

		latest := at
		if prior, ok := a.seen[o.Update.Epoch]; ok && prior.at.After(latest) {
			latest = prior.at
		}

		a.seen[o.Update.Epoch] = watermark{o.Update.Sequence, latest}
	}

	k := bucketKey{window: at.UnixNano() / int64(a.options.BucketWidth), generation: a.generation}
	b, ok := a.buckets[k]
	if !ok {
		b = bucket{state: resultsize.NewAccumulator(), factor: 1}
	}

	if err := b.state.Add(o); err != nil {
		return err
	}

	a.buckets[k] = b
	return nil
}

// Value returns an independent estimate snapshot at the supplied time.
func (a *accumulator) Value(now time.Time) (resultsize.Estimate, signal.Availability) {
	a.prune(now)
	e := resultsize.Estimate{}
	parts := []signal.WeightedDistribution{}
	sum, zero := 0.0, 0.0

	for k, b := range a.buckets {
		v, status := b.state.Value(now)
		if status != signal.Available {
			continue
		}

		mid := time.Unix(0, k.window*int64(a.options.BucketWidth)).Add(a.options.BucketWidth / 2)
		age := max(now.Sub(mid), time.Duration(0))
		factor := math.Exp2(-float64(age)/float64(a.options.HalfLife)) * b.factor
		weight := factor * float64(v.Samples)

		if len(parts) == 0 {
			e.Min, e.Max = v.Min, v.Max
		} else {
			e.Min = min(e.Min, v.Min)
			e.Max = max(e.Max, v.Max)
		}

		parts = append(parts, signal.WeightedDistribution{Distribution: v.Distribution, Weight: weight})
		e.Samples += v.Samples
		e.Weight += weight
		sum += weight * v.Mean
		zero += weight * v.ZeroFraction
	}

	if e.Weight < 1 {
		return resultsize.Estimate{}, signal.Expired
	}

	e.Mean = sum / e.Weight
	e.ZeroFraction = zero / e.Weight
	e.Distribution = signal.Mixture(parts, e.Min, e.Max)
	return e, signal.Available
}

// Size returns the estimated resident size of the accumulator in bytes.
func (a *accumulator) Size() int64 {
	size := int64(128 + len(a.seen)*64)

	for _, b := range a.buckets {
		size += 64 + b.state.Size()
	}

	return size
}

// Clone returns an independent copy of the accumulator state.
func (a *accumulator) Clone() signal.Accumulator[resultsize.Observation, resultsize.Estimate] {
	b := &accumulator{
		generation: a.generation,
		options:    a.options,
		now:        a.now,
		buckets:    map[bucketKey]bucket{},
		seen:       map[uint64]watermark{},
	}

	for k, v := range a.buckets {
		b.buckets[k] = bucket{state: v.state.Clone(), factor: v.factor}
	}

	for k, v := range a.seen {
		b.seen[k] = v
	}

	return b
}

// DiscountEvidence reduces existing evidence weight without renewing its age.
func (a *accumulator) DiscountEvidence(f float64) {
	a.generation++

	for k, b := range a.buckets {
		b.factor *= f
		a.buckets[k] = b
	}
}
