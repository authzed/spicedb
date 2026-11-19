package signals_test

import (
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/query/signals"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func TestCompositePopulationsRemainIndependent(t *testing.T) {
	type key struct {
		Names [2]string
		Mode  uint8
	}

	d := signal.NewSource[key, int, int]("sum", signal.SourceConfig[key, int, int]{
		New: func() signal.Accumulator[int, int] { return &sum{} },
	})
	o := querysignalstest.Owner(t, d)
	v := querysignalstest.View(t, o, "schema", 1)
	a := key{Names: [2]string{"doc", "view"}, Mode: 1}
	b := a
	b.Mode = 2

	require.NoError(t, signal.Observe(v, signal.Input[key, int]{Name: "sum"}, a, 7))
	e, err := signal.Lookup(v, signal.Reference[key, int]{Name: "sum"}, a)

	require.NoError(t, err)
	require.Equal(t, 7, e.Value)
	e, err = signal.Lookup(v, signal.Reference[key, int]{Name: "sum"}, b)

	require.NoError(t, err)
	require.False(t, e.IsAvailable())
	_, _, _, err = v.Read("sum", []string{"bad"}, reflect.TypeFor[key](), reflect.TypeFor[int]())

	require.ErrorContains(t, err, "key")
}

func TestEvidenceProfileAndLegacyIsolation(t *testing.T) {
	r, err := signals.NewRegistry(definition("sum"))

	require.NoError(t, err)
	require.True(t, r.Contains("sum"))
	require.False(t, r.Contains("missing"))
	_, err = signals.NewOwner(nil, signals.Options{})

	require.Error(t, err)
	_, err = signals.NewOwner(r, signals.Options{MaxSchemas: -1})

	require.Error(t, err)
	_, err = signals.NewOwner(r, signals.Options{Enabled: []string{"missing"}})

	require.Error(t, err)
	disabled, err := signals.NewOwner(r, signals.Options{Enabled: []string{}})

	require.NoError(t, err)
	v := view(t, disabled, "schema", 1)

	require.False(t, v.Enabled("sum"))
	require.Same(t, r, v.Registry())
	require.NoError(t, signal.Observe(v, sumInput, "x", 1))
	e, err := signal.Lookup(v, sumRef, "x")

	require.NoError(t, err)
	require.Equal(t, signal.Unsupported, e.Availability)
	enabled, err := signals.NewOwner(r, signals.Options{Enabled: []string{"sum"}})

	require.NoError(t, err)
	a := enabled.Local(signal.Scope{})
	b := enabled.Local(signal.Scope{})

	require.NoError(t, signal.Observe(a, sumInput, "x", 7))
	e, err = signal.Lookup(b, sumRef, "x")

	require.NoError(t, err)
	require.False(t, e.IsAvailable())
}

func TestStorageBudgetsEvictWholePopulations(t *testing.T) {
	t.Run("schema count", func(t *testing.T) {
		o := owner(t, signals.Options{MaxSchemas: 1})
		a := view(t, o, "a", 1)

		require.NoError(t, signal.Observe(a, sumInput, "x", 1))
		b := view(t, o, "b", 1)

		require.NoError(t, signal.Observe(b, sumInput, "x", 2))
		e, err := signal.Lookup(a, sumRef, "x")

		require.NoError(t, err)
		require.False(t, e.IsAvailable())
	})

	t.Run("total bytes", func(t *testing.T) {
		budget := stored("a").EstimatedSize() + 256 + 320
		o := owner(t, signals.Options{MaxBytes: budget})
		a := view(t, o, "a", 1)

		require.NoError(t, signal.Observe(a, sumInput, "first", 1))
		require.NoError(t, signal.Observe(a, sumInput, "second", 2))
		first, err := signal.Lookup(a, sumRef, "first")

		require.NoError(t, err)
		require.False(t, first.IsAvailable())
		second, err := signal.Lookup(a, sumRef, "second")

		require.NoError(t, err)
		require.Equal(t, 2, second.Value)
		require.LessOrEqual(t, o.ResidentBytes(), budget)
	})

	t.Run("oversized schema", func(t *testing.T) {
		o := owner(t, signals.Options{MaxBytes: 1})
		a := view(t, o, "a", 1)

		require.NoError(t, signal.Observe(a, sumInput, "x", 1))
		e, err := signal.Lookup(a, sumRef, "x")

		require.NoError(t, err)
		require.False(t, e.IsAvailable())
		require.LessOrEqual(t, o.ResidentBytes(), int64(1))
	})
}

type qualityEstimate struct{ count int }

func (e qualityEstimate) SignalSamples() uint64 { return safecast.MustConvert[uint64](e.count) }
func (e qualityEstimate) SignalWeight() float64 { return float64(e.count) }

type growingAccumulator struct{ count int }

func (a *growingAccumulator) Add(value int) error {
	if value < 0 {
		return errors.New("negative observation")
	}

	a.count += value
	return nil
}

func (a *growingAccumulator) Value(time.Time) (qualityEstimate, signal.Availability) {
	return qualityEstimate{count: a.count}, signal.Available
}

func (a *growingAccumulator) Size() int64 { return 8 + safecast.MustConvert[int64](a.count)*16 }

func (a *growingAccumulator) Clone() signal.Accumulator[int, qualityEstimate] {
	return &growingAccumulator{count: a.count}
}

func (a *growingAccumulator) DiscountEvidence(factor float64) {
	a.count = int(float64(a.count) * factor)
}

func TestPublicationMetadataAndGrowingEntry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		observed := time.Now()
		var updates []signal.UpdateID
		d := signal.NewSource[string, int, qualityEstimate]("size", signal.SourceConfig[string, int, qualityEstimate]{
			New:    func() signal.Accumulator[int, qualityEstimate] { return &growingAccumulator{} },
			Accept: func(k string, _ int) bool { return k != "ignored" },
			Stamp: func(value int, id signal.UpdateID, at time.Time) int {
				updates = append(updates, id)
				require.Equal(t, observed, at)
				return value
			},
			ObservationTime: func(int) time.Time { return observed },
		})
		r, err := signals.NewRegistry(d)

		require.NoError(t, err)
		o, err := signals.NewOwner(r, signals.Options{MaxEntryBytes: 512})

		require.NoError(t, err)
		v := view(t, o, "a", 1)
		input := signal.Input[string, int]{Name: "size"}
		ref := signal.Reference[string, qualityEstimate]{Name: "size"}

		require.NoError(t, signal.Observe(v, input, "ignored", 1))
		require.NoError(t, signal.Observe(v, input, "x", 4))
		require.ErrorContains(t, signal.Observe(v, input, "x", -1), "negative")
		plan := v.ForPlan()
		before, err := signal.Lookup(plan, ref, "x")

		require.NoError(t, err)
		require.Equal(t, uint64(4), before.Evidence.Samples)
		require.InDelta(t, 4.0, before.Evidence.EffectiveWeight, 1e-9)
		time.Sleep(time.Minute)

		require.NoError(t, signal.Observe(v, input, "x", 64))
		fresh, err := signal.Lookup(v, ref, "x")

		require.NoError(t, err)
		require.False(t, fresh.IsAvailable())
		after, err := signal.Lookup(plan, ref, "x")

		require.NoError(t, err)
		require.Equal(t, before, after)
		require.Len(t, updates, 2)
		require.Equal(t, updates[0].Epoch, updates[1].Epoch)
		require.Equal(t, updates[0].Sequence+1, updates[1].Sequence)
	})
}

func TestSchemaTransferDecisions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		action signal.TransferAction
		factor float64
		keep   bool
	}{
		{"retain", signal.Retain, 1, true},
		{"discount", signal.Discount, .5, true},
		{"reset", signal.Reset, 0, false},
		{"invalid factor", signal.Discount, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := signal.NewSource[string, int, qualityEstimate]("weighted", signal.SourceConfig[string, int, qualityEstimate]{
				Policy: signal.CrossRevision,
				New:    func() signal.Accumulator[int, qualityEstimate] { return &growingAccumulator{} },
				Transfer: func(signal.SchemaChange, signal.Footprint) signal.TransferDecision {
					return signal.TransferDecision{Action: tc.action, Factor: tc.factor}
				},
			})
			o := querysignalstest.Owner(t, d)
			a := view(t, o, "a", 1)

			require.NoError(t, signal.Observe(a, signal.Input[string, int]{Name: "weighted"}, "x", 8))
			b := view(t, o, "b", 1)
			e, err := signal.Lookup(b, signal.Reference[string, qualityEstimate]{Name: "weighted"}, "x")

			require.NoError(t, err)
			require.Equal(t, tc.keep, e.IsAvailable())

			if tc.keep {
				require.Equal(t, int(8*tc.factor), e.Value.count)
			}
		})
	}
}
