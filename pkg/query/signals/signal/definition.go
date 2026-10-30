package signal

import (
	"reflect"
	"time"
)

// Accumulator owns provider state. Value and Clone must return independent snapshots.
type Accumulator[O, V any] interface {
	// Add incorporates an observation into the accumulator.
	Add(O) error

	// Value returns an independent estimate snapshot at the supplied time.
	Value(time.Time) (V, Availability)

	// Size returns the estimated resident size of the accumulator in bytes.
	Size() int64

	// Clone returns an independent copy of the accumulator state.
	Clone() Accumulator[O, V]
}

// State provides the type-erased accumulator contract used by the store.
type State interface {
	// Add incorporates an observation into the accumulator.
	Add(any) error

	// Value returns an independent estimate snapshot at the supplied time.
	Value(time.Time) (any, Availability)

	// Size returns the estimated resident size of the accumulator in bytes.
	Size() int64

	// Clone returns an independent copy of the accumulator state.
	Clone() State
}

type adapter[O, V any] struct{ Accumulator[O, V] }

// Add incorporates an observation into the accumulator.
func (a adapter[O, V]) Add(v any) error { return a.Accumulator.Add(v.(O)) }

// Value returns an independent estimate snapshot at the supplied time.
func (a adapter[O, V]) Value(t time.Time) (any, Availability) { return a.Accumulator.Value(t) }

// DiscountEvidence reduces existing evidence weight without renewing its age.
func (a adapter[O, V]) DiscountEvidence(f float64) bool {
	if d, ok := a.Accumulator.(Discountable); ok {
		d.DiscountEvidence(f)
		return true
	}

	return false
}

// Clone returns an independent copy of the accumulator state.
func (a adapter[O, V]) Clone() State { return adapter[O, V]{a.Accumulator.Clone()} }

// Descriptor defines a named provider and its lifecycle callbacks.
// It retains runtime types so the shared registry can validate named accesses and
// dependencies between providers with different generic types.
type Descriptor struct {
	// Build constructs an accumulator using the store's clock, when supplied.
	Build func(func() time.Time) State

	// Stamp attaches a source increment ID and publication time to an observation.
	Stamp func(any, UpdateID, time.Time) any

	// Accept filters observations before publication or derived-provider delivery.
	Accept func(any, any) bool

	// Name identifies a registered signal provider.
	Name Name

	// Description summarizes the provider's purpose.
	Description string

	// Policy selects exact-revision or cross-revision evidence storage.
	Policy RevisionPolicy

	// KeyType is the runtime type of the provider's population key.
	KeyType reflect.Type

	// ObservationType is the runtime type of accepted observations.
	ObservationType reflect.Type

	// ValueType is the runtime type of published estimates.
	ValueType reflect.Type

	// Dependencies lists providers that must be registered before this provider.
	Dependencies []Name

	// Source names the provider whose accepted increments feed this provider.
	Source Name

	// New constructs an empty accumulator.
	New func() State

	// Footprint extracts the schema dependencies associated with a population key.
	Footprint func(any) Footprint

	// Transfer decides whether evidence is compatible with a schema change.
	Transfer func(SchemaChange, Footprint) TransferDecision

	// ObservationTime extracts an observation's original time for evidence aging.
	ObservationTime func(any) time.Time
}

// SourceConfig defines typed construction and lifecycle callbacks for a provider.
type SourceConfig[K comparable, O, V any] struct {
	// Build constructs an accumulator using the store's clock, when supplied.
	Build func(func() time.Time) Accumulator[O, V]

	// Stamp attaches a source increment ID and publication time to an observation.
	Stamp func(O, UpdateID, time.Time) O

	// Accept filters observations before publication or derived-provider delivery.
	Accept func(K, O) bool

	// Description summarizes the provider's purpose.
	Description string

	// Policy selects exact-revision or cross-revision evidence storage.
	Policy RevisionPolicy

	// New constructs an empty accumulator.
	New func() Accumulator[O, V]

	// Footprint extracts the schema dependencies associated with a population key.
	Footprint func(K) Footprint

	// Transfer decides whether evidence is compatible with a schema change.
	Transfer func(SchemaChange, Footprint) TransferDecision

	// ObservationTime extracts an observation's original time for evidence aging.
	ObservationTime func(O) time.Time
}

// NewSource adapts a typed provider configuration into a store descriptor.
func NewSource[K comparable, O, V any](name Name, c SourceConfig[K, O, V]) Descriptor {
	d := Descriptor{
		Name:            name,
		Description:     c.Description,
		Policy:          c.Policy,
		KeyType:         reflect.TypeFor[K](),
		ObservationType: reflect.TypeFor[O](),
		ValueType:       reflect.TypeFor[V](),
		Transfer:        c.Transfer,
	}

	if c.New != nil {
		d.New = func() State { return adapter[O, V]{c.New()} }
	}

	if c.Build != nil {
		d.Build = func(now func() time.Time) State { return adapter[O, V]{c.Build(now)} }
	}

	if c.Stamp != nil {
		d.Stamp = func(o any, id UpdateID, at time.Time) any { return c.Stamp(o.(O), id, at) }
	}

	if c.Accept != nil {
		d.Accept = func(k, o any) bool { return c.Accept(k.(K), o.(O)) }
	}

	if c.Footprint != nil {
		d.Footprint = func(k any) Footprint { return c.Footprint(k.(K)) }
	}

	if c.ObservationTime != nil {
		d.ObservationTime = func(o any) time.Time { return c.ObservationTime(o.(O)) }
	}

	return d
}

// NewDerived consumes accepted increments of its source, never cumulative snapshots.
func NewDerived[K comparable, O, V any](name Name, source Input[K, O], c SourceConfig[K, O, V]) Descriptor {
	d := NewSource(name, c)
	d.Source = source.Name
	d.Dependencies = []Name{source.Name}
	return d
}
