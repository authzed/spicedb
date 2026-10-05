package signal

import "reflect"

type (

	// Reference binds a signal name to its key and estimate types.
	Reference[K comparable, V any] struct {
		// Name identifies a registered signal provider.
		Name Name
	}

	// Input binds a signal name to its key and observation types.
	Input[K comparable, O any] struct {
		// Name identifies a registered signal provider.
		Name Name
	}

	// ReadView provides type-checked access to published signal estimates.
	ReadView interface {
		// Read reads an estimate after validating its key and value types.
		Read(Name, any, reflect.Type, reflect.Type) (any, Availability, Evidence, error)
	}
)

// Writer provides type-checked publication of signal observations.
type Writer interface {
	// Write publishes an observation after validating its key and observation types.
	Write(Name, any, any, reflect.Type, reflect.Type) error
}

// Lookup reads a typed estimate and its availability from a signal view.
func Lookup[K comparable, V any](v ReadView, ref Reference[K, V], key K) (Estimate[V], error) {
	value, status, evidence, err := v.Read(ref.Name, key, reflect.TypeFor[K](), reflect.TypeFor[V]())
	result := Estimate[V]{Availability: status, Evidence: evidence}

	if err == nil && status == Available {
		result.Value = value.(V)
	}

	return result, err
}

// LookupFirst returns the first available estimate and its signal name in candidate order.
// Unavailable estimates are skipped; lookup errors stop the search and identify the failing signal.
// If no candidate is available, it returns Missing availability and an empty signal name.
func LookupFirst[K comparable, V any](v ReadView, key K, refs ...Reference[K, V]) (Estimate[V], Name, error) {
	for _, ref := range refs {
		estimate, err := Lookup(v, ref, key)
		if err != nil {
			return estimate, ref.Name, err
		}

		if estimate.IsAvailable() {
			return estimate, ref.Name, nil
		}
	}

	return Estimate[V]{Availability: Missing}, "", nil
}

// Observe publishes a typed observation to its scoped signal writer.
func Observe[K comparable, O any](w Writer, in Input[K, O], key K, observation O) error {
	return w.Write(in.Name, key, observation, reflect.TypeFor[K](), reflect.TypeFor[O]())
}
