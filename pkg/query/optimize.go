package query

// TypedOptimizerFunc is a function that transforms an iterator of a specific type T
// into a potentially optimized iterator. It returns the optimized iterator, a boolean
// indicating whether any optimization was performed, and an error if the optimization failed.
//
// The type parameter T constrains the function to operate only on specific iterator types,
// providing compile-time type safety when creating typed optimizers.
type TypedOptimizerFunc[T Iterator] func(it T) (Iterator, bool, error)

// OptimizerFunc is a type-erased wrapper around TypedOptimizerFunc[T] that can be
// stored in a homogeneous list while maintaining type safety at runtime.
type OptimizerFunc func(it Iterator) (Iterator, bool, error)

// WrapOptimizer wraps a typed TypedOptimizerFunc[T] into a type-erased OptimizerFunc.
// This allows optimizer functions for different concrete iterator types to be stored
// together in a heterogeneous list.
func WrapOptimizer[T Iterator](fn TypedOptimizerFunc[T]) OptimizerFunc {
	return func(it Iterator) (Iterator, bool, error) {
		if v, ok := it.(T); ok {
			return fn(v)
		}
		return it, false, nil
	}
}

// ApplyOptimizations recursively applies a list of optimizer functions to an iterator
// tree, transforming it into an optimized form.
//
// The function operates bottom-up, optimizing leafs and subiterators first, and replacing the
// subtrees up to the top, which it then returns.
//
// Parameters:
//   - it: The iterator tree to optimize
//   - fns: A list of optimizer functions to apply
//
// Returns:
//   - The optimized iterator (which may be the same as the input if no optimizations applied)
//   - A boolean indicating whether any changes were made
//   - An error if any optimization failed
func ApplyOptimizations(it Iterator, fns []OptimizerFunc) (Iterator, bool, error) {
	var err error
	subs := it.Subiterators()
	changed := false
	if len(subs) != 0 {
		subChanged := false
		for i, subit := range subs {
			newit, ok, err := ApplyOptimizations(subit, fns)
			if err != nil {
				return nil, false, err
			}
			if ok {
				subs[i] = newit
				subChanged = true
			}
		}
		if subChanged {
			changed = true
			it, err = it.ReplaceSubiterators(subs)
			if err != nil {
				return nil, false, err
			}
		}
	}

	// Apply optimizers in a loop until no more changes occur
	// This ensures optimizations compose correctly regardless of order
	for {
		outerChanged := false
		for _, fn := range fns {
			newit, fnChanged, err := fn(it)
			if err != nil {
				return nil, false, err
			}
			if fnChanged {
				it = newit
				outerChanged = true
				changed = true
			}
		}
		if !outerChanged {
			break
		}
	}
	return it, changed, nil
}
