// Package signals stores and resolves named planning evidence.
package signals

import (
	"errors"
	"fmt"
	"slices"

	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

// Registry holds named signal descriptors and validates their dependencies.
type Registry struct {
	definitions map[signal.Name]signal.Descriptor
	order       []signal.Name
	frozen      bool
}

// NewRegistry creates a provider catalog and validates its dependency graph.
func NewRegistry(ds ...signal.Descriptor) (*Registry, error) {
	r := &Registry{definitions: map[signal.Name]signal.Descriptor{}}

	for _, d := range ds {
		if err := r.Register(d); err != nil {
			return nil, err
		}
	}

	if err := r.validate(); err != nil {
		return nil, err
	}

	return r, nil
}

// Register adds a descriptor to an unfrozen catalog, rejecting invalid or duplicate names.
func (r *Registry) Register(d signal.Descriptor) error {
	if r.frozen {
		return errors.New("signal registry is frozen")
	}

	if d.Name == "" || d.KeyType == nil || !d.KeyType.Comparable() ||
		d.ObservationType == nil || d.ValueType == nil || d.New == nil || d.Policy > signal.CrossRevision {
		return fmt.Errorf("invalid signal descriptor %q", d.Name)
	}

	if _, ok := r.definitions[d.Name]; ok {
		return fmt.Errorf("duplicate signal %q", d.Name)
	}

	d.Dependencies = slices.Clone(d.Dependencies)
	r.definitions[d.Name] = d
	return nil
}

func (r *Registry) validate() error {
	state := map[signal.Name]uint8{}
	r.order = nil
	var visit func(signal.Name) error
	visit = func(n signal.Name) error {
		if state[n] == 1 {
			return fmt.Errorf("signal dependency cycle at %q", n)
		}

		if state[n] == 2 {
			return nil
		}

		d, ok := r.definitions[n]
		if !ok {
			return fmt.Errorf("missing signal dependency %q", n)
		}

		state[n] = 1
		deps := slices.Clone(d.Dependencies)
		slices.Sort(deps)

		for _, dep := range deps {
			if err := visit(dep); err != nil {
				return err
			}
		}

		if d.Source != "" {
			source, ok := r.definitions[d.Source]
			if !ok || source.KeyType != d.KeyType || source.ObservationType != d.ObservationType {
				return fmt.Errorf("incompatible source binding for %q", n)
			}
		}

		state[n] = 2
		r.order = append(r.order, n)
		return nil
	}
	names := make([]signal.Name, 0, len(r.definitions))

	for n := range r.definitions {
		names = append(names, n)
	}

	slices.Sort(names)

	for _, n := range names {
		if err := visit(n); err != nil {
			return err
		}
	}

	return nil
}

// Freeze validates dependencies and prevents further registration.
func (r *Registry) Freeze() error {
	if err := r.validate(); err != nil {
		return err
	}

	r.frozen = true
	return nil
}

// Contains reports whether a name exists in the catalog. Freeze before concurrent use.
func (r *Registry) Contains(name signal.Name) bool { _, ok := r.definitions[name]; return ok }
