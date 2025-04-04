package schemalog

import (
	"iter"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type Definition struct {
	schema      *Schema
	Proto       *corev1.NamespaceDefinition
	relations   map[string]*Relation
	permissions map[string]*Permission
}

var _ Entity = &Definition{}

// Name returns the full name of the Definition
func (d *Definition) Name() string {
	return d.Proto.GetName()
}

func (d *Definition) Relations() iter.Seq2[string, *Relation] {
	return func(yield func(string, *Relation) bool) {
		for k, v := range d.relations {
			if !yield(k, v) {
				return
			}
		}
	}
}

func (d *Definition) Permissions() iter.Seq2[string, *Permission] {
	return func(yield func(string, *Permission) bool) {
		for k, v := range d.permissions {
			if !yield(k, v) {
				return
			}
		}
	}
}

func (d *Definition) Edges() iter.Seq2[string, Edge] {
	return func(yield func(string, Edge) bool) {
		for k, v := range d.permissions {
			if !yield(k, v) {
				return
			}
		}
		for k, v := range d.relations {
			if !yield(k, v) {
				return
			}
		}
	}
}
