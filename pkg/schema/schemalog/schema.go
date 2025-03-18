package schemalog

import corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"

type Schema struct {
	definitions map[string]*Definition
	edges       map[string]Edge
	caveats     map[string]*Caveat
}

type Caveat struct {
	Proto *corev1.CaveatDefinition
}

type Entity interface {
	Name() string
}
