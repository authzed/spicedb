// Package decorators defines the registry of schema decorators: which decorators
// exist, where each may be applied, what parameters each takes, and which `use`
// feature flag enables it.
package decorators

import "slices"

// Site is a location in a schema at which a decorator may be applied.
type Site string

const (
	SiteDefinition  Site = "definition"
	SiteRelation    Site = "relation"
	SitePermission  Site = "permission"
	SiteSubjectType Site = "subject type"
	SiteCaveat      Site = "caveat"
)

// ParamType is the declared type of a decorator parameter.
type ParamType string

const (
	ParamTypeInt    ParamType = "int"
	ParamTypeString ParamType = "string"
	ParamTypeBool   ParamType = "bool"
	ParamTypeEnum   ParamType = "enum"
)

// Parameter declares a single named parameter of a decorator.
type Parameter struct {
	// Name is the parameter's name, as written in `@decorator(name: value)`.
	Name string

	// Type is the parameter's declared type.
	Type ParamType

	// Required indicates the parameter must be supplied.
	Required bool

	// EnumValues is the set of legal values; ParamTypeEnum only.
	EnumValues []string
}

// Spec declares a single decorator.
type Spec struct {
	// Name is the decorator's name, without the leading `@`.
	Name string

	// RequiredFlag is the `use` feature flag that enables this decorator. Several
	// decorators may share one flag.
	RequiredFlag string

	// Sites are the locations at which this decorator may be applied. A decorator
	// listing SiteDefinition may also be applied to a `partial`, in which case it
	// applies to every definition including that partial.
	Sites []Site

	// Parameters are the decorator's parameters, in canonical order.
	Parameters []Parameter
}

// AllowsSite returns whether this decorator may be applied at the given site.
func (s Spec) AllowsSite(site Site) bool {
	return slices.Contains(s.Sites, site)
}

// Parameter returns the named parameter's declaration, if it exists.
func (s Spec) Parameter(name string) (Parameter, bool) {
	for _, param := range s.Parameters {
		if param.Name == name {
			return param, true
		}
	}
	return Parameter{}, false
}

// ParameterNames returns the names of all declared parameters, in canonical order.
func (s Spec) ParameterNames() []string {
	names := make([]string, 0, len(s.Parameters))
	for _, param := range s.Parameters {
		names = append(names, param.Name)
	}
	return names
}

// Registry is the set of known decorators, keyed by name.
type Registry map[string]Spec

// Names returns all registered decorator names, sorted.
func (r Registry) Names() []string {
	names := make([]string, 0, len(r))
	for name := range r {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// DefaultRegistry is the registry used in production. It is intentionally empty:
// the decorator machinery ships before any decorator does.
var DefaultRegistry = Registry{}
