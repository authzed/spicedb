package namespace

// Mapper provides an interface for creating synthetic namespace names from
// user provided namespace names.
type Mapper interface {
	// Encode translates a given namespace name to an encoded namespace identifier.
	Encode(name string) (string, error)

	// Reverse translates a given namespace identifier to the user supplied namespace name.
	Reverse(id string) (string, error)
}

type passthrough struct{}

// Encode implements Mapper
func (p passthrough) Encode(name string) (string, error) {
	return name, nil
}

// Reverse implements Mapper
func (p passthrough) Reverse(id string) (string, error) {
	return id, nil
}

// PassthroughMapper is a mapper implementation which passes through the original namespace
// names unmodified.
var PassthroughMapper Mapper = passthrough{}
