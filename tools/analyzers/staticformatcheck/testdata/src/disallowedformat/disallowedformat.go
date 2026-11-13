package disallowedformat

// NOTE: this def is just to get us to a `WithSourceErrorf` call without
// needing to import things, because importing in these tests is difficult.
type someNode struct{}

func (n *someNode) WithSourceErrorf(sourceCode string, message string, args ...any) error {
	return nil
}

func dynamicFormat(n *someNode, name string) error {
	message := "found dynamic message: " + name
	return n.WithSourceErrorf(name, message) // want "format string argument to `WithSourceErrorf` must be a static string"
}

func dynamicConcatFormat(n *someNode, name string) error {
	return n.WithSourceErrorf(name, "found dynamic message: "+name) // want "format string argument to `WithSourceErrorf` must be a static string"
}

func dynamicNestedFormat(n *someNode, name string) []error {
	message := "found dynamic message: " + name
	return append([]error{}, n.WithSourceErrorf(name, message)) // want "format string argument to `WithSourceErrorf` must be a static string"
}

func dynamicMethodExprFormat(n *someNode, name string) error {
	message := "found dynamic message: " + name
	return (*someNode).WithSourceErrorf(n, name, message) // want "format string argument to `WithSourceErrorf` must be a static string"
}
