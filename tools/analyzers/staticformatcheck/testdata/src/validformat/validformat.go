package validformat

// NOTE: this def is just to get us to a `WithSourceErrorf` call without
// needing to import things, because importing in these tests is difficult.
type someNode struct{}

func (n *someNode) WithSourceErrorf(sourceCode string, message string, args ...any) error {
	return nil
}

// unrelated has a non-format signature and must not be checked, even though
// its method shares the checked name.
type unrelated struct{}

func (u *unrelated) WithSourceErrorf(message string) error {
	return nil
}

const errTemplate = "found duplicate name: %s"

func literalFormat(n *someNode, name string) error {
	return n.WithSourceErrorf(name, "found duplicate name: %s", name)
}

func constantFormat(n *someNode, name string) error {
	return n.WithSourceErrorf(name, errTemplate, name)
}

func literalConcatFormat(n *someNode, name string) error {
	return n.WithSourceErrorf(name, "found duplicate "+"name: %s", name)
}

func methodExprFormat(n *someNode, name string) error {
	return (*someNode).WithSourceErrorf(n, name, "found duplicate name: %s", name)
}

func unrelatedSignature(u *unrelated, name string) error {
	return u.WithSourceErrorf("found dynamic message: " + name)
}
