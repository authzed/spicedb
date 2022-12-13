package _allowedpanics

func MustDoSomething() {
	panic("some panic")
}

func mustDoSomething() {
	panic("some panic")
}

func UnderADefault() {
	switch "foo" {
	default:
		panic("some panic")
	}
}
