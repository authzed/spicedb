package _nilreturn

type (
	someStruct    struct{}
	anotherStruct struct{}
)

func returnSomething() *someStruct {
	return nil // want "found `nil` returned for value of nil-disallowed type"
}

func returnNonNil() *someStruct {
	return &someStruct{}
}

func returnOtherNilType() *anotherStruct {
	return nil
}

func useNilOtherType() *anotherStruct {
	var a *anotherStruct
	return a
}

func useNil() any {
	var s *someStruct // want "default nil assignment to s"
	return s
}

func multiReturn() (int, bool, *someStruct) {
	return 1, true, nil // want "found `nil` returned for value of nil-disallowed type"
}
