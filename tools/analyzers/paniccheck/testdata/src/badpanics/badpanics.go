package _badpanics

func NotNamedMust() {
	panic("some panic") // want "disallowed panic statement"
}

func NotUnderADefault() {
	switch "foo" {
	case "foo":
		panic("some panic") // want "disallowed panic statement"
	}
}

func UnderADefaultThatReturnsAnError() error {
	switch "foo" {
	default:
		panic("some panic") // want "disallowed panic statement"
	}
}
