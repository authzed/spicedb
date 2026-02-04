package goodmustcalls

import (
	"regexp"
)

// Example 1: MustCompile in non-error-returning function (OK)
func GetPattern() *regexp.Regexp {
	return regexp.MustCompile("test")
}

// Example 2: MustCompile at package level (OK)
var packageLevelRegex = regexp.MustCompile("^test$")

// Example 3: Must method in init function (OK)
func init() {
	_ = regexp.MustCompile("init pattern")
}

// Example 4: Must method in function that doesn't return error (OK)
func ParseWithoutError(data string) Result {
	return MustParse(data)
}

// Example 5: Must method in function that returns only values, no error (OK)
func ConvertToInt(value string) int {
	return MustConvert(value)
}

// Example 6: Must method in closure without error return (OK)
func OuterWithNonErrorClosure() {
	innerFunc := func(pattern string) *regexp.Regexp {
		return regexp.MustCompile(pattern)
	}
	_ = innerFunc
}

// Example 7: Function that returns something named "error" but isn't the error interface (OK)
type ErrorCode struct {
	Code int
}

func GetErrorCode() ErrorCode {
	_ = regexp.MustCompile("pattern")
	return ErrorCode{Code: 42}
}

// Example 8: Must method in a function that panics anyway (OK - no error return)
func MustDoSomething() {
	_ = regexp.MustCompile("test")
	panic("intentional panic")
}

// Helper types and functions for testing
type Result struct {
	Value string
}

func MustParse(data string) Result {
	if data == "" {
		panic("empty data")
	}
	return Result{Value: data}
}

func MustConvert(value string) int {
	if value == "" {
		panic("empty value")
	}
	return 42
}
