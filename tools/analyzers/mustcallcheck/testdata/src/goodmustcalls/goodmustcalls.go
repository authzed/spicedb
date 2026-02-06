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

// Example 9: Must method called in a method that has a function parameter but doesn't return error (OK)
// This simulates the ForEachType pattern where a method accepts a handler function
func ProcessEachItem(items []string, handler func(item string)) {
	for _, item := range items {
		// This MustParse call should be OK because ProcessEachItem doesn't return an error
		parsed := MustParse(item)
		handler(parsed.Value)
	}
}

// Example 10: Similar pattern with a handler that returns error, but the outer method doesn't (OK)
func ForEachType(handler func(name string) error) {
	items := []string{"a", "b", "c"}
	for _, item := range items {
		// This MustParse should be OK because ForEachType itself doesn't return error
		_ = MustParse(item)
		_ = handler(item)
	}
}

// Example 11: Method with SubjectSet parameter type (mimics real ForEachType from datasets)
type SubjectSet interface{}

func (s *DataSet) ForEachTypeMethod(handler func(name string, subjects SubjectSet)) {
	items := map[string]string{"a": "x", "b": "y"}
	for key := range items {
		// This MustParse should be OK because ForEachTypeMethod doesn't return error
		result := MustParse(key)
		handler(result.Value, nil)
	}
}

type DataSet struct{}

