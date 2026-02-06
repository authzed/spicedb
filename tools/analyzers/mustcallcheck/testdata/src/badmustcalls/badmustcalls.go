package badmustcalls

import (
	"regexp"
)

// Example 1: MustCompile in error-returning function
func CompilePattern(pattern string) (*regexp.Regexp, error) {
	re := regexp.MustCompile(pattern) // want "found call to MustCompile in error-returning function"
	return re, nil
}

// Example 2: Custom Must method in error-returning function
func ParseData(data string) (Result, error) {
	result := MustParse(data) // want "found call to MustParse in error-returning function"
	return result, nil
}

// Example 3: Must method call on object in error-returning function
func ProcessItem(item string) error {
	parser := &Parser{}
	_ = parser.MustValidate(item) // want "found call to MustValidate in error-returning function"
	return nil
}

// Example 4: Multiple return values including error
func ConvertValue(value string) (int, string, error) {
	result := MustConvert(value) // want "found call to MustConvert in error-returning function"
	return result, "success", nil
}

// Example 5: Must call in nested function (closure) that returns error
func OuterFunction() {
	innerFunc := func(pattern string) error {
		_ = regexp.MustCompile(pattern) // want "found call to MustCompile in error-returning function"
		return nil
	}
	_ = innerFunc
}

// Helper types and functions for testing
type Result struct {
	Value string
}

type Parser struct{}

func (p *Parser) MustValidate(s string) bool {
	if s == "" {
		panic("invalid input")
	}
	return true
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

// Example 6: Should catch Must* even when error is not the last return value
func AnotherPattern() (error, string) {
	_ = regexp.MustCompile("test") // want "found call to MustCompile in error-returning function"
	return nil, "ok"
}

// Example 7: Must* call in a handler function that returns error should be caught
// This tests that the analyzer correctly identifies the error-returning closure
func OuterMethodNoError() {
	items := []string{"a", "b", "c"}
	for _, item := range items {
		// Define a handler that returns error
		handler := func(input string) error {
			_ = MustParse(input) // want "found call to MustParse in error-returning function"
			return nil
		}
		_ = handler(item)
	}
}
