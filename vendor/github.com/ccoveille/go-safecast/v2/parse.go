package safecast

import (
	"errors"
	"strconv"
	"strings"
)

// Parse attempts to convert any string to the desired [Number] type.
//
// # Concept
//
// Parse is a convenient wrapper around [strconv.ParseInt], [strconv.ParseUint], and [strconv.ParseFloat].
//
// # Behavior
//
// If the conversion is possible, the converted value is returned.
//
// # Errors when conversion exceeds range of the desired type, the following errors are wrapped in the returned error:
//
//   - [ErrRangeOverflow] when the value is outside the range of the desired type. (example: "1000" or "-1" to uint8).
//   - [ErrExceedMaximumValue] when the value exceeds the maximum value of the desired type (example: "1000" to uint8).
//   - [ErrExceedMinimumValue] when the value is less than the minimum value of the desired type (example: "-1" to uint16).
//
// # General errors wrapped on parsing and conversion failure:
//
//   - [ErrStringConversion] and [ErrConversionIssue] are wrapped when the conversion from string fails (example: "abc" to int).
//
// # Options
//
// The behavior of the parsing can be modified using [ParseOption]s.
// By default, the number base is set to decimal (base 10).
//
// Use one of the provided option functions to set the desired behavior.
// See [WithBaseDecimal], [WithBaseHexadecimal], [WithBaseOctal], [WithBaseBinary], and [WithBaseAutoDetection].
func Parse[NumOut Number](s string, opts ...ParseOption) (converted NumOut, err error) {
	options := newParseOptions(opts...)
	numberBase := options.numberBase

	// naive auto-detection of the sign
	isNegative := strings.HasPrefix(s, "-")

	// naive auto-detection of float
	if strings.Contains(s, ".") {
		o, err := strconv.ParseFloat(s, 64)
		if err != nil {
			errParseFloat := ErrStringConversion
			if errors.Is(err, strconv.ErrRange) {
				errParseFloat = ErrExceedMaximumValue
				if isNegative {
					errParseFloat = ErrExceedMinimumValue
				}
			}

			// If the error is a range error, wrap it in an errorHelper
			return 0, errorHelper[NumOut]{
				numberBase: numberBase,
				value:      s,
				err:        errParseFloat,
			}
		}
		return Convert[NumOut](o)
	}

	if isNegative {
		o, err := strconv.ParseInt(s, int(options.numberBase), 64)
		if err != nil {
			errParseInt := ErrStringConversion
			if errors.Is(err, strconv.ErrRange) {
				errParseInt = ErrExceedMinimumValue
				if isNegative {
					errParseInt = ErrExceedMinimumValue
				}
			}
			return 0, errorHelper[NumOut]{
				numberBase: numberBase,
				value:      s,
				err:        errParseInt,
			}
		}

		return Convert[NumOut](o)
	}

	o, err := strconv.ParseUint(s, int(options.numberBase), 64)
	if err != nil {
		errParseUint := ErrStringConversion
		if errors.Is(err, strconv.ErrRange) {
			errParseUint = ErrExceedMaximumValue
		}
		return 0, errorHelper[NumOut]{
			numberBase: numberBase,
			value:      s,
			err:        errParseUint,
		}
	}
	return Convert[NumOut](o)
}

// MustParse calls [Parse] to convert the value to the desired type, and panics if the conversion fails.
func MustParse[NumOut Number](orig string, opts ...ParseOption) NumOut {
	converted, err := Parse[NumOut](orig, opts...)
	if err != nil {
		panic(err)
	}
	return converted
}

// RequireParse is a test helper that calls [Parse] that converts the value to the desired type,
// and fails the test if the conversion fails.
func RequireParse[NumOut Number](t TestingT, orig string, opts ...ParseOption) (converted NumOut) {
	t.Helper()

	converted, err := Parse[NumOut](orig, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return converted
}

// ParseOption defines options for the [Parse] function.
//
// Use one of the provided option functions to set the desired behavior.
// See [WithBaseDecimal], [WithBaseHexadecimal], [WithBaseOctal], [WithBaseBinary], and [WithBaseAutoDetection].
type ParseOption func(*parseConfig)

func newParseOptions(opts ...ParseOption) *parseConfig {
	po := &parseConfig{
		numberBase: baseDecimal, // default to base 10
	}

	for _, opt := range opts {
		opt(po)
	}
	return po
}

type numberBase int

const (
	baseAuto        numberBase = 0
	baseHexadecimal numberBase = 16
	baseDecimal     numberBase = 10
	baseOctal       numberBase = 8
	baseBinary      numberBase = 2
)

func (nb numberBase) String() string {
	switch nb {
	case baseAuto:
		return "auto-detection"
	case baseHexadecimal:
		return "hexadecimal"
	case baseOctal:
		return "octal"
	case baseBinary:
		return "binary"
	default:
		return "" // decimal is the default, so we return empty string
	}
}

type parseConfig struct {
	numberBase numberBase
}

// WithBaseDecimal sets the number base to decimal (base 10) when used with [Parse].
//
// This is the default behavior of [Parse].
func WithBaseDecimal() ParseOption {
	return func(pc *parseConfig) {
		pc.numberBase = baseDecimal
	}
}

// WithBaseHexadecimal sets the number base to hexadecimal (base 16) when used with [Parse].
//
// Note that the string to parse must not have the "0x" prefix; use [WithBaseAutoDetection] for that.
func WithBaseHexadecimal() ParseOption {
	return func(pc *parseConfig) {
		pc.numberBase = baseHexadecimal
	}
}

// WithBaseOctal sets the number base to octal (base 8) when used with [Parse].
//
// Note that the string to parse must not have the "0o" prefix; use [WithBaseAutoDetection] for that.
func WithBaseOctal() ParseOption {
	return func(pc *parseConfig) {
		pc.numberBase = baseOctal
	}
}

// WithBaseBinary sets the number base to binary (base 2) when used with [Parse].
//
// Note that the string to parse must not have the "0b" prefix; use [WithBaseAutoDetection] for that.
func WithBaseBinary() ParseOption {
	return func(pc *parseConfig) {
		pc.numberBase = baseBinary
	}
}

// WithBaseAutoDetection sets the number base to auto-detection when used with [Parse].
//
// The base is implied by the string's prefix following the sign (if present): 2 for "0b", 8 for "0" or "0o",
// 16 for "0x", and 10 otherwise.
//
// Also, underscore characters are permitted as defined by the Go syntax for [integer literals].
//
// For more details on how auto-detection works, see the documentation of [strconv.ParseInt] or [strconv.ParseUint].
//
// # ⚠️ Warning: this option comes with some caveats that come from Go [strconv] package and Go [integer literals].
//
//	A string starting with "0" (and not "0x" or "0b") is interpreted as octal, which can lead to unexpected results.
//	 - "010" is interpreted as 8 in decimal, not 10.
//	 - "07" is interpreted as 7 in decimal, but "08" or "09" will fail to parse because 8 and 9 are not valid octal digits.
//
// [integer literals]: https://go.dev/ref/spec#Integer_literals
func WithBaseAutoDetection() ParseOption {
	return func(pc *parseConfig) {
		pc.numberBase = baseAuto
	}
}
