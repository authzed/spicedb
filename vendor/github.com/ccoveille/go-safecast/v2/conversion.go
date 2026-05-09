package safecast

import (
	"math"
)

// MustConvert calls [Convert] to convert the value to the desired type, and panics if the conversion fails.
func MustConvert[NumOut Number, NumIn Number](orig NumIn) NumOut {
	converted, err := Convert[NumOut](orig)
	if err != nil {
		panic(err)
	}
	return converted
}

// TestingT is an interface wrapper used by [RequireConvert] that we need for testing purposes.
//
// Only the methods used by [RequireConvert] are expected to be implemented.
//
// [*testing.T], [*testing.B], or [*testing.F] types satisfy this interface.
type TestingT interface {
	Helper()
	Fatal(args ...any)
}

// RequireConvert is a test helper that calls [Convert] that converts the value to the desired type,
// and fails the test if the conversion fails.
func RequireConvert[NumOut Number, NumIn Number](t TestingT, orig NumIn) (converted NumOut) {
	t.Helper()

	converted, err := Convert[NumOut](orig)
	if err != nil {
		t.Fatal(err)
	}
	return converted
}

// Convert attempts to convert any [Number] to the desired [Number] type.
//
// # Behavior
//
//   - If the conversion is possible, the converted value is returned.
//
// # Errors when conversion exceeds range of the desired type, the following errors are wrapped in the returned error:
//
//   - [ErrRangeOverflow] when the value is outside the range of the desired type. (example: 1000 or -1 to uint8).
//   - [ErrExceedMaximumValue] when the value exceeds the maximum value of the desired type (example: 1000 to uint8).
//   - [ErrExceedMinimumValue] when the value is less than the minimum value of the desired type (example: -1 to uint16).
//
// # Errors when conversion is not possible, the following errors are wrapped in the returned error:
//
//   - [ErrorUnsupportedConversion] when the conversion is not possible for the desired type (example: NaN to int).
//   - [ErrStringConversion] when the conversion from string fails (example: "abc" to int).
//
// # General errors wrapped on conversion failure:
//
//   - [ErrConversionIssue] is always wrapped in the returned error when [Convert] fails (example "abc", -1, or 1000 to uint8).
func Convert[NumOut Number, NumIn Number](orig NumIn, opts ...ConvertOption) (NumOut, error) {
	converted := NumOut(orig)
	if isFloat64[NumIn]() {
		floatOrig := float64(orig)
		if math.IsInf(floatOrig, 1) || math.IsInf(floatOrig, -1) {
			return converted, getRangeError[NumOut](orig)
		}
		if math.IsNaN(floatOrig) {
			return converted, errorHelper[NumOut]{
				value: orig,
				err:   ErrUnsupportedConversion,
			}
		}
	}

	config := newConvertOptions(opts...)

	if isFloat64[NumOut]() {
		// float64 cannot overflow, so we don't have to worry about it
		return converted, nil
	}

	if isFloat32[NumOut]() {
		// check boundary
		if math.Abs(float64(orig)) < math.MaxFloat32 {
			// the value is within float32 range, there is no overflow
			return converted, nil
		}

		// TODO: check for numbers close to math.MaxFloat32

		return converted, getRangeError[NumOut](orig)
	}

	if !sameSign(orig, converted) {
		return converted, getRangeError[NumOut](orig)
	}

	// and compare
	base := orig
	if isFloat[NumIn]() {
		base = NumIn(math.Trunc(float64(orig)))
	}

	// convert back to the original type
	cast := NumIn(converted)
	if cast != base {
		return converted, getRangeError[NumOut](orig)
	}

	if config.reportDecimalLoss && isFloat[NumIn]() && !isFloat[NumOut]() {
		if orig != cast {
			return converted, errorHelper[NumOut]{
				value: orig,
				err:   ErrDecimalLoss,
			}
		}
	}

	return converted, nil
}

func getRangeError[NumOut Number, NumIn Number](value NumIn) error {
	err := ErrExceedMaximumValue
	if value < 0 {
		err = ErrExceedMinimumValue
	}

	return errorHelper[NumOut]{
		value: value,
		err:   err,
	}
}

type convertConfig struct {
	reportDecimalLoss bool
}

// ConvertOption is a function type used to set options for the [Convert] function.
type ConvertOption func(*convertConfig)

func newConvertOptions(opts ...ConvertOption) *convertConfig {
	po := &convertConfig{
		reportDecimalLoss: false,
	}

	for _, opt := range opts {
		opt(po)
	}
	return po
}

// WithDecimalLossReport is a [ConvertOption] that enables reporting of decimal loss
// when converting from a floating-point type to an integer type.
//
// When this option is used, if the conversion results in loss of decimal information,
// the returned error will wrap [ErrDecimalLoss].
//
// Example:
//
//	value, err := Convert[int](3.14, WithDecimalLossReport())
func WithDecimalLossReport() ConvertOption {
	return func(cfg *convertConfig) {
		cfg.reportDecimalLoss = true
	}
}
