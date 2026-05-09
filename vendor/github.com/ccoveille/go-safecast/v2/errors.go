package safecast

import (
	"errors"
	"fmt"
)

// ErrConversionIssue is a generic error for type conversion issues
//
// This error is wrapped in all errors reported by this package
var ErrConversionIssue = errors.New("conversion issue")

// ErrRangeOverflow is an error for when the value is outside the range of the desired type
//
// Examples include converting 1000 or -1 to uint8.
//
// This is error is wrapped by [ErrExceedMaximumValue] and [ErrExceedMinimumValue]
// It can be used to check for range overflow issues in a generic way.
//
// [ErrConversionIssue] is also wrapped when this error is returned.
var ErrRangeOverflow = errors.New("range overflow")

// ErrExceedMaximumValue is an error for when the value is greater than the maximum value of the desired type.
//
// Examples include converting 1000 to uint8.
//
// [ErrRangeOverflow] and [ErrConversionIssue] are also wrapped when this error is returned.
var ErrExceedMaximumValue = errors.New("maximum value for this type exceeded")

// ErrExceedMinimumValue is an error for when the value is less than the minimum value of the desired type.
//
// Examples include converting -1 to uint16.
//
// [ErrRangeOverflow] and [ErrConversionIssue] are also wrapped when this error is returned.
var ErrExceedMinimumValue = errors.New("minimum value for this type exceeded")

// ErrUnsupportedConversion is an error for when the conversion is not supported from the provided type.
//
// Examples include converting [math.NaN] to int.
//
// [ErrConversionIssue] is also wrapped when this error is returned.
var ErrUnsupportedConversion = errors.New("unsupported type")

// ErrStringConversion is an error for when the conversion fails from string.
//
// Examples include converting "abc" to int.
//
// [ErrConversionIssue] is also wrapped when this error is returned.
var ErrStringConversion = errors.New("cannot convert from string")

// ErrDecimalLoss is an error for when decimal loss occurs during conversion.
//
// Examples include converting 3.14 to int.
//
// [ErrConversionIssue] is also wrapped when this error is returned.
var ErrDecimalLoss = errors.New("decimal loss during conversion")

// errorHelper is a helper struct for error messages
// It is used to wrap other errors, and provides additional information
type errorHelper[NumOut Number] struct {
	numberBase numberBase // base for number conversion, if applicable
	value      any
	err        error
}

func (e errorHelper[NumOut]) Error() string {
	errMessage := ErrConversionIssue.Error()

	switch {
	case errors.Is(e.err, ErrExceedMaximumValue):
		boundary := maxOf[NumOut]()
		errMessage = fmt.Sprintf("%s: %v (%T) is greater than %v (%T)", errMessage, e.value, e.value, boundary, boundary)
	case errors.Is(e.err, ErrExceedMinimumValue):
		boundary := minOf[NumOut]()
		errMessage = fmt.Sprintf("%s: %v (%T) is less than %v (%T)", errMessage, e.value, e.value, boundary, boundary)
	case errors.Is(e.err, ErrUnsupportedConversion):
		errMessage = fmt.Sprintf("%s: %v (%T) is not supported", errMessage, e.value, e.value)
	case errors.Is(e.err, ErrStringConversion):
		baseInfoSuffix := e.numberBase.String()
		if baseInfoSuffix != "" {
			baseInfoSuffix = " (base " + baseInfoSuffix + ")"
		}
		targetType := NumOut(0)
		return fmt.Sprintf("%s: cannot convert from %#q to %T%s", errMessage, e.value, targetType, baseInfoSuffix)
	}

	if e.err != nil {
		errMessage = fmt.Sprintf("%s: %s", errMessage, e.err.Error())
	}
	return errMessage
}

func (e errorHelper[NumOut]) Unwrap() []error {
	errs := []error{ErrConversionIssue}
	if e.err != nil {
		switch {
		case
			errors.Is(e.err, ErrExceedMaximumValue),
			errors.Is(e.err, ErrExceedMinimumValue):
			errs = append(errs, ErrRangeOverflow)
		}
		errs = append(errs, e.err)
	}
	return errs
}
