package genutil

import (
	"fmt"

	"buf.build/go/protovalidate"
)

// MustNewProtoValidator wraps protovalidate.New() to panic
// if the validator can't be constructed.
func MustNewProtoValidator(opts ...protovalidate.ValidatorOption) protovalidate.Validator {
	validator, err := protovalidate.New(opts...)
	if err != nil {
		wrappedErr := fmt.Errorf("could not construct validator: %w", err)
		panic(wrappedErr)
	}
	return validator
}
