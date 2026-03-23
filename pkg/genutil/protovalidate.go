package genutil

import (
	"fmt"

	"buf.build/go/protovalidate"
)

// MustNewProtoValidator wraps NewProtoValidator() to panic
// if the validator can't be constructed.
func MustNewProtoValidator(opts ...protovalidate.ValidatorOption) protovalidate.Validator {
	validator, err := NewProtoValidator(opts...)
	if err != nil {
		panic(err)
	}
	return validator
}

// MustNewProtoValidator returns a new protovalidate instance.
func NewProtoValidator(opts ...protovalidate.ValidatorOption) (protovalidate.Validator, error) {
	validator, err := protovalidate.New(opts...)
	if err != nil {
		wrappedErr := fmt.Errorf("could not construct validator: %w", err)
		return nil, wrappedErr
	}
	return validator, nil
}
