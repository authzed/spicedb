package genutil

import (
	"buf.build/go/protovalidate"
)

// MustNewProtoValidator wraps protovalidate.New() to panic
// if the validator can't be constructed.
func MustNewProtoValidator() protovalidate.Validator {
	return protovalidate.GlobalValidator
}
