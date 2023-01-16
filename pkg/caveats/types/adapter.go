package types

import (
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// CustomTypeAdapter implements a CEL type adapter for handling the custom defined types.
type CustomTypeAdapter struct{}

func (CustomTypeAdapter) NativeToValue(value interface{}) ref.Val {
	converted, ok := value.(ref.Val)
	if ok {
		return converted
	}

	return types.DefaultTypeAdapter.NativeToValue(value)
}
