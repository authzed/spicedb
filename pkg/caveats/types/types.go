package types

import (
	"fmt"
	"strings"

	"github.com/authzed/cel-go/cel"
)

// VariableType defines the supported types of variables in caveats.
type VariableType struct {
	localName  string
	celType    *cel.Type
	childTypes []VariableType
	converter  typedValueConverter
}

// CelType returns the underlying CEL type for the variable type.
func (vt VariableType) CelType() *cel.Type {
	return vt.celType
}

func (vt VariableType) String() string {
	if len(vt.childTypes) > 0 {
		childTypeStrings := make([]string, 0, len(vt.childTypes))
		for _, childType := range vt.childTypes {
			childTypeStrings = append(childTypeStrings, childType.String())
		}

		return vt.localName + "<" + strings.Join(childTypeStrings, ", ") + ">"
	}

	return vt.localName
}

// ConvertValue converts the given value into one expected by this variable type.
func (vt VariableType) ConvertValue(value any) (any, error) {
	converted, err := vt.converter(value)
	if err != nil {
		return nil, fmt.Errorf("for %s: %w", vt.String(), err)
	}

	return converted, nil
}
