package common

import (
	"encoding/json"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/types/known/structpb"
)

type TransactionMetadata map[string]any

func (tm TransactionMetadata) Equals(other TransactionMetadata) bool {
	return reflect.DeepEqual(tm, other)
}

func (tm TransactionMetadata) Len() int {
	return len(tm)
}

func (tm TransactionMetadata) MustStruct() *structpb.Struct {
	structpbMetadata, err := structpb.NewStruct(tm)
	if err != nil {
		panic("failed to convert metadata to structpb" + err.Error())
	}
	return structpbMetadata
}

func (tm *TransactionMetadata) Scan(val any) error {
	if val == nil {
		clear(*tm)
		*tm = nil
		return nil
	}

	v, ok := val.([]byte)
	if !ok {
		return fmt.Errorf("unsupported type: %T", v)
	}

	clear(*tm)
	return json.Unmarshal(v, tm)
}
