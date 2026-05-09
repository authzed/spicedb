package jsonrpc2

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// Request represents a JSON-RPC request or
// notification. See
// http://www.jsonrpc.org/specification#request_object and
// http://www.jsonrpc.org/specification#notification.
type Request struct {
	Method string           `json:"method"`
	Params *json.RawMessage `json:"params,omitempty"`
	ID     ID               `json:"id"`
	Notif  bool             `json:"-"`

	// Meta optionally provides metadata to include in the request.
	//
	// NOTE: It is not part of spec. However, it is useful for propagating
	// tracing context, etc.
	Meta *json.RawMessage `json:"meta,omitempty"`

	// ExtraFields optionally adds fields to the root of the JSON-RPC request.
	//
	// NOTE: It is not part of the spec, but there are other protocols based on
	// JSON-RPC 2 that require it.
	ExtraFields []RequestField `json:"-"`
}

// MarshalJSON implements json.Marshaler and adds the "jsonrpc":"2.0"
// property.
func (r Request) MarshalJSON() ([]byte, error) {
	r2 := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  r.Method,
	}
	for _, field := range r.ExtraFields {
		r2[field.Name] = field.Value
	}
	if !r.Notif {
		r2["id"] = &r.ID
	}
	if r.Params != nil {
		r2["params"] = r.Params
	}
	if r.Meta != nil {
		r2["meta"] = r.Meta
	}
	return json.Marshal(r2)
}

// UnmarshalJSON implements json.Unmarshaler.
func (r *Request) UnmarshalJSON(data []byte) error {
	r2 := make(map[string]interface{})
	pop := func(key string) interface{} {
		defer delete(r2, key)
		return r2[key]
	}

	// Detect if the "params" or "meta" fields are JSON "null" or just not
	// present by seeing if the field gets overwritten to nil.
	emptyParams := &json.RawMessage{}
	r2["params"] = emptyParams
	emptyMeta := &json.RawMessage{}
	r2["meta"] = emptyMeta

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&r2); err != nil {
		return err
	}

	var ok bool
	r.Method, ok = pop("method").(string)
	if !ok {
		return errors.New("missing method field")
	}
	switch params := pop("params"); params {
	case nil:
		r.Params = &jsonNull
	case emptyParams:
		r.Params = nil
	default:
		b, err := json.Marshal(params)
		if err != nil {
			return fmt.Errorf("failed to marshal params: %w", err)
		}
		r.Params = (*json.RawMessage)(&b)
	}
	switch meta := pop("meta"); meta {
	case nil:
		r.Meta = &jsonNull
	case emptyMeta:
		r.Meta = nil
	default:
		b, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("failed to marshal Meta: %w", err)
		}
		r.Meta = (*json.RawMessage)(&b)
	}
	switch rawID := pop("id").(type) {
	case nil:
		r.ID = ID{}
		r.Notif = true
	case string:
		r.ID = ID{Str: rawID, IsString: true}
		r.Notif = false
	case json.Number:
		id, err := rawID.Int64()
		if err != nil {
			return fmt.Errorf("failed to unmarshal ID: %w", err)
		}
		r.ID = ID{Num: uint64(id)}
		r.Notif = false
	default:
		return fmt.Errorf("unexpected ID type: %T", rawID)
	}

	// The jsonrpc field should not be added to ExtraFields.
	delete(r2, "jsonrpc")

	// Clear the extra fields before populating them again.
	r.ExtraFields = nil
	for name, value := range r2 {
		r.ExtraFields = append(r.ExtraFields, RequestField{
			Name:  name,
			Value: value,
		})
	}
	return nil
}

// SetParams sets r.Params to the JSON encoding of v. If JSON
// marshaling fails, it returns an error.
func (r *Request) SetParams(v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	r.Params = (*json.RawMessage)(&b)
	return nil
}

// SetMeta sets r.Meta to the JSON encoding of v. If JSON
// marshaling fails, it returns an error.
func (r *Request) SetMeta(v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	r.Meta = (*json.RawMessage)(&b)
	return nil
}

// SetExtraField adds an entry to r.ExtraFields, so that it is added to the
// JSON encoding of the request, as a way to add arbitrary extensions to
// JSON RPC 2.0. If JSON marshaling fails, it returns an error.
func (r *Request) SetExtraField(name string, v interface{}) error {
	switch name {
	case "id", "jsonrpc", "meta", "method", "params":
		return fmt.Errorf("invalid extra field %q", name)
	}
	r.ExtraFields = append(r.ExtraFields, RequestField{
		Name:  name,
		Value: v,
	})
	return nil
}

// RequestField is a top-level field that can be added to the JSON-RPC request.
type RequestField struct {
	Name  string
	Value interface{}
}
