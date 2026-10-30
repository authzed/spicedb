package signals

import (
	"errors"
	"reflect"
	"strings"

	"github.com/ccoveille/go-safecast/v2"

	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

// Value-only keys make retention predictable: no mutable object graph can hide
// behind a pointer or interface. Exported fields let the store own string data.
func supportedKey(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.String:
		return true
	case reflect.Array:
		return supportedKey(t.Elem())
	case reflect.Struct:
		for i := range t.NumField() {
			f := t.Field(i)
			if !f.IsExported() || !supportedKey(f.Type) {
				return false
			}
		}

		return true
	default:
		return false
	}
}

func validateKey(key any, expected reflect.Type) error {
	if reflect.TypeOf(key) != expected || !reflect.ValueOf(key).Comparable() {
		return errors.New("invalid signal key type")
	}

	// NaN, including a NaN inside an array or struct, cannot be found or deleted
	// from a map. Reject it before lookup as well as before admission.
	if !reflect.ValueOf(key).Equal(reflect.ValueOf(key)) {
		return errors.New("nonreflexive signal key")
	}

	return nil
}

func ownedKey(value reflect.Value) reflect.Value {
	switch value.Kind() {
	case reflect.String:
		snapshot := reflect.New(value.Type()).Elem()
		snapshot.SetString(strings.Clone(value.String()))
		return snapshot
	case reflect.Struct:
		snapshot := reflect.New(value.Type()).Elem()

		for i := range value.NumField() {
			snapshot.Field(i).Set(ownedKey(value.Field(i)))
		}

		return snapshot
	case reflect.Array:
		snapshot := reflect.New(value.Type()).Elem()

		for i := range value.Len() {
			snapshot.Index(i).Set(ownedKey(value.Index(i)))
		}

		return snapshot
	default:
		return value
	}
}

// String headers and inline fields are in Type.Size; only backing bytes are extra.
func keyStringBytes(value reflect.Value) int64 {
	switch value.Kind() {
	case reflect.String:
		return int64(value.Len())
	case reflect.Struct:
		var size int64

		for i := range value.NumField() {
			size += keyStringBytes(value.Field(i))
		}

		return size
	case reflect.Array:
		var size int64

		for i := range value.Len() {
			size += keyStringBytes(value.Index(i))
		}

		return size
	default:
		return 0
	}
}

func ownedFootprint(f signal.Footprint) signal.Footprint {
	result := signal.Footprint{
		Elements: make([]signal.Element, len(f.Elements)),
		Caveats:  make([]string, len(f.Caveats)),
	}

	for i, e := range f.Elements {
		result.Elements[i] = signal.Element{
			Namespace: strings.Clone(e.Namespace),
			Relation:  strings.Clone(e.Relation),
		}
	}

	for i, name := range f.Caveats {
		result.Caveats[i] = strings.Clone(name)
	}

	return result
}

func entryCost(key entryKey, state signal.State, footprint signal.Footprint) int64 {
	// Include map/entry overhead, retained names, inline key storage, string
	// backing bytes and the dependency slices as well as provider-owned state.
	size := int64(256)
	size += safecast.MustConvert[int64](len(key.schema))
	size += safecast.MustConvert[int64](len(key.name))
	size += safecast.MustConvert[int64](len(key.revision))
	size += safecast.MustConvert[int64](reflect.TypeOf(key.key).Size()) + keyStringBytes(reflect.ValueOf(key.key))
	size += safecast.MustConvert[int64](len(footprint.Elements)) * safecast.MustConvert[int64](reflect.TypeFor[signal.Element]().Size())
	size += safecast.MustConvert[int64](len(footprint.Caveats)) * safecast.MustConvert[int64](reflect.TypeFor[string]().Size())

	for _, e := range footprint.Elements {
		size += safecast.MustConvert[int64](len(e.Namespace)) + safecast.MustConvert[int64](len(e.Relation))
	}

	for _, name := range footprint.Caveats {
		size += safecast.MustConvert[int64](len(name))
	}

	return size + state.Size()
}
