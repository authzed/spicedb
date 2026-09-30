package testutil

import (
	"reflect"
	"testing"
)

// NonZeroValue returns a non-zero value of typ.
// A struct gets a non-zero value in its first exported field.
// A func returns zero values. An unsupported kind fails the test.
func NonZeroValue(tb testing.TB, typ reflect.Type) reflect.Value {
	tb.Helper()
	v := reflect.New(typ).Elem()
	switch typ.Kind() {
	case reflect.String:
		v.SetString("x")
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		v.SetUint(1)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(1)
	case reflect.Slice:
		s := reflect.MakeSlice(typ, 1, 1)
		s.Index(0).Set(NonZeroValue(tb, typ.Elem()))
		v.Set(s)
	case reflect.Map:
		m := reflect.MakeMap(typ)
		m.SetMapIndex(NonZeroValue(tb, typ.Key()), NonZeroValue(tb, typ.Elem()))
		v.Set(m)
	case reflect.Pointer:
		p := reflect.New(typ.Elem())
		p.Elem().Set(NonZeroValue(tb, typ.Elem()))
		v.Set(p)
	case reflect.Func:
		v.Set(reflect.MakeFunc(typ, func([]reflect.Value) []reflect.Value {
			out := make([]reflect.Value, typ.NumOut())
			for i := range out {
				out[i] = reflect.Zero(typ.Out(i))
			}
			return out
		}))
	case reflect.Struct:
		for i := range typ.NumField() {
			if typ.Field(i).IsExported() {
				v.Field(i).Set(NonZeroValue(tb, typ.Field(i).Type))
				return v
			}
		}
		tb.Fatalf("struct %s has no exported field", typ)
	default:
		tb.Fatalf("NonZeroValue does not support kind %s of %s", typ.Kind(), typ)
	}
	return v
}
