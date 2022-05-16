package caching

import (
	"reflect"
	"sync"
)

var processingPool = sync.Pool{
	New: func() interface{} {
		return []reflect.Value{}
	},
}

func isNativeType(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return true
	}
	return false
}

// Sizeof returns the estimated memory usage of object(s) not just the size of the type.
// On 64bit Sizeof("test") == 12 (8 = sizeof(StringHeader) + 4 bytes).
func Sizeof(objs ...interface{}) (sz uint64) {
	refmap := make(map[uintptr]bool)
	processing := processingPool.Get().([]reflect.Value)[:0]
	for i := range objs {
		processing = append(processing, reflect.ValueOf(objs[i]))
	}

	for len(processing) > 0 {
		var val reflect.Value
		val, processing = processing[len(processing)-1], processing[:len(processing)-1]
		if !val.IsValid() {
			continue
		}

		if val.CanAddr() {
			refmap[val.Addr().Pointer()] = true
		}

		typ := val.Type()

		sz += uint64(typ.Size())

		switch val.Kind() {
		case reflect.Ptr:
			if val.IsNil() {
				break
			}
			if refmap[val.Pointer()] {
				break
			}

			fallthrough
		case reflect.Interface:
			processing = append(processing, val.Elem())

		case reflect.Struct:
			sz -= uint64(typ.Size())
			for i := 0; i < val.NumField(); i++ {
				processing = append(processing, val.Field(i))
			}

		case reflect.Array:
			if isNativeType(typ.Elem().Kind()) {
				break
			}
			sz -= uint64(typ.Size())
			for i := 0; i < val.Len(); i++ {
				processing = append(processing, val.Index(i))
			}
		case reflect.Slice:
			el := typ.Elem()
			if isNativeType(el.Kind()) {
				sz += uint64(val.Len()) * uint64(el.Size())
				break
			}
			for i := 0; i < val.Len(); i++ {
				processing = append(processing, val.Index(i))
			}
		case reflect.Map:
			if val.IsNil() {
				break
			}
			kel, vel := typ.Key(), typ.Elem()
			if isNativeType(kel.Kind()) && isNativeType(vel.Kind()) {
				sz += uint64(kel.Size()+vel.Size()) * uint64(val.Len())
				break
			}
			keys := val.MapKeys()
			for i := 0; i < len(keys); i++ {
				processing = append(processing, keys[i])
				processing = append(processing, val.MapIndex(keys[i]))
			}
		case reflect.String:
			sz += uint64(val.Len())
		}
	}
	processingPool.Put(processing)
	return
}
