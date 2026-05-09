package slogcommon

import (
	"encoding"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"runtime"
	"slices"
	"strings"

	"github.com/samber/lo"
)

type ReplaceAttrFn = func(groups []string, a slog.Attr) slog.Attr

func AppendRecordAttrsToAttrs(attrs []slog.Attr, groups []string, record *slog.Record) []slog.Attr {
	output := make([]slog.Attr, 0, len(attrs)+record.NumAttrs())
	output = append(output, attrs...)

	record.Attrs(func(attr slog.Attr) bool {
		for i := len(groups) - 1; i >= 0; i-- {
			attr = slog.Group(groups[i], attr)
		}
		output = append(output, attr)
		return true
	})

	return output
}

func ReplaceAttrs(fn ReplaceAttrFn, groups []string, attrs ...slog.Attr) []slog.Attr {
	for i := range attrs {
		attr := attrs[i]
		value := attr.Value.Resolve()
		if value.Kind() == slog.KindGroup {
			attrs[i].Value = slog.GroupValue(ReplaceAttrs(fn, append(groups, attr.Key), value.Group()...)...)
		} else if fn != nil {
			attrs[i] = fn(groups, attr)
		}
	}

	return attrs
}

func AttrsToMap(attrs ...slog.Attr) map[string]any {
	output := map[string]any{}

	attrsByKey := groupValuesByKey(attrs)
	for k, values := range attrsByKey {
		v := mergeAttrValues(values...)
		if v.Kind() == slog.KindGroup {
			output[k] = AttrsToMap(v.Group()...)
		} else {
			output[k] = v.Any()
		}
	}

	return output
}

func RecordToAttrsMap(r slog.Record) map[string]any {
	attrs := make([]slog.Attr, r.NumAttrs())
	r.Attrs(func(attr slog.Attr) bool {
		attrs = append(attrs, attr)
		return true
	})

	return AttrsToMap(attrs...)
}

func groupValuesByKey(attrs []slog.Attr) map[string][]slog.Value {
	result := map[string][]slog.Value{}

	for _, item := range attrs {
		key := item.Key
		result[key] = append(result[key], item.Value)
	}

	return result
}

func mergeAttrValues(values ...slog.Value) slog.Value {
	v := values[0]

	for i := 1; i < len(values); i++ {
		if v.Kind() != slog.KindGroup || values[i].Kind() != slog.KindGroup {
			v = values[i]
			continue
		}

		v = slog.GroupValue(append(v.Group(), values[i].Group()...)...)
	}

	return v
}

func AttrToValue(attr slog.Attr) (string, any) {
	k := attr.Key
	v := attr.Value
	kind := v.Kind()

	switch kind {
	case slog.KindAny:
		return k, v.Any()
	case slog.KindLogValuer:
		return k, v.Any()
	case slog.KindGroup:
		return k, AttrsToMap(v.Group()...)
	case slog.KindInt64:
		return k, v.Int64()
	case slog.KindUint64:
		return k, v.Uint64()
	case slog.KindFloat64:
		return k, v.Float64()
	case slog.KindString:
		return k, v.String()
	case slog.KindBool:
		return k, v.Bool()
	case slog.KindDuration:
		return k, v.Duration()
	case slog.KindTime:
		return k, v.Time().UTC()
	default:
		return k, AnyValueToString(v)
	}
}

func AnyValueToString(v slog.Value) string {
	if tm, ok := v.Any().(encoding.TextMarshaler); ok {
		data, err := tm.MarshalText()
		if err != nil {
			return ""
		}

		return string(data)
	}

	return fmt.Sprintf("%+v", v.Any())
}

func AttrsToString(attrs ...slog.Attr) map[string]string {
	output := make(map[string]string, len(attrs))

	for i := range attrs {
		attr := attrs[i]
		k, v := attr.Key, attr.Value
		output[k] = ValueToString(v)
	}

	return output
}

func ValueToString(v slog.Value) string {
	switch v.Kind() {
	case slog.KindAny, slog.KindLogValuer, slog.KindGroup:
		return AnyValueToString(v)
	case slog.KindInt64, slog.KindUint64, slog.KindFloat64, slog.KindString, slog.KindBool, slog.KindDuration:
		return v.String()
	case slog.KindTime:
		return v.Time().UTC().String()
	default:
		return AnyValueToString(v)
	}
}

func ReplaceError(attrs []slog.Attr, errorKeys ...string) []slog.Attr {
	replaceAttr := func(groups []string, a slog.Attr) slog.Attr {
		if len(groups) > 1 {
			return a
		}

		for i := range errorKeys {
			if a.Key == errorKeys[i] {
				if err, ok := a.Value.Any().(error); ok {
					return slog.Any(a.Key, FormatError(err))
				}
			}
		}
		return a
	}
	return ReplaceAttrs(replaceAttr, []string{}, attrs...)
}

func ExtractError(attrs []slog.Attr, errorKeys ...string) ([]slog.Attr, error) {
	for i := range attrs {
		attr := attrs[i]

		if !slices.Contains(errorKeys, attr.Key) {
			continue
		}

		if err, ok := attr.Value.Resolve().Any().(error); ok {
			output := make([]slog.Attr, 0, len(attrs)-1)
			output = append(output, attrs[:i]...)
			output = append(output, attrs[i+1:]...)
			return output, err
		}
	}

	return attrs, nil
}

func FormatErrorKey(values map[string]any, errorKeys ...string) map[string]any {
	for _, errorKey := range errorKeys {
		if err, ok := values[errorKey]; ok {
			if e, ok := err.(error); ok {
				values[errorKey] = FormatError(e)
				break
			}
		}
	}

	return values
}

func FormatError(err error) map[string]any {
	return map[string]any{
		"kind":  reflect.TypeOf(err).String(),
		"error": err.Error(),
		"stack": nil, // @TODO
	}
}

func FormatRequest(req *http.Request, ignoreHeaders bool) map[string]any {
	output := map[string]any{
		"host":   req.Host,
		"method": req.Method,
		"url": map[string]any{
			"url":       req.URL.String(),
			"scheme":    req.URL.Scheme,
			"host":      req.URL.Host,
			"path":      req.URL.Path,
			"raw_query": req.URL.RawQuery,
			"fragment":  req.URL.Fragment,
			"query": lo.MapEntries(req.URL.Query(), func(key string, values []string) (string, string) {
				return key, strings.Join(values, ",")
			}),
		},
	}

	if !ignoreHeaders {
		output["headers"] = lo.MapEntries(req.Header, func(key string, values []string) (string, string) {
			return key, strings.Join(values, ",")
		})
	}

	return output
}

func Source(sourceKey string, r *slog.Record) slog.Attr {
	fs := runtime.CallersFrames([]uintptr{r.PC})
	f, _ := fs.Next()
	var args []any
	if f.Function != "" {
		args = append(args, slog.String("function", f.Function))
	}
	if f.File != "" {
		args = append(args, slog.String("file", f.File))
	}
	if f.Line != 0 {
		args = append(args, slog.Int("line", f.Line))
	}

	return slog.Group(sourceKey, args...)
}

func StringSource(sourceKey string, r *slog.Record) slog.Attr {
	fs := runtime.CallersFrames([]uintptr{r.PC})
	f, _ := fs.Next()
	return slog.String(sourceKey, fmt.Sprintf("%s:%d (%s)", f.File, f.Line, f.Function))
}

func FindAttribute(attrs []slog.Attr, groups []string, key string) (slog.Attr, bool) {
	// group traversal
	if len(groups) > 0 {
		for _, attr := range attrs {
			if attr.Value.Kind() == slog.KindGroup && attr.Key == groups[0] {
				attr, found := FindAttribute(attr.Value.Group(), groups[1:], key)
				if found {
					return attr, true
				}
			}
		}

		return slog.Attr{}, false
	}

	// starting here, groups is empty
	for _, attr := range attrs {
		if attr.Key == key {
			return attr, true
		}
	}

	return slog.Attr{}, false
}

func RemoveEmptyAttrs(attrs []slog.Attr) []slog.Attr {
	return lo.FilterMap(attrs, func(attr slog.Attr, _ int) (slog.Attr, bool) {
		if attr.Key == "" {
			return attr, false
		}

		if attr.Value.Kind() == slog.KindGroup {
			values := RemoveEmptyAttrs(attr.Value.Group())
			if len(values) == 0 {
				return attr, false
			}

			attr.Value = slog.GroupValue(values...)
			return attr, true
		}

		return attr, !attr.Value.Equal(slog.Value{})
	})
}
