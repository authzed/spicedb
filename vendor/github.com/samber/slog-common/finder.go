package slogcommon

import "log/slog"

func FindAttrByKey(attrs []slog.Attr, key string) (slog.Attr, bool) {
	for i := range attrs {
		if attrs[i].Key == key {
			return attrs[i], true
		}
	}

	return slog.Attr{}, false
}

func FindAttrByGroupAndKey(attrs []slog.Attr, groups []string, key string) (slog.Attr, bool) {
	if len(groups) == 0 {
		return FindAttrByKey(attrs, key)
	}

	for i := range attrs {
		if attrs[i].Key == key && attrs[i].Value.Kind() == slog.KindGroup {
			return FindAttrByGroupAndKey(attrs[i].Value.Group(), groups[1:], key)
		}
	}

	return slog.Attr{}, false
}
