package dynamodb

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
)

var (
	replaceRe = regexp.MustCompile("\\${|}")
	literalRe = regexp.MustCompile("\\${\\w+?}")
)

type CompositeFields struct {
	pattern *regexp.Regexp
	fields  []string
}

type KeyValues map[string]*string

func NewCompositeFields(prefix string, fields ...string) *CompositeFields {
	pattern := ""
	if prefix != "" {
		pattern += prefix + "#"
	}

	for i, field := range fields {
		pattern += fmt.Sprintf("(?<%s>.*?)", field)
		if i != (len(fields) - 1) {
			pattern += "#"
		}
	}

	pregex := regexp.MustCompile("^" + pattern + "$")

	return &CompositeFields{
		pattern: pregex,
		fields:  fields,
	}
}

func NewCompositeFieldsWithoutPrefix(fields ...string) *CompositeFields {
	return NewCompositeFields("", fields...)
}

func (c CompositeFields) Build(kvp KeyValues) string {
	result, _ := c.BuildFull(kvp)
	return result
}

func (c CompositeFields) BuildFull(kvp KeyValues) (result string, remainingFields []string) {
	result, remainingFields, required := c.BuildPartial(kvp)

	if len(required) != 0 {
		return "", slices.Collect(maps.Keys(kvp))
	}

	return
}

func (c CompositeFields) BuildPartial(kvp map[string]*string) (result string, remainingFields []string, required []string) {
	remainingFields = slices.Collect(maps.Keys(kvp))
	required = slices.Clone(c.fields)

	result = c.pattern.String()

	result = strings.TrimPrefix(result, "^")
	result = strings.TrimSuffix(result, "$")

	for _, field := range c.fields {
		namedGroup := fmt.Sprintf("(?<%s>.*?)", field)

		if value, exists := kvp[field]; exists && value != nil {
			result = strings.ReplaceAll(result, namedGroup, *value)
		} else {
			i := strings.Index(result, "(?<")
			result = result[:i]
			break
		}

		iu := slices.Index(required, field)
		if iu != -1 {
			required = append(required[:iu], required[iu+1:]...)
		}

		i := slices.Index(remainingFields, field)
		if i != -1 {
			remainingFields = append(remainingFields[:i], remainingFields[i+1:]...)
		}
	}

	return
}

func (c CompositeFields) Check(availables []string) (unused []string, required []string) {
	unused = slices.Clone(availables)
	required = slices.Clone(c.fields)

	for _, field := range c.fields {
		if !slices.Contains(availables, field) {
			break
		}

		i := slices.Index(required, field)
		if i != -1 {
			required = append(required[:i], required[i+1:]...)
		}

		iu := slices.Index(unused, field)
		if iu != -1 {
			unused = append(unused[:iu], unused[iu+1:]...)
		}
	}

	return
}

func (c CompositeFields) Extract(str string, kvp KeyValues) {
	matches := c.pattern.FindStringSubmatch(str)
	if matches == nil {
		return
	}

	fields := c.pattern.SubexpNames()

	for i, field := range fields {
		_, exists := kvp[field]
		if i > 0 && field != "" && i < len(matches) && exists {
			*kvp[field] = matches[i]
		}
	}
}
