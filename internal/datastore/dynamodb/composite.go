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

func NewCompositeFields(pt string) *CompositeFields {

	pattern := pt
	rawFields := literalRe.FindAllString(pattern, -1)

	fields := make([]string, len(rawFields))

	for i, field := range rawFields {
		ext := replaceRe.ReplaceAllLiteralString(field, "")
		fields[i] = ext
		pattern = strings.ReplaceAll(pattern,
			fmt.Sprintf("${%s}", ext),
			fmt.Sprintf("(?<%s>.*?)", ext),
		)
	}

	ptRe := regexp.MustCompile("^" + pattern + "$")

	return &CompositeFields{
		pattern: ptRe,
		fields:  fields,
	}

}

func (c CompositeFields) Build(kvp KeyValues) string {
	result := c.pattern.String()

	result = strings.TrimPrefix(result, "^")
	result = strings.TrimSuffix(result, "$")

	if len(c.fields) == 0 {
		return ""
	}

	for _, field := range c.fields {
		namedGroup := fmt.Sprintf("(?<%s>.*?)", field)

		if value, exists := kvp[field]; exists && value != nil {
			result = strings.ReplaceAll(result, namedGroup, *value)
		} else {
			return ""
		}
	}

	return result
}

func (c CompositeFields) BuildPartial(kvp map[string]*string) (result string, remainingFields []string) {
	remainingFields = slices.Collect(maps.Keys(kvp))

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

		i := slices.Index(remainingFields, field)
		remainingFields = append(remainingFields[:i], remainingFields[i+1:]...)
	}

	return result, remainingFields
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
