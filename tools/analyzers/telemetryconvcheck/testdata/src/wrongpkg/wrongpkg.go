package wrongpkg

// Placeholder types
type (
	Span            struct{}
	KeyValue        struct{}
	SpanStartOption struct{}
)

// Placeholder packages
var attribute attributePackage
var trace tracePackage

type attributePackage struct{}
type tracePackage struct{}

// Placeholder functions
func (s Span) AddEvent(name string, options ...interface{}) {}
func (s Span) SetAttributes(attributes ...KeyValue)         {}

func (a attributePackage) String(key string, value string) KeyValue         { return KeyValue{} }
func (a attributePackage) Int64(key string, value int64) KeyValue           { return KeyValue{} }
func (a attributePackage) Bool(key string, value bool) KeyValue             { return KeyValue{} }
func (a attributePackage) StringSlice(key string, values []string) KeyValue { return KeyValue{} }
func (a attributePackage) Int64Slice(key string, values []int64) KeyValue   { return KeyValue{} }
func (a attributePackage) BoolSlice(key string, values []bool) KeyValue     { return KeyValue{} }
func (t tracePackage) WithAttributes(attributes ...KeyValue) SpanStartOption {
	return SpanStartOption{}
}

// Different package with constants (not the one specified by conv-pkg flag's default value)
var wrongconv struct {
	EventWrongPackage1 string
	EventWrongPackage2 string
	AttrWrongPackage1  string
	AttrWrongPackage2  string
}

func init() {
	// valid prefixes but in the wrong package
	wrongconv.EventWrongPackage1 = "spicedb.internal.wrong.event1" // want "use a constant from `github.com/authzed/spicedb/internal/telemetry/otelconv` for event name, not from `wrongpkg`"
	wrongconv.EventWrongPackage2 = "spicedb.external.wrong.event2" // want "use a constant from `github.com/authzed/spicedb/internal/telemetry/otelconv` for event name, not from `wrongpkg`"

	wrongconv.AttrWrongPackage1 = "spicedb.internal.wrong.attr1" // want "use a constant from `github.com/authzed/spicedb/internal/telemetry/otelconv` for attribute key, not from `wrongpkg`"
	wrongconv.AttrWrongPackage2 = "spicedb.external.wrong.attr2" // want "use a constant from `github.com/authzed/spicedb/internal/telemetry/otelconv` for attribute key, not from `wrongpkg`"
}

func wrongPackageEvents(span Span) {
	// These should be flagged because the constants are not from the package specified by conv-pkg flag
	span.AddEvent(wrongconv.EventWrongPackage1)
	span.AddEvent(wrongconv.EventWrongPackage2)
}

func wrongPackageAttributes(span Span) {
	span.SetAttributes(
		// These should be flagged because the constants are not from the package specified by conv-pkg flag
		attribute.String(wrongconv.AttrWrongPackage1, "read"),
		attribute.String(wrongconv.AttrWrongPackage2, "write"),
	)
}

func wrongPackageWithAttributes() SpanStartOption {
	return trace.WithAttributes(
		// These should be flagged because the constants are not from the package specified by conv-pkg flag
		attribute.String(wrongconv.AttrWrongPackage1, "query"),
		attribute.String(wrongconv.AttrWrongPackage2, "execute"),
	)
}
