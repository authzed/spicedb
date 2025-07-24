package prefixcheck

// NOTE: These definitions are just to mimic the OpenTelemetry API without importing packages
// because importing in these tests is difficult.

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

// Test functions with hardcoded strings that have invalid prefixes
func invalidPrefixEventNames(span Span) {
	span.AddEvent("spicedb.caveats.names_collected") // want "use a constant from github.com/authzed/spicedb/internal/telemetry/otelconv for event name instead of the hardcoded string \"spicedb.caveats.names_collected\""
	span.AddEvent("invalid.prefix.event") // want "use a constant from github.com/authzed/spicedb/internal/telemetry/otelconv for event name instead of the hardcoded string \"invalid.prefix.event\""
}

func invalidPrefixAttributeKeys() []KeyValue {
	return []KeyValue{
		attribute.String("spicedb.datastore.operation", "read"), // want "use a constant from github.com/authzed/spicedb/internal/telemetry/otelconv for attribute key instead of the hardcoded string \"spicedb.datastore.operation\""
		attribute.String("another.invalid.prefix", "write"), // want "use a constant from github.com/authzed/spicedb/internal/telemetry/otelconv for attribute key instead of the hardcoded string \"another.invalid.prefix\""
	}
}

func invalidPrefixSetAttributes(span Span) {
	span.SetAttributes(
		attribute.String("spicedb.datastore.operation", "write"), // want "use a constant from github.com/authzed/spicedb/internal/telemetry/otelconv for attribute key instead of the hardcoded string \"spicedb.datastore.operation\""
		attribute.String("another.invalid.prefix", "read"), // want "use a constant from github.com/authzed/spicedb/internal/telemetry/otelconv for attribute key instead of the hardcoded string \"another.invalid.prefix\""
	)
}

func invalidPrefixWithAttributes() SpanStartOption {
	return trace.WithAttributes(
		attribute.String("spicedb.datastore.operation", "query"), // want "use a constant from github.com/authzed/spicedb/internal/telemetry/otelconv for attribute key instead of the hardcoded string \"spicedb.datastore.operation\""
		attribute.String("another.invalid.prefix", "execute"), // want "use a constant from github.com/authzed/spicedb/internal/telemetry/otelconv for attribute key instead of the hardcoded string \"another.invalid.prefix\""
	)
}