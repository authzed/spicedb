package _invalidtelemetry

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

func invalidEventNames(span Span) {
	span.AddEvent("spicedb.datastore.read") // want "use a constant from github.com/authzed/spicedb/internal/telemetry/otelconv for event name instead of the hardcoded string \"spicedb.datastore.read\""
}

func invalidAttributeKeys() []KeyValue {
	return []KeyValue{
		attribute.String("spicedb.datastore.operation", "read"), // want "use a constant from github.com/authzed/spicedb/internal/telemetry/otelconv for attribute key instead of the hardcoded string \"spicedb.datastore.operation\""
	}
}

func invalidSetAttributes(span Span) {
	attr := attribute.String("spicedb.datastore.operation", "write") // want "use a constant from github.com/authzed/spicedb/internal/telemetry/otelconv for attribute key instead of the hardcoded string \"spicedb.datastore.operation\""
	span.SetAttributes(attr)
}

func invalidWithAttributes() SpanStartOption {
	attr := attribute.String("spicedb.datastore.operation", "query") // want "use a constant from github.com/authzed/spicedb/internal/telemetry/otelconv for attribute key instead of the hardcoded string \"spicedb.datastore.operation\""
	return trace.WithAttributes(attr)
}

func invalidAttributeSlices() []KeyValue {
	return []KeyValue{
		attribute.StringSlice("spicedb.datastore.operations", []string{"read", "write"}), // want "use a constant from github.com/authzed/spicedb/internal/telemetry/otelconv for attribute key instead of the hardcoded string \"spicedb.datastore.operations\""
	}
}

func invalidCompositeLiterals() []KeyValue {
	attrs := []KeyValue{
		attribute.String("spicedb.datastore.operation", "read"), // want "use a constant from github.com/authzed/spicedb/internal/telemetry/otelconv for attribute key instead of the hardcoded string \"spicedb.datastore.operation\""
	}
	return attrs
}
