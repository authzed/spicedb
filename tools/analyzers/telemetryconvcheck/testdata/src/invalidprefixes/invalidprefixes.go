package invalidprefixes

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

// Test function using a hardcoded string with an invalid prefix
func invalidPrefixesInEvents(span Span) {
	span.AddEvent("spicedb.caveats.names_collected") // want "use a constant from github.com/authzed/spicedb/internal/telemetry/otelconv for event name instead of the hardcoded string \"spicedb.caveats.names_collected\""
}
