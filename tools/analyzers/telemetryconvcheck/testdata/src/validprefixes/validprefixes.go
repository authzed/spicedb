package validprefixes

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

// otelconv constants
var otelconv struct {
	// Constants with spicedb.internal. prefix
	EventInternalCaveatsNamesCollected string
	AttrInternalDatastoreColumnCount   string

	// Constants with spicedb.external. prefix
	EventExternalDatastoreRowsReturned string
	AttrExternalDispatchCached         string
}

func init() {
	// Initialize with valid prefixes
	otelconv.EventInternalCaveatsNamesCollected = "spicedb.internal.caveats.names_collected"
	otelconv.AttrInternalDatastoreColumnCount = "spicedb.internal.datastore.column_count"

	otelconv.EventExternalDatastoreRowsReturned = "spicedb.external.datastore.rows_returned"
	otelconv.AttrExternalDispatchCached = "spicedb.external.dispatch.cached"
}

func validInternalPrefixes(span Span) {
	span.AddEvent(otelconv.EventInternalCaveatsNamesCollected)
	span.SetAttributes(
		attribute.String(otelconv.AttrInternalDatastoreColumnCount, "read"),
	)
}

func validExternalPrefixes(span Span) {
	span.AddEvent(otelconv.EventExternalDatastoreRowsReturned)
	span.SetAttributes(
		attribute.String(otelconv.AttrExternalDispatchCached, "true"),
	)
}

func validMixedPrefixes() SpanStartOption {
	return trace.WithAttributes(
		attribute.String(otelconv.AttrInternalDatastoreColumnCount, "write"),
		attribute.String(otelconv.AttrExternalDispatchCached, "false"),
	)
}
