package _validtelemetry

// otelconv constants
var otelconv struct {
	EventCaveatsNamesCollected            string
	EventDatastoreCommonRowsFirstReturned string
	AttrDatastoreColumnCount              string
	AttrDatastoreRelationshipsCount       string
	AttrDispatchCached                    string
	AttrDatastorePostgresLogLevel         string
	AttrDatastoreRelationCount            string
	AttrCaveatsNames                      string
	AttrDatastoreNamespaceCount           string
	AttrCaveatsOperations                 string
}

func init() {
	otelconv.EventCaveatsNamesCollected = "spicedb.caveats.names_collected"
	otelconv.EventDatastoreCommonRowsFirstReturned = "spicedb.datastore.common.first_row_returned"
	otelconv.AttrDatastoreColumnCount = "spicedb.datastore.common.column_count"
	otelconv.AttrDatastoreRelationshipsCount = "spicedb.datastore.common.relationship_count"
	otelconv.AttrDispatchCached = "spicedb.dispatch.cached"
	otelconv.AttrDatastorePostgresLogLevel = "spicedb.datastore.postgres.log_level"
	otelconv.AttrDatastoreRelationCount = "spicedb.datastore.spanner.relation_count"
	otelconv.AttrCaveatsNames = "spicedb.caveats.names"
	otelconv.AttrDatastoreNamespaceCount = "spicedb.datastore.spanner.namespace_count"
	otelconv.AttrCaveatsOperations = "spicedb.caveats.operations"
}

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

func validEventNames(span Span) {
	span.AddEvent(otelconv.EventCaveatsNamesCollected)
	span.AddEvent(otelconv.EventDatastoreCommonRowsFirstReturned)
}

func validAttributeKeys() []KeyValue {
	return []KeyValue{
		attribute.String(otelconv.AttrDatastoreColumnCount, "read"),
		attribute.Int64(otelconv.AttrDatastoreRelationshipsCount, 10),
		attribute.Bool(otelconv.AttrDispatchCached, true),
	}
}

func validSetAttributes(span Span) {
	span.SetAttributes(
		attribute.String(otelconv.AttrDatastorePostgresLogLevel, "write"),
		attribute.Int64(otelconv.AttrDatastoreRelationCount, 5),
	)
}

func validWithAttributes() SpanStartOption {
	return trace.WithAttributes(
		attribute.String(otelconv.AttrCaveatsNames, "query"),
		attribute.Int64(otelconv.AttrDatastoreNamespaceCount, 20),
	)
}

func validAttributeSlices() []KeyValue {
	return []KeyValue{
		attribute.StringSlice(otelconv.AttrCaveatsOperations, []string{"read", "write"}),
		attribute.Int64Slice(otelconv.AttrDatastoreRelationshipsCount, []int64{10, 20, 30}),
		attribute.BoolSlice(otelconv.AttrDispatchCached, []bool{true, false, true}),
	}
}
