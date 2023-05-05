package types

// CustomType is the interface for custom-defined types.
type CustomType interface {
	// SerializedString returns the serialized string form of the data within
	// this instance of the type.
	SerializedString() string
}
