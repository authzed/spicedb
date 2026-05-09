package wire

// FormatCode represents the encoding format of a given column
type FormatCode int16

const (
	// TextFormat is the default, text format.
	TextFormat FormatCode = 0
	// BinaryFormat is an alternative, binary, encoding.
	BinaryFormat FormatCode = 1
)
