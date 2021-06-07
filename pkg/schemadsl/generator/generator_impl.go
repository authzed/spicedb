package generator

import (
	"bytes"
	"strings"
)

type sourceGenerator struct {
	buf                bytes.Buffer // The buffer for the new source code.
	indentationLevel   int          // The current indentation level.
	hasNewline         bool         // Whether there is a newline at the end of the buffer.
	hasBlankline       bool         // Whether there is a blank line at the end of the buffer.
	existingLineLength int          // Length of the existing line.
}

// ensureBlankLine ensures that there is a blank line at the tail of the buffer. If not,
// a new line is added.
func (sf *sourceGenerator) ensureBlankLine() {
	if !sf.hasBlankline {
		sf.appendLine()
	}
}

// indent increases the current indentation.
func (sf *sourceGenerator) indent() {
	sf.indentationLevel = sf.indentationLevel + 1
}

// dedent decreases the current indentation.
func (sf *sourceGenerator) dedent() {
	sf.indentationLevel = sf.indentationLevel - 1
}

// appendRaw adds the given value to the buffer without indenting.
func (sf *sourceGenerator) appendRaw(value string) {
	if len(value) > 0 {
		sf.hasNewline = value[len(value)-1] == '\n'
	} else {
		sf.hasNewline = false
	}

	sf.buf.WriteString(value)
}

// append adds the given value to the buffer, indenting as necessary.
func (sf *sourceGenerator) append(value string) {
	for _, currentRune := range value {
		if currentRune == '\n' {
			if sf.hasNewline {
				sf.hasBlankline = true
			}

			sf.buf.WriteRune('\n')
			sf.hasNewline = true
			sf.existingLineLength = 0
			continue
		}

		sf.hasBlankline = false

		if sf.hasNewline {
			sf.buf.WriteString(strings.Repeat("\t", sf.indentationLevel))
			sf.hasNewline = false
			sf.existingLineLength += sf.indentationLevel
		}

		sf.existingLineLength++
		sf.buf.WriteRune(currentRune)
	}
}

// appendLine adds a newline.
func (sf *sourceGenerator) appendLine() {
	sf.append("\n")
}
