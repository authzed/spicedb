package generator

import (
	"strings"
)

type sourceGenerator struct {
	buf                strings.Builder // The buffer for the new source code.
	indentationLevel   int             // The current indentation level.
	hasNewline         bool            // Whether there is a newline at the end of the buffer.
	hasBlankline       bool            // Whether there is a blank line at the end of the buffer.
	hasIssue           bool            // Whether there is a translation issue.
	hasNewScope        bool            // Whether there is a new scope at the end of the buffer.
	existingLineLength int             // Length of the existing line.
}

// ensureBlankLineOrNewScope ensures that there is a blank line or new scope at the tail of the buffer. If not,
// a new line is added.
func (sf *sourceGenerator) ensureBlankLineOrNewScope() {
	if !sf.hasBlankline && !sf.hasNewScope {
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

// appendIssue adds an issue found in generation.
func (sf *sourceGenerator) appendIssue(description string) {
	sf.append("/* ")
	sf.append(description)
	sf.append(" */")
	sf.hasIssue = true
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
		sf.hasNewScope = false

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

func (sf *sourceGenerator) markNewScope() {
	sf.hasNewScope = true
}
