package generator

import (
	"strings"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
)

type sourceGenerator struct {
	buf                strings.Builder   // The buffer for the new source code.
	indentationLevel   int               // The current indentation level.
	hasNewline         bool              // Whether there is a newline at the end of the buffer.
	hasBlankline       bool              // Whether there is a blank line at the end of the buffer.
	hasIssue           bool              // Whether there is a translation issue.
	hasNewScope        bool              // Whether there is a new scope at the end of the buffer.
	existingLineLength int               // Length of the existing line.
	flags              *mapz.Set[string] // The flags added while generating.
}

// ensureBlankLineOrNewScope ensures that there is a blank line or new scope at the tail of the buffer. If not,
// a new line is added.
func (sg *sourceGenerator) ensureBlankLineOrNewScope() {
	if !sg.hasBlankline && !sg.hasNewScope {
		sg.appendLine()
	}
}

// indent increases the current indentation.
func (sg *sourceGenerator) indent() {
	sg.indentationLevel = sg.indentationLevel + 1
}

// dedent decreases the current indentation.
func (sg *sourceGenerator) dedent() {
	sg.indentationLevel = sg.indentationLevel - 1
}

// appendIssue adds an issue found in generation.
func (sg *sourceGenerator) appendIssue(description string) {
	sg.append("/* ")
	sg.append(description)
	sg.append(" */")
	sg.hasIssue = true
}

// append adds the given value to the buffer, indenting as necessary.
func (sg *sourceGenerator) append(value string) {
	for _, currentRune := range value {
		if currentRune == '\n' {
			if sg.hasNewline {
				sg.hasBlankline = true
			}

			sg.buf.WriteRune('\n')
			sg.hasNewline = true
			sg.existingLineLength = 0
			continue
		}

		sg.hasBlankline = false
		sg.hasNewScope = false

		if sg.hasNewline {
			sg.buf.WriteString(strings.Repeat("\t", sg.indentationLevel))
			sg.hasNewline = false
			sg.existingLineLength += sg.indentationLevel
		}

		sg.existingLineLength++
		sg.buf.WriteRune(currentRune)
	}
}

// appendLine adds a newline.
func (sg *sourceGenerator) appendLine() {
	sg.append("\n")
}

func (sg *sourceGenerator) markNewScope() {
	sg.hasNewScope = true
}
