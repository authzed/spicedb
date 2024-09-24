package testutil

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"github.com/stretchr/testify/require"
)

// AreProtoEqual returns whether the expected and required protocol buffer messages are equal, via proto.Equal.
// If the messages are not equal, returns an error.
func AreProtoEqual[T proto.Message](expected, found T, message string, args ...any) error {
	areEqual := proto.Equal(expected, found)
	if areEqual {
		return nil
	}

	formattedMessage := fmt.Sprintf(message, args...)

	return fmt.Errorf("%s\n\nExpected:\n%s\nActual:\n%s\nDiff:%s",
		formattedMessage,
		indent(prototext.Format(expected)),
		indent(prototext.Format(found)),
		cmp.Diff(prototext.Format(expected), prototext.Format(found)))
}

func indent(value string) string {
	lines := strings.Split(value, "\n")
	newLines := make([]string, 0, len(lines))
	for _, line := range lines {
		newLines = append(newLines, "\t"+line)
	}
	return strings.Join(newLines, "\n")
}

// RequireProtoEqual ensures that the expected and required protocol buffer messages are equal, via proto.Equal.
func RequireProtoEqual[T proto.Message](t testing.TB, expected, found T, message string, args ...any) {
	areEqual := AreProtoEqual(expected, found, message, args...)
	require.NoError(t, areEqual)
}

func formatMessages[T proto.Message](messages []T) string {
	formatted := make([]string, 0, len(messages))
	for _, message := range messages {
		formatted = append(formatted, prototext.Format(message))
	}
	return strings.Join(formatted, ",")
}

// AreProtoSlicesEqual returns whether the slices of protocol buffers are equal via protocol buffer comparison.
func AreProtoSlicesEqual[T proto.Message](expected, found []T, cmp func(a, b T) int, message string, args ...any) error {
	formattedMessage := fmt.Sprintf(message, args...)

	if len(expected) != len(found) {
		return fmt.Errorf("%s\n\nFound different number of elements in slices: %d in expected, %d in actual\nExpected: %s\nActual: %s",
			formattedMessage,
			len(expected),
			len(found),
			formatMessages(expected),
			formatMessages(found),
		)
	}

	if cmp != nil {
		slices.SortFunc(expected, cmp)
		slices.SortFunc(found, cmp)
	}

	for index := range expected {
		err := AreProtoEqual(expected[index], found[index], "%s\n\nFound mismatch for element at index %d", formattedMessage, index)
		if err != nil {
			return err
		}
	}

	return nil
}

// RequireProtoSlicesEqual ensures that the expected slices of protocol buffers are equal. The
// sort function is used to sort the messages before comparison.
func RequireProtoSlicesEqual[T proto.Message](t testing.TB, expected, found []T, cmp func(a, b T) int, message string, args ...any) {
	err := AreProtoSlicesEqual(expected, found, cmp, message, args...)
	require.NoError(t, err)
}
