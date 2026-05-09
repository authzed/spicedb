package buffer

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
)

// ErrMissingNulTerminator is thrown when no NUL terminator is found when
// interperating a message property as a string.
var ErrMissingNulTerminator = errors.New("NUL terminator not found")

// NewMissingNulTerminator constructs a new error message wrapping the ErrMissingNulTerminator
// type with additional metadata.
func NewMissingNulTerminator() error {
	return psqlerr.WithSeverity(psqlerr.WithCode(ErrMissingNulTerminator, codes.DataCorrupted), psqlerr.LevelFatal)
}

// ErrInsufficientData is thrown when there is insufficient data available inside
// the given message to unmarshal into a given type.
var ErrInsufficientData = errors.New("insufficient data")

// NewInsufficientData constructs a new error message wrapping the ErrInsufficientData
// type with additional metadata.
func NewInsufficientData(length int) error {
	err := fmt.Errorf("length: %d %w", length, ErrInsufficientData)
	return psqlerr.WithSeverity(psqlerr.WithCode(err, codes.DataCorrupted), psqlerr.LevelFatal)
}

// ErrMessageSizeExceeded is thrown when the maximum message size is exceeded.
var ErrMessageSizeExceeded = MessageSizeExceeded{Message: "maximum message size exceeded"}

// MessageSizeExceeded represents a error implementation which could be used to
// indicate that the message size limit has been exceeded. The message size and
// maximum message length could be included inside the struct.
type MessageSizeExceeded struct {
	Message string
	Size    int
	Max     int
}

func (err MessageSizeExceeded) Error() string {
	return err.Message
}

func (err MessageSizeExceeded) Is(target error) bool {
	return reflect.TypeOf(target) == reflect.TypeOf(err)
}

// NewMessageSizeExceeded constructs a new error message wrapping the
// ErrMaxMessageSizeExceeded type with additional metadata.
func NewMessageSizeExceeded(max, size int) error {
	err := MessageSizeExceeded{
		Message: fmt.Sprintf("message size %d, bigger than maximum allowed message size %d", size, max),
		Size:    size,
		Max:     max,
	}

	return psqlerr.WithSeverity(psqlerr.WithCode(err, codes.ProgramLimitExceeded), psqlerr.LevelError)
}

// UnwrapMessageSizeExceeded attempts to unwrap the given error as
// MessageSizeExceeded. A boolean is returned indicating whether the error
// contained a MessageSizeExceeded message.
func UnwrapMessageSizeExceeded(err error) (result MessageSizeExceeded, _ bool) {
	return result, errors.As(err, &result)
}
