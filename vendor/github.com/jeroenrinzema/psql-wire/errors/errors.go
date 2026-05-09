package errors

import "github.com/jeroenrinzema/psql-wire/codes"

// Error contains all Postgres wire protocol error fields.
// See https://www.postgresql.org/docs/current/static/protocol-error-fields.html
// for a list of all Postgres error fields, most of which are optional and can
// be used to provide auxiliary error information.
type Error struct {
	Code           codes.Code
	Message        string
	Detail         string
	Hint           string
	Severity       Severity
	ConstraintName string
	Source         *Source
}

// Source represents whenever possible the source of a given error.
type Source struct {
	File     string
	Line     int32
	Function string
}

// Flatten returns a flattened error which could be used to construct Postgres
// wire error messages.
func Flatten(err error) Error {
	if err == nil {
		return Error{
			Code:     codes.Internal,
			Message:  "unknown error, an internal process attempted to throw an error",
			Severity: LevelFatal,
		}
	}

	result := Error{
		Code:           GetCode(err),
		Message:        err.Error(),
		Detail:         GetDetail(err),
		Hint:           GetHint(err),
		Severity:       DefaultSeverity(GetSeverity(err)),
		ConstraintName: GetConstraintName(err),
		Source:         GetSource(err),
	}

	return result
}
