package errors

// Severity represents the severity of a thrown error. The possible error
// severities are ERROR, FATAL, or PANIC (in an error message), or WARNING,
// NOTICE, DEBUG, INFO, or LOG (in a notice message)
type Severity string

// Represents the severity of a thrown error. The possible error severities are
// ERROR, FATAL, or PANIC (in an error message), or WARNING, NOTICE, DEBUG,
// INFO, or LOG (in a notice message)
const (
	LevelError   Severity = "ERROR"
	LevelFatal   Severity = "FATAL"
	LevelPanic   Severity = "PANIC"
	LevelWarning Severity = "WARNING"
	LevelNotice  Severity = "NOTICE"
	LevelDebug   Severity = "DEBUG"
	LevelInfo    Severity = "INFO"
	LevelLog     Severity = "LOG"
)
