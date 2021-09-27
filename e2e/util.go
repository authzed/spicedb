package e2e

import (
	"io"
	"testing"
)

// TLogger wraps a testing.TB and makes it conform to io.Writer
type TLogger struct {
	testing.TB
}

// Write satisfied io.Writer
func (t *TLogger) Write(p []byte) (int, error) {
	t.Log(string(p))
	return len(p), nil
}

// TLog returns a TLogger
func TLog(t testing.TB) io.Writer {
	return &TLogger{TB: t}
}
