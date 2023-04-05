package work

import (
	"testing"
)

type TestLogger struct {
	t *testing.T
}

func NewTestLogger(t *testing.T) *TestLogger {
	return &TestLogger{t: t}
}

func (l *TestLogger) Printf(format string, v ...any) {
	l.t.Helper()
	l.t.Logf(format, v...)
}
