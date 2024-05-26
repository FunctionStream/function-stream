package common

import "github.com/go-logr/logr"

const (
	DebugLevel int = 4
)

type Logger struct {
	*logr.Logger
}

func NewLogger(logger *logr.Logger) *Logger {
	return &Logger{logger}
}

func (l *Logger) DebugEnabled() bool {
	return l.GetV() >= DebugLevel
}

func (l *Logger) Debug(msg string, keysAndValues ...interface{}) {
	if l.DebugEnabled() {
		l.V(DebugLevel).Info(msg, keysAndValues...)
	}
}

func (l *Logger) SubLogger(keysAndValues ...any) *Logger {
	internalLogger := l.WithValues(keysAndValues...)
	return &Logger{&internalLogger}
}
