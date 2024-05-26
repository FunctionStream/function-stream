package common

import (
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
)

const (
	DebugLevel int = 4
)

type Logger struct {
	*logr.Logger
}

func NewDefaultLogger() *Logger {
	zapLogger, err := zap.NewDevelopment()
	if err != nil {
		panic("failed to create zap logger:" + err.Error())
	}
	zaprLog := zapr.NewLogger(zapLogger)
	return NewLogger(&zaprLog)
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
