/*
 * Copyright 2024 Function Stream Org.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package common

import (
	"context"
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
)

const (
	DebugLevel int = -1
	InfoLevel  int = 0 // Default log level
	WarnLevel  int = 1
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

func (l *Logger) Warn(msg string, keysAndValues ...interface{}) {
	l.V(WarnLevel).Info(msg, keysAndValues...)
}

func (l *Logger) Info(msg string, keysAndValues ...interface{}) {
	l.V(InfoLevel).Info(msg, keysAndValues...)
}

func (l *Logger) SubLogger(keysAndValues ...any) *Logger {
	internalLogger := l.WithValues(keysAndValues...)
	return &Logger{&internalLogger}
}

type loggerKey struct{}

func WithLogger(ctx context.Context, logger *Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

func GetLogger(ctx context.Context) *Logger {
	logger, ok := ctx.Value(loggerKey{}).(*Logger)
	if !ok {
		return NewDefaultLogger()
	}
	return logger
}
