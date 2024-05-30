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
