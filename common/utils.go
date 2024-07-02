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

import "log/slog"

func OptionalStr(s string) *string {
	return &s
}

type expensive struct {
	slog.LogValuer
	f func() slog.Value
}

func (e *expensive) LogValue() slog.Value {
	return e.f()
}

func Expensive(f func() slog.Value) slog.LogValuer {
	e := &expensive{}
	e.f = f
	return e
}

type logCounter struct {
	slog.LogValuer
	count int
}

func (l *logCounter) LogValue() slog.Value {
	l.count++
	return slog.IntValue(l.count)
}

func LogCounter() slog.LogValuer {
	return &logCounter{}
}

type NamespacedName struct {
	namespace string
	name      string
}

func (n NamespacedName) String() string {
	if n.namespace == "" {
		return n.name
	}
	return n.namespace + "/" + n.name
}

func GetNamespacedName(namespace, name string) NamespacedName {
	return NamespacedName{
		namespace: namespace,
		name:      name,
	}
}
