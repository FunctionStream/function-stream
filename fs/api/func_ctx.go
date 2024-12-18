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

package api

import (
	"context"

	"github.com/functionstream/function-stream/fs/contube"
)

type FunctionContext interface {
	PutState(ctx context.Context, key string, value []byte) error
	GetState(ctx context.Context, key string) ([]byte, error)
	ListStates(ctx context.Context, startInclusive string, endExclusive string) ([]string, error)
	DeleteState(ctx context.Context, key string) error
	Write(record contube.Record) error
	GetConfig() map[string]string
}
