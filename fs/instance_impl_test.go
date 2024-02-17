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

package fs

import (
	"github.com/functionstream/function-stream/common/model"
	"testing"
)

func TestNewFunctionInstance(t *testing.T) {
	defaultInstanceFactory := DefaultInstanceFactory{}
	definition := &model.Function{
		Name: "test-function",
	}
	index := int32(1)
	instance := defaultInstanceFactory.NewFunctionInstance(definition, nil, nil, index)

	if instance == nil {
		t.Error("FunctionInstance should not be nil")
	}

	Instance := instance.(*FunctionInstanceImpl)

	if ctxValue, ok := Instance.ctx.Value(ctxKey("function-name")).(string); !ok || ctxValue != definition.Name {
		t.Errorf("Expected 'function-name' in ctx to be '%s'", definition.Name)
	}

	if ctxValue, ok := Instance.ctx.Value(ctxKey("function-index")).(int32); !ok || ctxValue != index {
		t.Errorf("Expected 'function-index' in ctx to be '%d'", index)
	}

}
