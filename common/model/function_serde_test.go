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

package model

import (
	"encoding/json"
	"fmt"
	"github.com/functionstream/function-stream/common"
	"gopkg.in/yaml.v3"
	"reflect"
	"testing"
)

func TestFunctionSerde(t *testing.T) {
	f := Function{
		Name:     "TestFunction",
		Runtime:  &RuntimeConfig{Type: common.OptionalStr("runtime"), Config: map[string]interface{}{"key": "value"}},
		Sources:  []*TubeConfig{{Type: common.OptionalStr("source"), Config: map[string]interface{}{"key": "value"}}},
		Sink:     &TubeConfig{Type: common.OptionalStr("sink"), Config: map[string]interface{}{"key": "value"}},
		Config:   map[string]string{"key": "value"},
		Replicas: 2,
	}

	// JSON Serialization
	data, err := json.Marshal(f)
	if err != nil {
		t.Fatal("JSON Serialization error:", err)
	}

	fmt.Println(string(data))

	// JSON Deserialization
	var f2 Function
	err = json.Unmarshal(data, &f2)
	if err != nil {
		t.Fatal("JSON Deserialization error:", err)
	}

	if !reflect.DeepEqual(f, f2) {
		t.Error("JSON Deserialization does not match original")
	}

	// YAML Serialization
	data, err = yaml.Marshal(f)
	if err != nil {
		t.Fatal("YAML Serialization error:", err)
	}

	fmt.Println(string(data))

	// YAML Deserialization
	err = yaml.Unmarshal(data, &f2)
	if err != nil {
		t.Fatal("YAML Deserialization error:", err)
	}

	if !reflect.DeepEqual(f, f2) {
		t.Error("YAML Deserialization does not match original")
	}
}

func TestFunctionSerdeWithNil(t *testing.T) {
	f := Function{
		Name:     "TestFunction",
		Runtime:  nil,
		Sources:  nil,
		Sink:     nil,
		Config:   map[string]string{"key": "value"},
		Replicas: 2,
	}

	// JSON Serialization
	data, err := json.Marshal(f)
	if err != nil {
		t.Fatal("JSON Serialization error:", err)
	}

	fmt.Println(string(data))

	// JSON Deserialization
	var f2 Function
	err = json.Unmarshal(data, &f2)
	if err != nil {
		t.Fatal("JSON Deserialization error:", err)
	}

	if !reflect.DeepEqual(f, f2) {
		t.Error("JSON Deserialization does not match original")
	}

	// YAML Serialization
	data, err = yaml.Marshal(f)
	if err != nil {
		t.Fatal("YAML Serialization error:", err)
	}

	fmt.Println(string(data))

	f.Sources = []*TubeConfig{} // The nil would be expected to be converted to a zero-length array for the YAML serialization

	// YAML Deserialization
	err = yaml.Unmarshal(data, &f2)
	if err != nil {
		t.Fatal("YAML Deserialization error:", err)
	}

	if !reflect.DeepEqual(f, f2) {
		t.Error("YAML Deserialization does not match original")
	}
}
