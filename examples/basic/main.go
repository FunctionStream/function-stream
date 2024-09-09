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

package main

import (
	"github.com/functionstream/function-stream/clients/gofs"
	"log/slog"
)

func main() {
	slog.Info("Hello from Go wasm!")
	gofs.Run()
}

type Person struct {
	Name     string `json:"name"`
	Money    int    `json:"money"`
	Expected int    `json:"expected"`
}

func init() {
	err := gofs.Register(gofs.DefaultModule, myProcess)
	if err != nil {
		slog.Error(err.Error())
	}
}

func myProcess(person *Person) *Person {
	person.Money += 1
	return person
}
