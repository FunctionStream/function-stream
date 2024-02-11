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
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type Person struct {
	Name     string `json:"name"`
	Money    int    `json:"money"`
	Expected int    `json:"expected"`
}

func main() {
	_, _ = fmt.Fprintln(os.Stderr, "Hello from Go!")
}

//export process
func process() {
	dataBytes, err := io.ReadAll(os.Stdin)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Failed to read data:", err)
		os.Exit(1)
	}

	var person Person
	err = json.Unmarshal(dataBytes, &person)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Failed to parse JSON:", err)
		os.Exit(1)
	}

	person.Money++

	jsonBytes, err := json.Marshal(person)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Failed to encode JSON:", err)
		os.Exit(1)
	}

	_, err = fmt.Printf("%s", jsonBytes)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Failed to print JSON:", err)
		os.Exit(1)
	}
}
