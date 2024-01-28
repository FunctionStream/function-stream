package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type Person struct {
	Name  string `json:"name"`
	Money int    `json:"money"`
}

func main() {
	_, _ = fmt.Fprintln(os.Stderr, "Hello from Go!")

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
