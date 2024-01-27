package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
)

type Person struct {
	Name  string `json:"name"`
	Money int    `json:"money"`
}

func main() {
	_, _ = fmt.Fprintln(os.Stderr, "Hello from Go!")
	reader := bufio.NewReader(os.Stdin)
	lengthBytes := make([]byte, 8)
	_, err := reader.Read(lengthBytes)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Failed to read length:", err)
		os.Exit(1)
	}

	length := binary.BigEndian.Uint64(lengthBytes)
	dataBytes := make([]byte, length)
	_, err = reader.Read(dataBytes)
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

	lengthBytes = make([]byte, 8)
	binary.BigEndian.PutUint64(lengthBytes, uint64(len(jsonBytes)))
	_, err = os.Stdout.Write(lengthBytes)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Failed to write length to stdout:", err)
		os.Exit(1)
	}

	_, err = fmt.Printf("%s", jsonBytes)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Failed to print JSON:", err)
		os.Exit(1)
	}
}
