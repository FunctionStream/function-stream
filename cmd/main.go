package main

import (
	"fmt"
	"github.com/functionstream/functionstream/cmd/server"
	"github.com/spf13/cobra"
	"os"
)

var (
	rootCmd = &cobra.Command{
		Use:   "function-stream",
		Short: "function-stream root command",
		Long:  `function-stream root command`,
	}
)

func init() {
	rootCmd.AddCommand(server.Cmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
