package server

import (
	"github.com/functionstream/functionstream/lib"
	"github.com/spf13/cobra"
)

var (
	Cmd = &cobra.Command{
		Use:   "server",
		Short: "Start a server",
		Long:  `Start a server`,
		Run:   exec,
	}
)

func exec(*cobra.Command, []string) {
	lib.Run()
}
