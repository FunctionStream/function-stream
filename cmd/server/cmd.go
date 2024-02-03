package server

import (
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/server"
	"github.com/spf13/cobra"
	"io"
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
	common.RunProcess(func() (io.Closer, error) {
		s := server.New()
		go s.Run()
		return s, nil
	})
}
