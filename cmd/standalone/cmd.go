package standalone

import (
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/server"
	"github.com/spf13/cobra"
	"io"
)

var (
	Cmd = &cobra.Command{
		Use:   "standalone",
		Short: "Start a standalone server",
		Long:  `Start a standalone server`,
		Run:   exec,
	}
)

func exec(*cobra.Command, []string) {
	common.RunProcess(func() (io.Closer, error) {
		s := server.NewStandalone()
		go s.Run()
		return s, nil
	})
}
