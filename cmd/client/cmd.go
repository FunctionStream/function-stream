package client

import (
	c "github.com/functionstream/functionstream/cmd/client/common"
	"github.com/functionstream/functionstream/cmd/client/consume"
	"github.com/functionstream/functionstream/cmd/client/create"
	del "github.com/functionstream/functionstream/cmd/client/delete"
	"github.com/functionstream/functionstream/cmd/client/list"
	"github.com/functionstream/functionstream/cmd/client/produce"
	"github.com/spf13/cobra"
)

var (
	Cmd = &cobra.Command{
		Use:   "client",
		Short: "Function Stream Client Tool",
		Long:  `Operations to manage functions in a function stream server`,
	}
)

func init() {
	Cmd.PersistentFlags().StringVarP(&c.Config.ServiceAddr, "service-address", "s", "http://localhost:7300", "Service address")

	Cmd.AddCommand(create.Cmd)
	Cmd.AddCommand(list.Cmd)
	Cmd.AddCommand(del.Cmd)
	Cmd.AddCommand(produce.Cmd)
	Cmd.AddCommand(consume.Cmd)
}
