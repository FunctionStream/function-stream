package client

import (
	"fmt"
	"github.com/functionstream/function-stream/cmd/client/common"
	"github.com/spf13/cobra"
)

func NewDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete resources",
		RunE: func(cmd *cobra.Command, args []string) error {
			res := args[0]
			name := args[1]
			apiCLi := common.GetApiClient()

			err := apiCLi.GenericService(res).Delete(cmd.Context(), name)
			if err != nil {
				return err
			}
			fmt.Printf("Resource %s/%s deleted\n", res, name)
			return nil
		},
	}
	cmd.Args = cobra.ExactArgs(2)
	return cmd
}
