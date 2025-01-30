package client

import (
	"encoding/json"
	"fmt"
	"github.com/functionstream/function-stream/cmd/client/common"
	"github.com/spf13/cobra"
)

func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list resources",
		RunE: func(cmd *cobra.Command, args []string) error {
			res := args[0]
			apiCLi := common.GetApiClient()

			resources, err := apiCLi.GenericService(res).List(cmd.Context())
			if err != nil {
				return err
			}

			data, err := json.MarshalIndent(resources, "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal data: %v", err)
			}

			fmt.Println(string(data))
			return nil
		},
	}
	cmd.Args = cobra.ExactArgs(1)
	return cmd
}
