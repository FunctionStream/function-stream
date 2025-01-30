package client

import (
	"encoding/json"
	"fmt"
	"github.com/functionstream/function-stream/cmd/client/common"
	"github.com/spf13/cobra"
)

func NewGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get",
		Short: "get resources",
		RunE: func(cmd *cobra.Command, args []string) error {
			res := args[0]
			name := args[1]
			apiCLi := common.GetApiClient()

			t, err := apiCLi.GenericService(res).Read(cmd.Context(), name)
			if err != nil {
				return err
			}

			data, err := json.MarshalIndent(t, "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal data: %v", err)
			}

			fmt.Println(string(data))
			return nil
		},
	}
	cmd.Args = cobra.ExactArgs(2)
	return cmd
}
