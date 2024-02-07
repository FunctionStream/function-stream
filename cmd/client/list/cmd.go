package list

import (
	"context"
	"fmt"
	"github.com/functionstream/functionstream/cmd/client/common"
	"github.com/functionstream/functionstream/restclient"
	"github.com/spf13/cobra"
	"os"
)

var Cmd = &cobra.Command{
	Use:   "list",
	Short: "List All Functions",
	Long:  `List all functions on the function stream server`,
	Args:  cobra.NoArgs,
	Run:   exec,
}

func exec(_ *cobra.Command, _ []string) {
	cfg := restclient.NewConfiguration()
	cfg.Servers = []restclient.ServerConfiguration{{
		URL: common.Config.ServiceAddr,
	}}
	cli := restclient.NewAPIClient(cfg)

	list, res, err := cli.DefaultAPI.ApiV1FunctionsGet(context.Background()).Execute()
	if err != nil {
		fmt.Printf("Failed to list functions: %v\n", err)
		os.Exit(1)
	}
	if res.StatusCode != 200 {
		fmt.Printf("Failed to list functions with status code: %d\n", res.StatusCode)
		os.Exit(1)
	}
	for _, f := range list {
		fmt.Println(f)
	}
}
