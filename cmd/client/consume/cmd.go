package consume

import (
	"context"
	"fmt"
	"github.com/functionstream/functionstream/cmd/client/common"
	"github.com/functionstream/functionstream/restclient"
	"github.com/spf13/cobra"
	"os"
)

var (
	config = flags{}
)

type flags struct {
	name string
}

var Cmd = &cobra.Command{
	Use:   "consume",
	Short: "Consume an event",
	Long:  `Consume an event from a queue`,
	Args:  cobra.NoArgs,
	Run:   exec,
}

func init() {
	Cmd.Flags().StringVarP(&config.name, "name", "n", "", "The name of the queue")
	Cmd.MarkFlagsRequiredTogether("name")
}

func exec(_ *cobra.Command, _ []string) {
	cfg := restclient.NewConfiguration()
	cfg.Servers = []restclient.ServerConfiguration{{
		URL: common.Config.ServiceAddr,
	}}
	cli := restclient.NewAPIClient(cfg)

	e, res, err := cli.DefaultAPI.ApiV1ConsumeQueueNameGet(context.Background(), config.name).Execute()
	if err != nil {
		fmt.Printf("Failed to consume event: %v\n", err)
		os.Exit(1)
	}
	if res.StatusCode != 200 {
		fmt.Printf("Failed to consume event: %v\n", res.Status)
		os.Exit(1)
	}
	fmt.Printf("%s\n", e)
}
