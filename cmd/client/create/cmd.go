/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package create

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
	//configFile string
	//config     string
	archive string
	inputs  []string
	output  string
	replica int32
}

var Cmd = &cobra.Command{
	Use:   "create",
	Short: "Create Function",
	Long:  `Create a function on the function stream server`,
	Args:  cobra.NoArgs,
	Run:   exec,
}

func init() {
	Cmd.Flags().StringVarP(&config.name, "name", "n", "", "The name of the function")
	//Cmd.Flags().StringVarP(&config.configFile, "configFile", "f", "", "The file path of the function config")
	//Cmd.Flags().StringVarP(&config.config, "config", "c", "", "The config of the function")
	Cmd.Flags().StringVarP(&config.archive, "archive", "a", "", "The archive path of the function")
	Cmd.Flags().StringSliceVarP(&config.inputs, "inputs", "i", []string{}, "The inputs of the function")
	Cmd.Flags().StringVarP(&config.output, "output", "o", "", "The output of the function")
	Cmd.Flags().Int32VarP(&config.replica, "replica", "r", 1, "The replica of the function")

	Cmd.MarkFlagsRequiredTogether("name")
}

func exec(_ *cobra.Command, _ []string) {
	cfg := restclient.NewConfiguration()
	cfg.Servers = []restclient.ServerConfiguration{{
		URL: common.Config.ServiceAddr,
	}}
	cli := restclient.NewAPIClient(cfg)
	f := restclient.Function{
		Name:    &config.name,
		Archive: config.archive,
		Inputs:  config.inputs,
		Output:  config.output,
	}

	res, err := cli.DefaultAPI.ApiV1FunctionFunctionNamePost(context.Background(), config.name).Function(f).Execute()
	if err != nil {
		fmt.Printf("Failed to create function: %v\n", err)
		os.Exit(1)
	}
	if res.StatusCode != 200 {
		fmt.Printf("Failed to create function with status code: %d\n", res.StatusCode)
		os.Exit(1)
	}
}
