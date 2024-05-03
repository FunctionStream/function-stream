/*
 * Copyright 2024 Function Stream Org.
 *
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

package del

import (
	adminclient "github.com/functionstream/function-stream/admin/client"
	"github.com/functionstream/function-stream/cmd/client/common"
	"github.com/spf13/cobra"
)

var (
	config = flags{}
)

type flags struct {
	name string
}

var Cmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete Function",
	Long:  `Delete a function on the function stream server`,
	Args:  cobra.NoArgs,
	Run:   exec,
}

func init() {
	Cmd.Flags().StringVarP(&config.name, "name", "n", "", "The name of the function")
	Cmd.MarkFlagsRequiredTogether("name")
}

func exec(_ *cobra.Command, _ []string) {
	cfg := adminclient.NewConfiguration()
	cfg.Servers = []adminclient.ServerConfiguration{{
		URL: common.Config.ServiceAddr,
	}}
	_ = adminclient.NewAPIClient(cfg)

	//res, err := cli.DefaultAPI.ApiV1FunctionFunctionNameDelete(context.Background(), config.name).Execute()
	//if err != nil {
	//	fmt.Printf("Failed to delete function: %v\n", err)
	//	os.Exit(1)
	//}
	//if res.StatusCode != 200 {
	//	fmt.Printf("Failed to delete function with status code: %d\n", res.StatusCode)
	//	os.Exit(1)
	//}
}
