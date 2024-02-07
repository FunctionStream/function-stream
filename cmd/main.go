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

package main

import (
	"fmt"
	"github.com/functionstream/functionstream/cmd/client"
	"github.com/functionstream/functionstream/cmd/perf"
	"github.com/functionstream/functionstream/cmd/server"
	"github.com/spf13/cobra"
	"os"
)

var (
	rootCmd = &cobra.Command{
		Use:   "function-stream",
		Short: "function-stream root command",
		Long:  `function-stream root command`,
	}
)

func init() {
	rootCmd.AddCommand(server.Cmd)
	rootCmd.AddCommand(client.Cmd)
	rootCmd.AddCommand(perf.Cmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
