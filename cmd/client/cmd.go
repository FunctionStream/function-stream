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

package client

import (
	c "github.com/functionstream/function-stream/cmd/client/common"
	"github.com/functionstream/function-stream/cmd/client/consume"
	"github.com/functionstream/function-stream/cmd/client/create"
	del "github.com/functionstream/function-stream/cmd/client/delete"
	"github.com/functionstream/function-stream/cmd/client/list"
	"github.com/functionstream/function-stream/cmd/client/produce"
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
