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

package server

import (
	"context"
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/server"
	"github.com/spf13/cobra"
	"io"
)

var (
	Cmd = &cobra.Command{
		Use:   "server",
		Short: "Start a server",
		Long:  `Start a server`,
		Run:   exec,
	}
)

func exec(*cobra.Command, []string) {
	common.RunProcess(func() (io.Closer, error) {
		s := server.New(server.LoadConfigFromEnv())
		go s.Run(context.Background())
		return s, nil
	})
}
