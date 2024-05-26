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
	"io"

	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/server"
	"github.com/spf13/cobra"
)

var (
	Cmd = &cobra.Command{
		Use:   "server",
		Short: "Start a server",
		Long:  `Start a server`,
		Run:   exec,
	}
)

type flags struct {
	configFile        string
	loadConfigFromEnv bool
}

var (
	config = flags{}
)

func init() {
	Cmd.Flags().StringVarP(&config.configFile, "config-file", "c", "conf/function-stream.yaml",
		"path to the config file (default is conf/function-stream.yaml)")
	Cmd.Flags().BoolVarP(&config.loadConfigFromEnv, "load-config-from-env", "e", false,
		"load config from env (default is false)")
}

func exec(*cobra.Command, []string) {
	common.RunProcess(func() (io.Closer, error) {
		var c *server.Config
		var err error
		if config.loadConfigFromEnv {
			c, err = server.LoadConfigFromEnv()
			if err != nil {
				return nil, err
			}
		} else {
			c, err = server.LoadConfigFromFile(config.configFile)
			if err != nil {
				return nil, err
			}
		}
		s, err := server.NewServer(
			server.WithTubeFactoryBuilders(server.GetBuiltinTubeFactoryBuilder()),
			server.WithRuntimeFactoryBuilders(server.GetBuiltinRuntimeFactoryBuilder()),
			server.WithConfig(c))
		if err != nil {
			return nil, err
		}
		go s.Run(context.Background())
		return s, nil
	})
}
