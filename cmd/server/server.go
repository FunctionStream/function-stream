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
	"fmt"
	"github.com/functionstream/function-stream/pkg/cmdutil"
	"github.com/functionstream/function-stream/pkg/process"
	"github.com/functionstream/function-stream/server"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"io"
	"net/http"
)

type ServerCloser struct {
	cancel context.CancelFunc
}

func (s *ServerCloser) Close() error {
	s.cancel()
	return nil
}

func NewServerCmd() *cobra.Command {
	configFile := ""
	cmd := &cobra.Command{
		Use: "server",
		RunE: func(cmd *cobra.Command, args []string) error {
			if configFile == "" {
				return fmt.Errorf("config file is required")
			}
			process.RunProcess(func() (io.Closer, error) {
				c, err := server.LoadConfigFromFile(configFile)
				cmdutil.CheckErr(err)
				svr, err := server.NewServer(cmd.Context(), c, &server.ServerOption{
					Logger: cmdutil.GetLogger(),
				})
				cmdutil.CheckErr(err)
				svrCtx, cancel := context.WithCancel(cmd.Context())
				err = svr.Run(svrCtx)
				if err != nil && !errors.Is(err, http.ErrServerClosed) {
					cmdutil.CheckErr(err)
				}
				// TODO: Add health check
				return &ServerCloser{
					cancel: cancel,
				}, nil
			})
			return nil
		},
	}
	cmd.Flags().StringVarP(&configFile, "config-file", "c", "conf/function-stream.yaml",
		"path to the config file (default is conf/function-stream.yaml)")
	return cmd
}
