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

package standalone

import (
	"context"
	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/server"
	"github.com/spf13/cobra"
	"io"
)

var (
	Cmd = &cobra.Command{
		Use:   "standalone",
		Short: "Start a standalone server",
		Long:  `Start a standalone server`,
		Run:   exec,
	}
)

func exec(*cobra.Command, []string) {
	common.RunProcess(func() (io.Closer, error) {
		s, err := server.NewDefaultServer()
		if err != nil {
			return nil, err
		}
		go s.Run(context.Background())
		return s, nil
	})
}
