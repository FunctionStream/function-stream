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

package _package

import (
	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/common/model"
	"github.com/functionstream/function-stream/fs/api"
)

type WasmPackage struct {
	api.Package
	path string
}

type emptyPackage struct{}

func (p *emptyPackage) GetSupportedRuntimeConfig() []model.RuntimeConfig {
	return nil
}

var EmptyPackage = &emptyPackage{}

func (p *WasmPackage) GetSupportedRuntimeConfig() []model.RuntimeConfig {
	return []model.RuntimeConfig{
		{
			Type: common.WASMRuntime,
			Config: map[string]interface{}{
				"archive": p.path,
			},
		},
	}
}

type DefaultPackageLoader struct {
}

func (p DefaultPackageLoader) Load(path string) (api.Package, error) {
	if path == "" {
		return EmptyPackage, nil
	}
	return &WasmPackage{path: path}, nil
}

func NewDefaultPackageLoader() api.PackageLoader {
	return &DefaultPackageLoader{}
}
