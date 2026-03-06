// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package fssdk is the Function Stream Go SDK for WASM processors.
// Implement Driver (or embed BaseDriver) and call Run(driver).
package fssdk

import (
	"github.com/functionstream/function-stream/go-sdk/api"
	"github.com/functionstream/function-stream/go-sdk/impl"
)

// Re-export API types and errors so existing code keeps using fssdk.*.
type (
	Context    = api.Context
	Store      = api.Store
	Iterator   = api.Iterator
	Driver     = api.Driver
	BaseDriver = api.BaseDriver
	Module     = api.Module
	ComplexKey = api.ComplexKey
	ErrorCode  = api.ErrorCode
	SDKError   = api.SDKError
)

// Re-export error codes.
const (
	ErrRuntimeInvalidDriver  = api.ErrRuntimeInvalidDriver
	ErrRuntimeNotInitialized = api.ErrRuntimeNotInitialized
	ErrRuntimeClosed         = api.ErrRuntimeClosed
	ErrStoreInvalidName      = api.ErrStoreInvalidName
	ErrStoreInternal         = api.ErrStoreInternal
	ErrStoreNotFound         = api.ErrStoreNotFound
	ErrStoreIO               = api.ErrStoreIO
	ErrResultUnexpected      = api.ErrResultUnexpected
)

// Run wires the driver to the WASM processor. Call from main.
func Run(driver Driver) {
	impl.Run(driver)
}
