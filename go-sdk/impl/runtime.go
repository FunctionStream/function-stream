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

package impl

import (
	"github.com/functionstream/function-stream/go-sdk/api"
	"github.com/functionstream/function-stream/go-sdk/bindings/functionstream/core/processor"
	"go.bytecodealliance.org/cm"
)

type guestRuntime struct {
	driver api.Driver
	ctx    *runtimeContext
}

// Run wires the driver to the WASM processor exports. Call from the main package (e.g. fssdk.Run(driver)).
func Run(driver api.Driver) {
	if driver == nil {
		panic(api.NewError(api.ErrRuntimeInvalidDriver, "driver must not be nil"))
	}

	rt := &guestRuntime{
		driver: driver,
		ctx:    newRuntimeContext(map[string]string{}),
	}

	processor.Exports.FsInit = rt.fsInit
	processor.Exports.FsProcess = rt.fsProcess
	processor.Exports.FsProcessWatermark = rt.fsProcessWatermark
	processor.Exports.FsTakeCheckpoint = rt.fsTakeCheckpoint
	processor.Exports.FsCheckHeartbeat = rt.fsCheckHeartbeat
	processor.Exports.FsClose = rt.fsClose
	processor.Exports.FsExec = rt.fsExec
	processor.Exports.FsCustom = rt.fsCustom
}

func (r *guestRuntime) fsInit(config cm.List[[2]string]) {
	cfg := liftConfig(config)
	newCtx := newRuntimeContext(cfg)
	oldCtx := r.swapContext(newCtx)
	if oldCtx != nil {
		_ = oldCtx.Close()
	}
	if err := r.driver.Init(newCtx, cfg); err != nil {
		panic(err)
	}
}

func (r *guestRuntime) fsProcess(sourceID uint32, data cm.List[uint8]) {
	ctx := r.context()
	if err := r.driver.Process(ctx, sourceID, cloneBytes(data.Slice())); err != nil {
		panic(err)
	}
}

func (r *guestRuntime) fsProcessWatermark(sourceID uint32, watermark uint64) {
	ctx := r.context()
	if err := r.driver.ProcessWatermark(ctx, sourceID, watermark); err != nil {
		panic(err)
	}
}

func (r *guestRuntime) fsTakeCheckpoint(checkpointID uint64) {
	ctx := r.context()
	if err := r.driver.TakeCheckpoint(ctx, checkpointID); err != nil {
		panic(err)
	}
}

func (r *guestRuntime) fsCheckHeartbeat() bool {
	return r.driver.CheckHeartbeat(r.context())
}

func (r *guestRuntime) fsClose() {
	ctx := r.context()
	driverErr := r.driver.Close(ctx)
	ctxErr := r.closeContext()
	if driverErr != nil {
		panic(driverErr)
	}
	if ctxErr != nil {
		panic(ctxErr)
	}
}

func (r *guestRuntime) fsExec(className string, modules cm.List[cm.Tuple[string, cm.List[uint8]]]) {
	ctx := r.context()
	if err := r.driver.Exec(ctx, className, liftModules(modules)); err != nil {
		panic(err)
	}
}

func (r *guestRuntime) fsCustom(payload cm.List[uint8]) cm.List[uint8] {
	ctx := r.context()
	result, err := r.driver.Custom(ctx, cloneBytes(payload.Slice()))
	if err != nil {
		panic(err)
	}
	return toList(result)
}

func (r *guestRuntime) context() *runtimeContext {
	if r.ctx == nil {
		r.ctx = newRuntimeContext(map[string]string{})
	}
	return r.ctx
}

func (r *guestRuntime) swapContext(next *runtimeContext) *runtimeContext {
	previous := r.ctx
	r.ctx = next
	return previous
}

func (r *guestRuntime) closeContext() error {
	ctx := r.ctx
	r.ctx = nil
	if ctx == nil {
		return nil
	}
	return ctx.Close()
}

func liftConfig(config cm.List[[2]string]) map[string]string {
	items := config.Slice()
	out := make(map[string]string, len(items))
	for idx := range items {
		out[items[idx][0]] = items[idx][1]
	}
	return out
}

func liftModules(modules cm.List[cm.Tuple[string, cm.List[uint8]]]) []api.Module {
	items := modules.Slice()
	out := make([]api.Module, len(items))
	for idx := range items {
		out[idx] = api.Module{
			Name:  items[idx].F0,
			Bytes: cloneBytes(items[idx].F1.Slice()),
		}
	}
	return out
}
