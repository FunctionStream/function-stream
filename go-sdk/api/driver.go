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

package api

type Module struct {
	Name  string
	Bytes []byte
}

type Driver interface {
	Init(ctx Context, config map[string]string) error
	Process(ctx Context, sourceID uint32, data []byte) error
	ProcessWatermark(ctx Context, sourceID uint32, watermark uint64) error
	TakeCheckpoint(ctx Context, checkpointID uint64) error
	CheckHeartbeat(ctx Context) bool
	Close(ctx Context) error
	Exec(ctx Context, className string, modules []Module) error
	Custom(ctx Context, payload []byte) ([]byte, error)
}

type BaseDriver struct{}

func (BaseDriver) Init(Context, map[string]string) error {
	return nil
}

func (BaseDriver) Process(Context, uint32, []byte) error {
	return nil
}

func (BaseDriver) ProcessWatermark(Context, uint32, uint64) error {
	return nil
}

func (BaseDriver) TakeCheckpoint(Context, uint64) error {
	return nil
}

func (BaseDriver) CheckHeartbeat(Context) bool {
	return true
}

func (BaseDriver) Close(Context) error {
	return nil
}

func (BaseDriver) Exec(Context, string, []Module) error {
	return nil
}

func (BaseDriver) Custom(_ Context, payload []byte) ([]byte, error) {
	return cloneBytes(payload), nil
}

func cloneBytes(input []byte) []byte {
	if input == nil {
		return nil
	}
	out := make([]byte, len(input))
	copy(out, input)
	return out
}
