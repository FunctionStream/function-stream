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

package keyed

import (
	"encoding/base64"
	"fmt"

	"github.com/functionstream/function-stream/go-sdk/api"
	"github.com/functionstream/function-stream/go-sdk/state/codec"
	"github.com/functionstream/function-stream/go-sdk/state/common"
)

type keyedStateFactory struct {
	store common.Store
	name  string
}

func newKeyedStateFactory(store common.Store, name string, kind string) (*keyedStateFactory, error) {
	stateName, err := common.ValidateStateName(name)
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, api.NewError(api.ErrStoreInternal, "keyed %s state factory %q store must not be nil", kind, stateName)
	}
	return &keyedStateFactory{store: store, name: stateName}, nil
}

func keyedSubStateName[K any](base string, keyCodec codec.Codec[K], key K, kind string) (string, error) {
	if keyCodec == nil {
		return "", api.NewError(api.ErrStoreInternal, "key codec must not be nil")
	}
	encoded, err := keyCodec.Encode(key)
	if err != nil {
		return "", fmt.Errorf("encode keyed state key failed: %w", err)
	}
	return fmt.Sprintf("%s/%s/%s", base, kind, base64.RawURLEncoding.EncodeToString(encoded)), nil
}
