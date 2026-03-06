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
	"fmt"
	"sync"

	"github.com/functionstream/function-stream/go-sdk/api"
	"github.com/functionstream/function-stream/go-sdk/bindings/functionstream/core/kv"
	"go.bytecodealliance.org/cm"
)

type storeImpl struct {
	name      string
	raw       kv.Store
	closeOnce sync.Once
}

type iteratorImpl struct {
	name      string
	raw       kv.Iterator
	closeOnce sync.Once
}

func newStore(name string) *storeImpl {
	return &storeImpl{
		name: name,
		raw:  kv.NewStore(name),
	}
}

func (s *storeImpl) PutState(key []byte, value []byte) error {
	result := s.raw.PutState(toList(key), toList(value))
	if kvErr := result.Err(); kvErr != nil {
		return mapKVError(s.name, *kvErr)
	}
	return nil
}

func (s *storeImpl) GetState(key []byte) ([]byte, bool, error) {
	result := s.raw.GetState(toList(key))
	if kvErr := result.Err(); kvErr != nil {
		return nil, false, mapKVError(s.name, *kvErr)
	}
	ok := result.OK()
	if ok == nil {
		return nil, false, api.NewError(api.ErrResultUnexpected, "store %q get-state missing ok payload", s.name)
	}
	val := ok.Some()
	if val == nil {
		return nil, false, nil
	}
	return cloneBytes(val.Slice()), true, nil
}

func (s *storeImpl) DeleteState(key []byte) error {
	result := s.raw.DeleteState(toList(key))
	if kvErr := result.Err(); kvErr != nil {
		return mapKVError(s.name, *kvErr)
	}
	return nil
}

func (s *storeImpl) ListStates(startInclusive []byte, endExclusive []byte) ([][]byte, error) {
	result := s.raw.ListStates(toList(startInclusive), toList(endExclusive))
	if kvErr := result.Err(); kvErr != nil {
		return nil, mapKVError(s.name, *kvErr)
	}
	ok := result.OK()
	if ok == nil {
		return nil, api.NewError(api.ErrResultUnexpected, "store %q list-states missing ok payload", s.name)
	}
	return liftListOfBytes(*ok), nil
}

func (s *storeImpl) Put(key api.ComplexKey, value []byte) error {
	result := s.raw.Put(toKVComplexKey(key), toList(value))
	if kvErr := result.Err(); kvErr != nil {
		return mapKVError(s.name, *kvErr)
	}
	return nil
}

func (s *storeImpl) Get(key api.ComplexKey) ([]byte, bool, error) {
	result := s.raw.Get(toKVComplexKey(key))
	if kvErr := result.Err(); kvErr != nil {
		return nil, false, mapKVError(s.name, *kvErr)
	}
	ok := result.OK()
	if ok == nil {
		return nil, false, api.NewError(api.ErrResultUnexpected, "store %q get missing ok payload", s.name)
	}
	val := ok.Some()
	if val == nil {
		return nil, false, nil
	}
	return cloneBytes(val.Slice()), true, nil
}

func (s *storeImpl) Delete(key api.ComplexKey) error {
	result := s.raw.Delete(toKVComplexKey(key))
	if kvErr := result.Err(); kvErr != nil {
		return mapKVError(s.name, *kvErr)
	}
	return nil
}

func (s *storeImpl) Merge(key api.ComplexKey, value []byte) error {
	result := s.raw.Merge(toKVComplexKey(key), toList(value))
	if kvErr := result.Err(); kvErr != nil {
		return mapKVError(s.name, *kvErr)
	}
	return nil
}

func (s *storeImpl) DeletePrefix(key api.ComplexKey) error {
	result := s.raw.DeletePrefix(toKVComplexKey(key))
	if kvErr := result.Err(); kvErr != nil {
		return mapKVError(s.name, *kvErr)
	}
	return nil
}

func (s *storeImpl) ListComplex(
	keyGroup []byte,
	key []byte,
	namespace []byte,
	startInclusive []byte,
	endExclusive []byte,
) ([][]byte, error) {
	result := s.raw.ListComplex(
		toList(keyGroup),
		toList(key),
		toList(namespace),
		toList(startInclusive),
		toList(endExclusive),
	)
	if kvErr := result.Err(); kvErr != nil {
		return nil, mapKVError(s.name, *kvErr)
	}
	ok := result.OK()
	if ok == nil {
		return nil, api.NewError(api.ErrResultUnexpected, "store %q list-complex missing ok payload", s.name)
	}
	return liftListOfBytes(*ok), nil
}

func (s *storeImpl) ScanComplex(keyGroup []byte, key []byte, namespace []byte) (api.Iterator, error) {
	result := s.raw.ScanComplex(toList(keyGroup), toList(key), toList(namespace))
	if kvErr := result.Err(); kvErr != nil {
		return nil, mapKVError(s.name, *kvErr)
	}
	ok := result.OK()
	if ok == nil {
		return nil, api.NewError(api.ErrResultUnexpected, "store %q scan-complex missing ok payload", s.name)
	}
	return &iteratorImpl{
		name: s.name,
		raw:  *ok,
	}, nil
}

func (s *storeImpl) Close() error {
	s.closeOnce.Do(func() {
		s.raw.ResourceDrop()
	})
	return nil
}

func (i *iteratorImpl) HasNext() (bool, error) {
	result := i.raw.HasNext()
	if kvErr := result.Err(); kvErr != nil {
		return false, mapKVError(i.name, *kvErr)
	}
	ok := result.OK()
	if ok == nil {
		return false, api.NewError(api.ErrResultUnexpected, "iterator for store %q has-next missing ok payload", i.name)
	}
	return *ok, nil
}

func (i *iteratorImpl) Next() ([]byte, []byte, bool, error) {
	result := i.raw.Next()
	if kvErr := result.Err(); kvErr != nil {
		return nil, nil, false, mapKVError(i.name, *kvErr)
	}
	ok := result.OK()
	if ok == nil {
		return nil, nil, false, api.NewError(api.ErrResultUnexpected, "iterator for store %q next missing ok payload", i.name)
	}
	tupleOpt := ok.Some()
	if tupleOpt == nil {
		return nil, nil, false, nil
	}
	tuple := *tupleOpt
	return cloneBytes(tuple[0].Slice()), cloneBytes(tuple[1].Slice()), true, nil
}

func (i *iteratorImpl) Close() error {
	i.closeOnce.Do(func() {
		i.raw.ResourceDrop()
	})
	return nil
}

func mapKVError(storeName string, kvErr kv.Error) error {
	if kvErr.NotFound() {
		return api.NewError(api.ErrStoreNotFound, "store %q key not found", storeName)
	}
	if ioErr := kvErr.IOError(); ioErr != nil {
		return api.NewError(api.ErrStoreIO, "store %q io error: %s", storeName, *ioErr)
	}
	if other := kvErr.Other(); other != nil {
		return api.NewError(api.ErrStoreInternal, "store %q error: %s", storeName, *other)
	}
	return api.NewError(api.ErrStoreInternal, "store %q error: %s", storeName, fmt.Sprintf("%v", kvErr))
}

func toKVComplexKey(input api.ComplexKey) kv.ComplexKey {
	return kv.ComplexKey{
		KeyGroup:  toList(input.KeyGroup),
		Key:       toList(input.Key),
		Namespace: toList(input.Namespace),
		UserKey:   toList(input.UserKey),
	}
}

func toList(input []byte) cm.List[uint8] {
	return cm.ToList(cloneBytes(input))
}

func liftListOfBytes(input cm.List[cm.List[uint8]]) [][]byte {
	raw := input.Slice()
	out := make([][]byte, len(raw))
	for idx := range raw {
		out[idx] = cloneBytes(raw[idx].Slice())
	}
	return out
}
