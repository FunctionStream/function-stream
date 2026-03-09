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
	"fmt"
	"iter"

	"github.com/functionstream/function-stream/go-sdk/api"
	"github.com/functionstream/function-stream/go-sdk/state/codec"
	"github.com/functionstream/function-stream/go-sdk/state/common"
)

type KeyedMapStateFactory[MK any, MV any] struct {
	inner         *keyedStateFactory
	groupKey      []byte
	mapKeyCodec   codec.Codec[MK]
	mapValueCodec codec.Codec[MV]
}

func NewKeyedMapStateFactory[MK any, MV any](
	store common.Store,
	keyGroup []byte,
	mapKeyCodec codec.Codec[MK],
	mapValueCodec codec.Codec[MV],
) (*KeyedMapStateFactory[MK, MV], error) {

	inner, err := newKeyedStateFactory(store, "", "map")
	if err != nil {
		return nil, err
	}

	if keyGroup == nil {
		return nil, api.NewError(api.ErrStoreInternal, "keyed map state factory key_group must not be nil")
	}
	if mapKeyCodec == nil || mapValueCodec == nil {
		return nil, api.NewError(api.ErrStoreInternal, "keyed map state factory map_key_codec and map_value_codec must not be nil")
	}

	if !mapKeyCodec.IsOrderedKeyCodec() {
		return nil, api.NewError(api.ErrStoreInternal, "map key codec must be ordered")
	}

	return &KeyedMapStateFactory[MK, MV]{
		inner:         inner,
		groupKey:      common.DupBytes(keyGroup),
		mapKeyCodec:   mapKeyCodec,
		mapValueCodec: mapValueCodec,
	}, nil
}

type KeyedMapState[MK any, MV any] struct {
	factory    *KeyedMapStateFactory[MK, MV]
	primaryKey []byte
	namespace  []byte
}

func (f *KeyedMapStateFactory[MK, MV]) NewKeyedMap(primaryKey []byte, mapName string) (*KeyedMapState[MK, MV], error) {
	if primaryKey == nil || mapName == "" {
		return nil, api.NewError(api.ErrStoreInternal, "primary key and map name are required")
	}
	return &KeyedMapState[MK, MV]{
		factory:    f,
		primaryKey: common.DupBytes(primaryKey),
		namespace:  []byte(mapName),
	}, nil
}

func (s *KeyedMapState[MK, MV]) buildCK(mapKey MK) (api.ComplexKey, error) {
	encodedMapKey, err := s.factory.mapKeyCodec.Encode(mapKey)
	if err != nil {
		return api.ComplexKey{}, fmt.Errorf("encode map userKey failed: %w", err)
	}
	return api.ComplexKey{
		KeyGroup:  s.factory.groupKey,
		Key:       s.primaryKey,
		Namespace: s.namespace,
		UserKey:   encodedMapKey,
	}, nil
}

func (s *KeyedMapState[MK, MV]) Put(mapKey MK, value MV) error {
	ck, err := s.buildCK(mapKey)
	if err != nil {
		return err
	}
	encodedValue, err := s.factory.mapValueCodec.Encode(value)
	if err != nil {
		return err
	}
	return s.factory.inner.store.Put(ck, encodedValue)
}

func (s *KeyedMapState[MK, MV]) Get(mapKey MK) (MV, bool, error) {
	var zero MV
	ck, err := s.buildCK(mapKey)
	if err != nil {
		return zero, false, err
	}
	raw, found, err := s.factory.inner.store.Get(ck)
	if err != nil || !found {
		return zero, found, err
	}
	decoded, err := s.factory.mapValueCodec.Decode(raw)
	if err != nil {
		return zero, false, err
	}
	return decoded, true, nil
}

func (s *KeyedMapState[MK, MV]) Delete(mapKey MK) error {
	ck, err := s.buildCK(mapKey)
	if err != nil {
		return err
	}
	return s.factory.inner.store.Delete(ck)
}

func (s *KeyedMapState[MK, MV]) Clear() error {
	return s.factory.inner.store.DeletePrefix(api.ComplexKey{
		KeyGroup:  s.factory.groupKey,
		Key:       s.primaryKey,
		Namespace: s.namespace,
		UserKey:   []byte{},
	})
}

func (s *KeyedMapState[MK, MV]) All() iter.Seq2[MK, MV] {
	return func(yield func(MK, MV) bool) {
		iter, err := s.factory.inner.store.ScanComplex(
			s.factory.groupKey,
			s.primaryKey,
			s.namespace,
		)
		if err != nil {
			return
		}
		defer iter.Close()

		for {
			has, err := iter.HasNext()
			if err != nil || !has {
				return
			}
			keyRaw, valRaw, ok, err := iter.Next()
			if err != nil || !ok {
				return
			}

			k, _ := s.factory.mapKeyCodec.Decode(keyRaw)
			v, _ := s.factory.mapValueCodec.Decode(valRaw)

			if !yield(k, v) {
				return
			}
		}
	}
}
