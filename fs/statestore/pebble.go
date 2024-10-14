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

package statestore

import (
	"context"
	"fmt"
	"github.com/functionstream/function-stream/common/config"
	"github.com/functionstream/function-stream/common/model"
	"os"

	"github.com/cockroachdb/pebble"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/pkg/errors"
)

type PebbleStateStoreFactory struct {
	db *pebble.DB
}

type PebbleStateStoreFactoryConfig struct {
	DirName string `json:"dir_name" validate:"required"`
}

type PebbleStateStoreConfig struct {
	KeyPrefix string `json:"key_prefix,omitempty"`
}

func NewPebbleStateStoreFactory(config config.ConfigMap) (api.StateStoreFactory, error) {
	c := &PebbleStateStoreFactoryConfig{}
	err := config.ToConfigStruct(c)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}
	db, err := pebble.Open(c.DirName, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &PebbleStateStoreFactory{db: db}, nil
}

func NewDefaultPebbleStateStoreFactory() (api.StateStoreFactory, error) {
	dir, err := os.MkdirTemp("", "")
	if err != nil {
		return nil, err
	}
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &PebbleStateStoreFactory{db: db}, nil
}

func (fact *PebbleStateStoreFactory) NewStateStore(f *model.Function) (api.StateStore, error) {
	if f == nil {
		return &PebbleStateStore{
			db:        fact.db,
			keyPrefix: "",
		}, nil
	}
	c := &PebbleStateStoreConfig{}
	err := f.State.ToConfigStruct(c)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}
	return &PebbleStateStore{
		db:        fact.db,
		keyPrefix: c.KeyPrefix,
	}, nil
}

func (fact *PebbleStateStoreFactory) Close() error {
	return fact.db.Close()
}

type PebbleStateStore struct {
	db        *pebble.DB
	keyPrefix string
}

func (s *PebbleStateStore) getKey(key string) string {
	return s.keyPrefix + key
}

func (s *PebbleStateStore) PutState(ctx context.Context, key string, value []byte) error {
	if err := s.db.Set([]byte(s.getKey(key)), value, pebble.NoSync); err != nil {
		return err
	}
	return nil
}

func (s *PebbleStateStore) GetState(ctx context.Context, key string) ([]byte, error) {
	value, closer, err := s.db.Get([]byte(s.getKey(key)))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, api.ErrNotFound
		}
		return nil, err
	}
	result := make([]byte, len(value))
	copy(result, value)
	if err := closer.Close(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *PebbleStateStore) ListStates(
	ctx context.Context, startInclusive string, endExclusive string) ([]string, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(s.getKey(startInclusive)),
		UpperBound: []byte(s.getKey(endExclusive)),
	})
	if err != nil {
		return nil, err
	}
	defer func(iter *pebble.Iterator) {
		_ = iter.Close()
	}(iter)
	var keys []string
	for iter.First(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	return keys, nil
}

func (s *PebbleStateStore) DeleteState(ctx context.Context, key string) error {
	if err := s.db.Delete([]byte(s.getKey(key)), pebble.NoSync); err != nil {
		return err
	}
	return nil
}

func (s *PebbleStateStore) Close() error {
	return nil
}
