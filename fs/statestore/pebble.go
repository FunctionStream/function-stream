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
	"log/slog"
	"os"

	"github.com/cockroachdb/pebble"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/pkg/errors"
)

type PebbleStateStore struct {
	api.StateStore
	log *slog.Logger
	db  *pebble.DB
}

type PebbleStateStoreConfig struct {
	DirName string
}

func NewTmpPebbleStateStore() (api.StateStore, error) {
	dir, err := os.MkdirTemp("", "")
	if err != nil {
		return nil, err
	}
	store, err := NewPebbleStateStore(&PebbleStateStoreConfig{DirName: dir}, slog.Default())
	if err != nil {
		return nil, err
	}
	return store, nil
}

func NewPebbleStateStore(config *PebbleStateStoreConfig, logger *slog.Logger) (*PebbleStateStore, error) {
	log := logger.With(slog.String("component", "pebble-state-store"))
	log.Info("Creating pebble state store", slog.String("dir", config.DirName))
	db, err := pebble.Open(config.DirName, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &PebbleStateStore{
		log: log,
		db:  db,
	}, nil
}

func (s *PebbleStateStore) PutState(ctx context.Context, key string, value []byte) error {
	s.log.Debug("PutState", slog.String("key", key))
	if err := s.db.Set([]byte(key), value, pebble.NoSync); err != nil {
		return err
	}
	return nil
}

func (s *PebbleStateStore) GetState(ctx context.Context, key string) ([]byte, error) {
	s.log.Debug("GetState", slog.String("key", key))
	value, closer, err := s.db.Get([]byte(key))
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
	s.log.Debug("ListStates", slog.String("start", startInclusive), slog.String("end", endExclusive))
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(startInclusive),
		UpperBound: []byte(endExclusive),
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
	s.log.Debug("DeleteState", slog.String("key", key))
	if err := s.db.Delete([]byte(key), pebble.NoSync); err != nil {
		return err
	}
	return nil
}

func (s *PebbleStateStore) Close() error {
	return s.db.Close()
}
