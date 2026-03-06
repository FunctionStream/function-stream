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

// ComplexKey is a composite key for store operations.
type ComplexKey struct {
	KeyGroup  []byte
	Key       []byte
	Namespace []byte
	UserKey   []byte
}

// Iterator iterates over key-value pairs from Store.ScanComplex.
type Iterator interface {
	HasNext() (bool, error)
	Next() (key []byte, value []byte, ok bool, err error)
	Close() error
}

// Store provides state and key-value operations.
type Store interface {
	PutState(key []byte, value []byte) error
	GetState(key []byte) (value []byte, found bool, err error)
	DeleteState(key []byte) error
	ListStates(startInclusive []byte, endExclusive []byte) ([][]byte, error)
	Put(key ComplexKey, value []byte) error
	Get(key ComplexKey) (value []byte, found bool, err error)
	Delete(key ComplexKey) error
	Merge(key ComplexKey, value []byte) error
	DeletePrefix(key ComplexKey) error
	ListComplex(
		keyGroup []byte,
		key []byte,
		namespace []byte,
		startInclusive []byte,
		endExclusive []byte,
	) ([][]byte, error)
	ScanComplex(keyGroup []byte, key []byte, namespace []byte) (Iterator, error)
	Close() error
}
