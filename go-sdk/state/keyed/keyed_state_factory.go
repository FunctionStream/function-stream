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
