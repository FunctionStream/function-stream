package common

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/functionstream/function-stream/go-sdk/api"
)

type Store = api.Store

const (
	StateValuePrefix       = "__fssdk__/value/"
	StateListPrefix        = "__fssdk__/list/"
	StatePQPrefix          = "__fssdk__/priority_queue/"
	StateMapGroup          = "__fssdk__/map"
	StateListGroup         = "__fssdk__/list"
	StatePQGroup           = "__fssdk__/priority_queue"
	StateAggregatingPrefix = "__fssdk__/aggregating/"
	StateReducingPrefix    = "__fssdk__/reducing/"
)

func ValidateStateName(name string) (string, error) {
	stateName := strings.TrimSpace(name)
	if stateName == "" {
		return "", api.NewError(api.ErrStoreInvalidName, "state name must not be empty")
	}
	return stateName, nil
}

func EncodeInt64Lex(v int64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, uint64(v)^(uint64(1)<<63))
	return out
}

func DecodeInt64Lex(data []byte) (int64, error) {
	if len(data) != 8 {
		return 0, fmt.Errorf("invalid int64 lex key length: %d", len(data))
	}
	return int64(binary.BigEndian.Uint64(data) ^ (uint64(1) << 63)), nil
}

func EncodePriorityUserKey(priority int64, seq uint64) []byte {
	out := make([]byte, 16)
	copy(out[:8], EncodeInt64Lex(priority))
	binary.BigEndian.PutUint64(out[8:], seq)
	return out
}

func DecodePriorityUserKey(data []byte) (int64, error) {
	if len(data) != 16 {
		return 0, fmt.Errorf("invalid priority key length: %d", len(data))
	}
	return DecodeInt64Lex(data[:8])
}

func DupBytes(input []byte) []byte {
	if input == nil {
		return nil
	}
	out := make([]byte, len(input))
	copy(out, input)
	return out
}
