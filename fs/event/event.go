package event

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/functionstream/function-stream/fs/api"
	"io"
)

type RawEvent struct {
	id       string
	payload  []byte
	commitCh chan struct{}
}

func NewRawEvent(id string, payload []byte) *RawEvent {
	return &RawEvent{
		id:       id,
		payload:  payload,
		commitCh: make(chan struct{}, 1), // Don't block the sender
	}
}

func (e *RawEvent) ID() string {
	return ""
}

func (e *RawEvent) SchemaID() int64 {
	return -1
}

func (e *RawEvent) Payload() io.Reader {
	return bytes.NewReader(e.payload)
}

func (e *RawEvent) Properties() map[string]string {
	return nil
}

func (e *RawEvent) Commit(ctx context.Context) error {
	e.commitCh <- struct{}{}
	return nil
}

func (e *RawEvent) OnCommit() <-chan struct{} {
	return e.commitCh
}

func NewStructEvent(id string, payload any) (api.Event, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return NewRawEvent(id, data), nil
}
