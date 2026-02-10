package protobuf

import (
	"context"

	"github.com/alonexy/complug/components/queue"
	"google.golang.org/protobuf/proto"
)

// Codec encodes/decodes protobuf messages.
type Codec[T proto.Message] struct{}

func (Codec[T]) Encode(_ context.Context, value T) ([]byte, error) {
	return proto.Marshal(value)
}

func (Codec[T]) Decode(_ context.Context, data []byte) (T, error) {
	var v T
	return v, proto.Unmarshal(data, v)
}

// Ensure Codec implements queue.Codec for protobuf messages.
var _ queue.Codec[proto.Message] = Codec[proto.Message]{}
