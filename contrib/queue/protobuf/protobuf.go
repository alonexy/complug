package protobuf

import (
	"context"
	"errors"
	"reflect"

	"github.com/alonexy/complug/components/queue"
	"google.golang.org/protobuf/proto"
)

// Codec encodes/decodes protobuf messages.
type Codec[T proto.Message] struct{}

func (Codec[T]) Encode(_ context.Context, value T) ([]byte, error) {
	return proto.Marshal(value)
}

func (Codec[T]) Decode(_ context.Context, data []byte) (T, error) {
	var zero T
	msg, err := newMessage[T]()
	if err != nil {
		return zero, err
	}
	if err := proto.Unmarshal(data, msg); err != nil {
		return zero, err
	}
	out, ok := msg.(T)
	if !ok {
		return zero, errors.New("protobuf: decoded message type mismatch")
	}
	return out, nil
}

func newMessage[T proto.Message]() (proto.Message, error) {
	var zero T
	typ := reflect.TypeOf(zero)
	if typ == nil {
		return nil, errors.New("protobuf: message type must be a concrete pointer type")
	}
	if typ.Kind() != reflect.Ptr {
		return nil, errors.New("protobuf: message type must be a pointer type")
	}
	msg, ok := reflect.New(typ.Elem()).Interface().(proto.Message)
	if !ok {
		return nil, errors.New("protobuf: message type does not implement proto.Message")
	}
	return msg, nil
}

// Ensure Codec implements queue.Codec for protobuf messages.
var _ queue.Codec[proto.Message] = Codec[proto.Message]{}
