package protobuf

import (
	"context"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"
)

func TestCodecRoundtrip(t *testing.T) {
	codec := Codec[*structpb.Struct]{}
	msg, err := structpb.NewStruct(map[string]any{"id": "demo"})
	if err != nil {
		t.Fatalf("new struct failed: %v", err)
	}
	data, err := codec.Encode(context.Background(), msg)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	out, err := codec.Decode(context.Background(), data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if out.GetFields()["id"].GetStringValue() != "demo" {
		t.Fatalf("unexpected decode result: %v", out)
	}
}
