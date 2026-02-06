package queue

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestNewConfigDefaultsAndOptions(t *testing.T) {
	cfg := NewConfig()
	if cfg.MaxChanSize != 1 {
		t.Fatalf("expected default MaxChanSize=1, got %d", cfg.MaxChanSize)
	}

	cfg = NewConfig(
		WithMaxChanSize(128),
		WithDialTimeout(2*time.Second),
		WithReadTimeout(3*time.Second),
		WithWriteTimeout(4*time.Second),
	)
	if cfg.MaxChanSize != 128 {
		t.Fatalf("expected MaxChanSize=128, got %d", cfg.MaxChanSize)
	}
	if cfg.DialTimeout != 2*time.Second {
		t.Fatalf("expected DialTimeout=2s, got %s", cfg.DialTimeout)
	}
	if cfg.ReadTimeout != 3*time.Second {
		t.Fatalf("expected ReadTimeout=3s, got %s", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout != 4*time.Second {
		t.Fatalf("expected WriteTimeout=4s, got %s", cfg.WriteTimeout)
	}
}

func TestJSONCodecRoundtrip(t *testing.T) {
	type payload struct {
		ID string `json:"id"`
	}
	codec := JSONCodec[payload]{}
	data, err := codec.Encode(context.Background(), payload{ID: "demo"})
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	got, err := codec.Decode(context.Background(), data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if !reflect.DeepEqual(got, payload{ID: "demo"}) {
		t.Fatalf("unexpected decode result: %#v", got)
	}
}
