package stripe

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestRound2CancellationNeedsNoUnrelatedWake(t *testing.T) {
	for range 100 {
		reader, writer := io.Pipe()
		scheduler := New(reader, Config{})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, err := scheduler.Next(ctx, 1, 0); done <- err }()
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("lost cancellation wake")
		}
		reader.Close()
		writer.Close()
		scheduler.Close()
	}
}

func TestRound2DispatchPinsReliability(t *testing.T) {
	reliable := true
	scheduler := New(bytes.NewReader([]byte("payload")), Config{Reliable: func(uint64) bool { return reliable }})
	defer scheduler.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	chunk, err := scheduler.Next(ctx, 1, 0)
	if err != nil || chunk == nil {
		t.Fatal(err)
	}
	reliable = false
	if !chunk.Reliable {
		t.Fatal("dispatch did not carry reliability into writer queue")
	}
	scheduler.Complete(1, chunk)
}
