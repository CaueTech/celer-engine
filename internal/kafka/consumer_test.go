package kafka

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// mockReader implements the Reader interface for unit testing.
type mockReader struct {
	readMessageFunc func(ctx context.Context) (kafka.Message, error)
	closeFunc       func() error
}

func (m *mockReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	if m.readMessageFunc != nil {
		return m.readMessageFunc(ctx)
	}
	return kafka.Message{}, nil
}

func (m *mockReader) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func TestStartConsuming_Success(t *testing.T) {
	messages := [][]byte{
		[]byte("payload-1"),
		[]byte("payload-2"),
		[]byte("payload-3"),
	}

	index := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mock := &mockReader{
		readMessageFunc: func(ctx context.Context) (kafka.Message, error) {
			if index < len(messages) {
				msg := kafka.Message{Value: messages[index]}
				index++
				return msg, nil
			}
			// Once all messages are sent, cancel context to stop the consumer loop
			cancel()
			<-ctx.Done()
			return kafka.Message{}, ctx.Err()
		},
	}

	consumer := &Consumer{reader: mock}
	ingestionChan := make(chan []byte, len(messages))

	errChan := make(chan error, 1)
	go func() {
		errChan <- consumer.StartConsuming(ctx, ingestionChan)
	}()

	// Collect received messages from channel
	for i, expected := range messages {
		select {
		case actual := <-ingestionChan:
			if !bytes.Equal(actual, expected) {
				t.Fatalf("message %d mismatch: got %s, want %s", i, string(actual), string(expected))
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}

	// Wait for consumer to finish after cancellation
	select {
	case err := <-errChan:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled error, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for StartConsuming to exit")
	}
}

func TestStartConsuming_ContextAlreadyCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	mock := &mockReader{
		readMessageFunc: func(ctx context.Context) (kafka.Message, error) {
			t.Fatal("ReadMessage should not have been called when context is already canceled")
			return kafka.Message{}, nil
		},
	}

	consumer := &Consumer{reader: mock}
	ingestionChan := make(chan []byte, 1)

	err := consumer.StartConsuming(ctx, ingestionChan)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}

func TestStartConsuming_ReaderError(t *testing.T) {
	expectedErr := errors.New("network connection lost")

	mock := &mockReader{
		readMessageFunc: func(ctx context.Context) (kafka.Message, error) {
			return kafka.Message{}, expectedErr
		},
	}

	consumer := &Consumer{reader: mock}
	ingestionChan := make(chan []byte, 1)

	err := consumer.StartConsuming(context.Background(), ingestionChan)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected wrapped error %v, got %v", expectedErr, err)
	}
}

func TestConsumer_Close(t *testing.T) {
	closed := false
	mock := &mockReader{
		closeFunc: func() error {
			closed = true
			return nil
		},
	}

	consumer := &Consumer{reader: mock}
	err := consumer.Close()
	if err != nil {
		t.Fatalf("unexpected error on Close(): %v", err)
	}

	if !closed {
		t.Fatal("expected reader.Close() to have been called")
	}
}
