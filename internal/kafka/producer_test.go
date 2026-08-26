package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/CaueTech/celer-engine/internal/domain"
	"github.com/segmentio/kafka-go"
)

type mockWriter struct {
	writeMessagesFunc func(ctx context.Context, msgs ...kafka.Message) error
	closeFunc         func() error
}

func (m *mockWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	if m.writeMessagesFunc != nil {
		return m.writeMessagesFunc(ctx, msgs...)
	}
	return nil
}

func (m *mockWriter) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func TestPublishAlert_Success(t *testing.T) {
	alert := domain.Alert{
		AlertID:              "alert-001",
		RobotID:              "robot-123",
		RuleName:             domain.RuleTooManyErrorsInWindow,
		Severity:             domain.SeverityCritical,
		TriggeredEventsCount: 5,
		TimeWindow: domain.TimeWindow{
			StartTimestamp:  1000,
			EndTimestamp:    2000,
			DurationSeconds: 60,
		},
		GeneratedAt: time.Now().Unix(),
	}

	writtenMsgs := []kafka.Message{}
	mockAlertWriter := &mockWriter{
		writeMessagesFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			writtenMsgs = append(writtenMsgs, msgs...)
			return nil
		},
	}

	producer := &Producer{
		alertWriter: mockAlertWriter,
		dlqWriter:   &mockWriter{},
	}

	err := producer.PublishAlert(context.Background(), alert)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(writtenMsgs) != 1 {
		t.Fatalf("expected 1 message written, got: %d", len(writtenMsgs))
	}

	msg := writtenMsgs[0]
	if string(msg.Key) != alert.AlertID {
		t.Errorf("expected msg key %s, got: %s", alert.AlertID, string(msg.Key))
	}

	var unmarshaledAlert domain.Alert
	if err := json.Unmarshal(msg.Value, &unmarshaledAlert); err != nil {
		t.Fatalf("failed to unmarshal published alert JSON: %v", err)
	}

	if !reflect.DeepEqual(unmarshaledAlert, alert) {
		t.Errorf("expected published alert to match input alert.\nGot: %+v\nWant: %+v", unmarshaledAlert, alert)
	}
}

func TestPublishAlert_WriterError(t *testing.T) {
	alert := domain.Alert{AlertID: "alert-002", RobotID: "robot-456"}
	expectedErr := errors.New("kafka cluster unreachable")

	mockAlertWriter := &mockWriter{
		writeMessagesFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			return expectedErr
		},
	}

	producer := &Producer{
		alertWriter: mockAlertWriter,
		dlqWriter:   &mockWriter{},
	}

	err := producer.PublishAlert(context.Background(), alert)
	if err == nil {
		t.Fatal("expected error publishing alert, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected wrapped error %v, got: %v", expectedErr, err)
	}
}

func TestPublishDLQ_Success(t *testing.T) {
	dlq := domain.DLQEnvelope{
		RawPayload:  "corrupted_bytes_data",
		ErrorReason: "payload is not a valid protobuf message",
		FailedAt:    time.Now().Unix(),
	}

	writtenMsgs := []kafka.Message{}
	mockDLQWriter := &mockWriter{
		writeMessagesFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			writtenMsgs = append(writtenMsgs, msgs...)
			return nil
		},
	}

	producer := &Producer{
		alertWriter: &mockWriter{},
		dlqWriter:   mockDLQWriter,
	}

	err := producer.PublishDLQ(context.Background(), dlq)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(writtenMsgs) != 1 {
		t.Fatalf("expected 1 message written, got: %d", len(writtenMsgs))
	}

	msg := writtenMsgs[0]
	var unmarshaledDLQ domain.DLQEnvelope
	if err := json.Unmarshal(msg.Value, &unmarshaledDLQ); err != nil {
		t.Fatalf("failed to unmarshal published DLQ envelope JSON: %v", err)
	}

	if !reflect.DeepEqual(unmarshaledDLQ, dlq) {
		t.Errorf("expected published DLQ to match input DLQ.\nGot: %+v\nWant: %+v", unmarshaledDLQ, dlq)
	}
}

func TestPublishDLQ_WriterError(t *testing.T) {
	dlq := domain.DLQEnvelope{RawPayload: "bad_payload"}
	expectedErr := errors.New("topic not found")

	mockDLQWriter := &mockWriter{
		writeMessagesFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			return expectedErr
		},
	}

	producer := &Producer{
		alertWriter: &mockWriter{},
		dlqWriter:   mockDLQWriter,
	}

	err := producer.PublishDLQ(context.Background(), dlq)
	if err == nil {
		t.Fatal("expected error publishing DLQ, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected wrapped error %v, got: %v", expectedErr, err)
	}
}

func TestProducer_Close_Success(t *testing.T) {
	alertClosed := false
	dlqClosed := false

	producer := &Producer{
		alertWriter: &mockWriter{
			closeFunc: func() error {
				alertClosed = true
				return nil
			},
		},
		dlqWriter: &mockWriter{
			closeFunc: func() error {
				dlqClosed = true
				return nil
			},
		},
	}

	err := producer.Close()
	if err != nil {
		t.Fatalf("expected no error closing producer, got: %v", err)
	}

	if !alertClosed || !dlqClosed {
		t.Errorf("expected both writers to be closed. alertClosed=%v, dlqClosed=%v", alertClosed, dlqClosed)
	}
}

func TestProducer_Close_Errors(t *testing.T) {
	errAlert := errors.New("alert close error")
	errDLQ := errors.New("dlq close error")

	// Alert writer error only
	producer1 := &Producer{
		alertWriter: &mockWriter{closeFunc: func() error { return errAlert }},
		dlqWriter:   &mockWriter{closeFunc: func() error { return nil }},
	}
	err1 := producer1.Close()
	if !errors.Is(err1, errAlert) {
		t.Errorf("expected alert close error, got: %v", err1)
	}

	// DLQ writer error only
	producer2 := &Producer{
		alertWriter: &mockWriter{closeFunc: func() error { return nil }},
		dlqWriter:   &mockWriter{closeFunc: func() error { return errDLQ }},
	}
	err2 := producer2.Close()
	if !errors.Is(err2, errDLQ) {
		t.Errorf("expected DLQ close error, got: %v", err2)
	}

	// Both error
	producer3 := &Producer{
		alertWriter: &mockWriter{closeFunc: func() error { return errAlert }},
		dlqWriter:   &mockWriter{closeFunc: func() error { return errDLQ }},
	}
	err3 := producer3.Close()
	if err3 == nil {
		t.Error("expected error when both writers fail to close, got nil")
	}
}

func TestNewProducer(t *testing.T) {
	producer := NewProducer([]string{"localhost:9092"}, "alerts-topic", "dlq-topic")
	if producer == nil {
		t.Fatal("expected NewProducer to return non-nil instance")
	}

	if producer.alertWriter == nil || producer.dlqWriter == nil {
		t.Error("expected NewProducer to initialize alertWriter and dlqWriter")
	}
}
