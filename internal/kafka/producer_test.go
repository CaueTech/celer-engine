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

func TestPublishWarning_Success(t *testing.T) {
	event := domain.Event{
		EventID:   "evt-100",
		RobotID:   "robot-789",
		Timestamp: time.Now().Unix(),
		Status:    domain.StatusOK,
	}

	writtenMsgs := []kafka.Message{}
	mockWarningWriter := &mockWriter{
		writeMessagesFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			writtenMsgs = append(writtenMsgs, msgs...)
			return nil
		},
	}

	producer := &Producer{
		alertWriter:   &mockWriter{},
		dlqWriter:     &mockWriter{},
		warningWriter: mockWarningWriter,
	}

	err := producer.PublishWarning(context.Background(), event)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(writtenMsgs) != 1 {
		t.Fatalf("expected 1 message written, got: %d", len(writtenMsgs))
	}

	msg := writtenMsgs[0]
	if string(msg.Key) != event.EventID {
		t.Errorf("expected msg key %s, got: %s", event.EventID, string(msg.Key))
	}

	var unmarshaledEvent domain.Event
	if err := json.Unmarshal(msg.Value, &unmarshaledEvent); err != nil {
		t.Fatalf("failed to unmarshal published event JSON: %v", err)
	}

	if !reflect.DeepEqual(unmarshaledEvent, event) {
		t.Errorf("expected published warning to match input event.\nGot: %+v\nWant: %+v", unmarshaledEvent, event)
	}
}

func TestPublishWarning_WriterError(t *testing.T) {
	event := domain.Event{EventID: "evt-101", RobotID: "robot-789"}
	expectedErr := errors.New("kafka cluster unreachable")

	mockWarningWriter := &mockWriter{
		writeMessagesFunc: func(ctx context.Context, msgs ...kafka.Message) error {
			return expectedErr
		},
	}

	producer := &Producer{
		alertWriter:   &mockWriter{},
		dlqWriter:     &mockWriter{},
		warningWriter: mockWarningWriter,
	}

	err := producer.PublishWarning(context.Background(), event)
	if err == nil {
		t.Fatal("expected error publishing warning, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected wrapped error %v, got: %v", expectedErr, err)
	}
}

func TestProducer_Close_Success(t *testing.T) {
	alertClosed := false
	dlqClosed := false
	warningClosed := false

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
		warningWriter: &mockWriter{
			closeFunc: func() error {
				warningClosed = true
				return nil
			},
		},
	}

	err := producer.Close()
	if err != nil {
		t.Fatalf("expected no error closing producer, got: %v", err)
	}

	if !alertClosed || !dlqClosed || !warningClosed {
		t.Errorf("expected all writers to be closed. alertClosed=%v, dlqClosed=%v, warningClosed=%v", alertClosed, dlqClosed, warningClosed)
	}
}

func TestProducer_Close_Errors(t *testing.T) {
	errAlert := errors.New("alert close error")

	producer1 := &Producer{
		alertWriter:   &mockWriter{closeFunc: func() error { return errAlert }},
		dlqWriter:     &mockWriter{closeFunc: func() error { return nil }},
		warningWriter: &mockWriter{closeFunc: func() error { return nil }},
	}
	err1 := producer1.Close()
	if !errors.Is(err1, errAlert) {
		t.Errorf("expected alert close error, got: %v", err1)
	}
}

func TestNewProducer(t *testing.T) {
	producer := NewProducer([]string{"localhost:9092"}, "alerts-topic", "dlq-topic", "warnings-topic")
	if producer == nil {
		t.Fatal("expected NewProducer to return non-nil instance")
	}

	if producer.alertWriter == nil || producer.dlqWriter == nil || producer.warningWriter == nil {
		t.Error("expected NewProducer to initialize alertWriter, dlqWriter, and warningWriter")
	}
}
