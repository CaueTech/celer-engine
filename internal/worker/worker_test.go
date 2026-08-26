package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/CaueTech/celer-engine/internal/domain"
)

// Mocks

type mockValidator struct {
	validateFunc func(rawPayload []byte) (*domain.Event, error)
}

func (m *mockValidator) Validate(rawPayload []byte) (*domain.Event, error) {
	if m.validateFunc != nil {
		return m.validateFunc(rawPayload)
	}
	return nil, errors.New("default mock validator error")
}

type mockAggregator struct {
	processFunc func(event *domain.Event) (*domain.Alert, error)
}

func (m *mockAggregator) Process(event *domain.Event) (*domain.Alert, error) {
	if m.processFunc != nil {
		return m.processFunc(event)
	}
	return nil, nil
}

type mockConsumer struct {
	startConsumingFunc func(ctx context.Context, ingestionChan chan<- []byte) error
	closeFunc          func() error
}

func (m *mockConsumer) StartConsuming(ctx context.Context, ingestionChan chan<- []byte) error {
	if m.startConsumingFunc != nil {
		return m.startConsumingFunc(ctx, ingestionChan)
	}
	return nil
}

func (m *mockConsumer) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

type mockProducer struct {
	mu             sync.Mutex
	alertsPublished   []domain.Alert
	warningsPublished []domain.Event
	dlqPublished      []domain.DLQEnvelope
}

func (m *mockProducer) PublishAlert(ctx context.Context, alert domain.Alert) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alertsPublished = append(m.alertsPublished, alert)
	return nil
}

func (m *mockProducer) PublishWarning(ctx context.Context, event domain.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.warningsPublished = append(m.warningsPublished, event)
	return nil
}

func (m *mockProducer) PublishDLQ(ctx context.Context, dlq domain.DLQEnvelope) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dlqPublished = append(m.dlqPublished, dlq)
	return nil
}

func (m *mockProducer) Close() error {
	return nil
}

// Tests

func TestWorker_ProcessPayload_InvalidPayload(t *testing.T) {
	valErr := errors.New("corrupted binary")
	v := &mockValidator{
		validateFunc: func(rawPayload []byte) (*domain.Event, error) {
			return nil, valErr
		},
	}
	w := NewWorker(v, nil)

	msgs := w.ProcessPayload([]byte("bad_data"))
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message for invalid payload, got %d", len(msgs))
	}
	if msgs[0].Type != OutboundTypeDLQ {
		t.Errorf("expected OutboundTypeDLQ, got %v", msgs[0].Type)
	}
	if msgs[0].DLQ == nil || msgs[0].DLQ.ErrorReason != valErr.Error() {
		t.Errorf("expected DLQ error reason %s, got %+v", valErr.Error(), msgs[0].DLQ)
	}
}

func TestWorker_ProcessPayload_ValidEvent_NoAlert(t *testing.T) {
	evt := &domain.Event{EventID: "evt-001", RobotID: "robot-1", Timestamp: 100}
	v := &mockValidator{
		validateFunc: func(rawPayload []byte) (*domain.Event, error) {
			return evt, nil
		},
	}
	a := &mockAggregator{
		processFunc: func(event *domain.Event) (*domain.Alert, error) {
			return nil, nil // No alert
		},
	}
	w := NewWorker(v, a)

	msgs := w.ProcessPayload([]byte("valid_bytes"))
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message (Warning), got %d", len(msgs))
	}
	if msgs[0].Type != OutboundTypeWarning {
		t.Errorf("expected OutboundTypeWarning, got %v", msgs[0].Type)
	}
	if msgs[0].Warning.EventID != "evt-001" {
		t.Errorf("expected Warning EventID evt-001, got %s", msgs[0].Warning.EventID)
	}
}

func TestWorker_ProcessPayload_ValidEvent_TriggersAlert(t *testing.T) {
	evt := &domain.Event{EventID: "evt-002", RobotID: "robot-1", Timestamp: 100}
	alert := &domain.Alert{AlertID: "alert-001", RobotID: "robot-1"}

	v := &mockValidator{
		validateFunc: func(rawPayload []byte) (*domain.Event, error) {
			return evt, nil
		},
	}
	a := &mockAggregator{
		processFunc: func(event *domain.Event) (*domain.Alert, error) {
			return alert, nil
		},
	}
	w := NewWorker(v, a)

	msgs := w.ProcessPayload([]byte("valid_bytes"))
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages (Warning + Alert), got %d", len(msgs))
	}
	if msgs[0].Type != OutboundTypeWarning {
		t.Errorf("expected first message to be OutboundTypeWarning, got %v", msgs[0].Type)
	}
	if msgs[1].Type != OutboundTypeAlert {
		t.Errorf("expected second message to be OutboundTypeAlert, got %v", msgs[1].Type)
	}
	if msgs[1].Alert.AlertID != "alert-001" {
		t.Errorf("expected AlertID alert-001, got %s", msgs[1].Alert.AlertID)
	}
}

func TestPipeline_FullIntegration(t *testing.T) {
	v := &mockValidator{
		validateFunc: func(rawPayload []byte) (*domain.Event, error) {
			if string(rawPayload) == "bad" {
				return nil, errors.New("bad payload")
			}
			return &domain.Event{EventID: "evt-" + string(rawPayload), RobotID: "robot-1"}, nil
		},
	}
	a := &mockAggregator{
		processFunc: func(event *domain.Event) (*domain.Alert, error) {
			if event.EventID == "evt-alert" {
				return &domain.Alert{AlertID: "alert-999", RobotID: "robot-1"}, nil
			}
			return nil, nil
		},
	}
	worker := NewWorker(v, a)
	producer := &mockProducer{}

	consumer := &mockConsumer{
		startConsumingFunc: func(ctx context.Context, ingestionChan chan<- []byte) error {
			ingestionChan <- []byte("bad")
			ingestionChan <- []byte("good")
			ingestionChan <- []byte("alert")
			return nil
		},
	}

	pipeline := NewPipeline(consumer, worker, producer, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := pipeline.Run(ctx)
	if err != nil {
		t.Fatalf("pipeline returned unexpected error: %v", err)
	}

	producer.mu.Lock()
	defer producer.mu.Unlock()

	if len(producer.dlqPublished) != 1 {
		t.Errorf("expected 1 DLQ published, got %d", len(producer.dlqPublished))
	}
	if len(producer.warningsPublished) != 2 {
		t.Errorf("expected 2 Warnings published, got %d", len(producer.warningsPublished))
	}
	if len(producer.alertsPublished) != 1 {
		t.Errorf("expected 1 Alert published, got %d", len(producer.alertsPublished))
	}
}

func TestPipeline_ContextCancel(t *testing.T) {
	v := &mockValidator{
		validateFunc: func(rawPayload []byte) (*domain.Event, error) {
			return &domain.Event{EventID: "e", RobotID: "r"}, nil
		},
	}
	worker := NewWorker(v, nil)
	producer := &mockProducer{}

	consumer := &mockConsumer{
		startConsumingFunc: func(ctx context.Context, ingestionChan chan<- []byte) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}

	pipeline := NewPipeline(consumer, worker, producer, 10)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := pipeline.Run(ctx)
	if err != nil {
		t.Fatalf("expected clean shutdown on context cancel, got error: %v", err)
	}
}
