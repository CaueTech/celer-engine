package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/CaueTech/celer-engine/internal/domain"
	"github.com/segmentio/kafka-go"
)

// Writer is an interface abstracting kafka-go Writer operations for testability.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Producer handles publishing alerts, warnings, and DLQ envelopes to Kafka.
type Producer struct {
	alertWriter   Writer
	dlqWriter     Writer
	warningWriter Writer
}

// NewProducer creates a new Producer configured with Kafka brokers and topics.
func NewProducer(brokers []string, alertTopic, dlqTopic, warningTopic string) *Producer {
	return &Producer{
		alertWriter: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    alertTopic,
			Balancer: &kafka.LeastBytes{},
		},
		dlqWriter: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    dlqTopic,
			Balancer: &kafka.LeastBytes{},
		},
		warningWriter: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    warningTopic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

// PublishAlert serializes an Alert to JSON and produces it to the alerts topic.
func (p *Producer) PublishAlert(ctx context.Context, alert domain.Alert) error {
	payload, err := json.Marshal(alert)
	if err != nil {
		return fmt.Errorf("failed to marshal alert to JSON: %w", err)
	}

	err = p.alertWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(alert.AlertID),
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("failed to publish alert message to kafka: %w", err)
	}

	return nil
}

// PublishWarning serializes a validated Event to JSON and produces it to the warnings topic.
func (p *Producer) PublishWarning(ctx context.Context, event domain.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event to JSON: %w", err)
	}

	err = p.warningWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.EventID),
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("failed to publish warning message to kafka: %w", err)
	}

	return nil
}

// PublishDLQ serializes a DLQEnvelope to JSON and produces it to the DLQ topic.
func (p *Producer) PublishDLQ(ctx context.Context, dlq domain.DLQEnvelope) error {
	payload, err := json.Marshal(dlq)
	if err != nil {
		return fmt.Errorf("failed to marshal DLQ envelope to JSON: %w", err)
	}

	err = p.dlqWriter.WriteMessages(ctx, kafka.Message{
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("failed to publish DLQ message to kafka: %w", err)
	}

	return nil
}

// Closes alert, DLQ, and warning writers.
func (p *Producer) Close() error {
	var alertErr, dlqErr, warningErr error

	if p.alertWriter != nil {
		alertErr = p.alertWriter.Close()
	}
	if p.dlqWriter != nil {
		dlqErr = p.dlqWriter.Close()
	}
	if p.warningWriter != nil {
		warningErr = p.warningWriter.Close()
	}

	if alertErr != nil || dlqErr != nil || warningErr != nil {
		return errors.Join(alertErr, dlqErr, warningErr)
	}

	return nil
}
