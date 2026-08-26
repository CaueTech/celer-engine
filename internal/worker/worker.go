package worker

import (
	"context"
	"time"

	"github.com/CaueTech/celer-engine/internal/domain"
)

// Worker coordinates validation and aggregation for raw incoming payloads.
type Worker struct {
	validator  domain.Validator
	aggregator domain.Aggregator
}

// NewWorker constructs a Worker with the required validator and optional aggregator.
func NewWorker(v domain.Validator, a domain.Aggregator) *Worker {
	return &Worker{
		validator:  v,
		aggregator: a,
	}
}

// ProcessPayload validates raw payload bytes:
// - If invalid: creates a DLQEnvelope and returns domain.OutboundTypeDLQ message.
// - If valid: creates domain.OutboundTypeWarning message, and if Aggregator triggers an Alert, adds domain.OutboundTypeAlert message.
func (w *Worker) ProcessPayload(rawPayload []byte) []domain.OutboundMessage {
	outboundMsgs := make([]domain.OutboundMessage, 0, 2)

	// 1. Validate payload
	event, err := w.validator.Validate(rawPayload)
	if err != nil {
		dlq := domain.DLQEnvelope{
			RawPayload:  string(rawPayload),
			ErrorReason: err.Error(),
			FailedAt:    time.Now().Unix(),
		}
		outboundMsgs = append(outboundMsgs, domain.OutboundMessage{
			Type: domain.OutboundTypeDLQ,
			DLQ:  &dlq,
		})
		return outboundMsgs
	}

	// 2. Payload is a valid domain event -> push to Warnings topic
	outboundMsgs = append(outboundMsgs, domain.OutboundMessage{
		Type:    domain.OutboundTypeWarning,
		Warning: event,
	})

	// 3. Process event through Aggregator if provided
	if w.aggregator != nil {
		alert, aggErr := w.aggregator.Process(event)
		if aggErr != nil {
			dlq := domain.DLQEnvelope{
				RawPayload:  string(rawPayload),
				ErrorReason: "aggregator error: " + aggErr.Error(),
				FailedAt:    time.Now().Unix(),
			}
			outboundMsgs = append(outboundMsgs, domain.OutboundMessage{
				Type: domain.OutboundTypeDLQ,
				DLQ:  &dlq,
			})
			return outboundMsgs
		}

		if alert != nil {
			outboundMsgs = append(outboundMsgs, domain.OutboundMessage{
				Type:  domain.OutboundTypeAlert,
				Alert: alert,
			})
		}
	}

	return outboundMsgs
}

// StartProcessing listens on ingestionChan, processes incoming raw bytes,
// and emits outbound messages into outboundChan.
func (w *Worker) StartProcessing(ctx context.Context, ingestionChan <-chan []byte, outboundChan chan<- domain.OutboundMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		case rawPayload, ok := <-ingestionChan:
			if !ok {
				return
			}
			msgs := w.ProcessPayload(rawPayload)
			for _, msg := range msgs {
				select {
				case <-ctx.Done():
					return
				case outboundChan <- msg:
				}
			}
		}
	}
}
