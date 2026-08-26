package domain

import "context"

// Validator is responsible for validating and parsing raw incoming payloads.
type Validator interface {
	Validate(rawPayload []byte) (*Event, error)
}

// Aggregator manages the Time Window and idempotency checks.
type Aggregator interface {
	// Process receives a valid event. Returns an *Alert if a rule is triggered,
	// or nil if the event is processed without generating alerts.
	Process(event *Event) (*Alert, error)
}

type MessageProducer interface {
	PublishAlert(ctx context.Context, alert Alert) error
	PublishWarning(ctx context.Context, event Event) error
	PublishDLQ(ctx context.Context, dlq DLQEnvelope) error
	Close() error
}

type MessageConsumer interface {
    // StartConsuming receives the channel where raw bytes should be sent.
    StartConsuming(ctx context.Context, ingestionChan chan<- []byte) error
    Close() error
}