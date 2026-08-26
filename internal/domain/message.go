package domain

// OutboundType indicates which target Kafka topic the outbound message is routed to.
type OutboundType int

const (
	OutboundTypeDLQ OutboundType = iota
	OutboundTypeWarning
	OutboundTypeAlert
)

// OutboundMessage wraps the payload and type to be dispatched by the producer.
type OutboundMessage struct {
	Type    OutboundType
	DLQ     *DLQEnvelope
	Warning *Event
	Alert   *Alert
}
