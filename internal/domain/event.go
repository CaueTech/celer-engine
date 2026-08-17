package domain

/*
	JSON struct tags are used to standardize fields in snake_case. Even though Protobuf is used in the Kafka pipeline in most cases, keeping these tags is helpful for JSON compatibility.
*/

// Event represents the telemetry payload sent by the Chaos Generator imitating robots.
type Event struct {
	EventID   string                 `json:"event_id"`
	RobotID   string                 `json:"robot_id"`
	Timestamp int64                  `json:"timestamp"`
	Status    string                 `json:"status"`

	// Maps dynamic telemetry metric keys to their values
	Telemetry map[string]interface{} `json:"telemetry"` 
}

// Alert represents the notification generated when a time window rule is violated.
type Alert struct {
	AlertID               string     `json:"alert_id"`
	RobotID               string     `json:"robot_id"`
	RuleName              string     `json:"rule_name"`
	Severity              string     `json:"severity"`
	TriggeredEventsCount  int        `json:"triggered_events_count"`
	TimeWindow            TimeWindow `json:"time_window"`
	GeneratedAt           int64      `json:"generated_at"`
}

// TimeWindow details the interval analyzed when triggering an alert.
type TimeWindow struct {
	StartTimestamp  int64 `json:"start_timestamp"`
	EndTimestamp    int64 `json:"end_timestamp"`
	DurationSeconds int   `json:"duration_seconds"`
}

// DLQEnvelope wraps corrupted or invalid data to be sent to the Dead Letter Queue.
type DLQEnvelope struct {
	RawPayload  string `json:"raw_payload"`
	ErrorReason string `json:"error_reason"`
	FailedAt    int64  `json:"failed_at"`
}

// Telemetry status constants.
const (
	StatusOK      = "OK"
	StatusWarning = "WARNING"
	StatusError   = "ERROR"
)

// Business rules and severity constants.
const (
	RuleTooManyErrorsInWindow = "TOO_MANY_ERRORS_IN_WINDOW"
	SeverityCritical          = "CRITICAL"
)