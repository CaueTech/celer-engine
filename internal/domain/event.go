package domain

// import "time"

// As structs tags json servem somente para uniformizar os campos em snake_case

// Event representa o payload de telemetria enviado pelo Gerador de Caos que "imita" robôs
type Event struct {
	EventID   string                 `json:"event_id"`
	RobotID   string                 `json:"robot_id"`
	Timestamp int64                  `json:"timestamp"`
	Status    string                 `json:"status"`

	// Relaciona uma chave string com qualquer tipagem (interface{})
	Telemetry map[string]interface{} `json:"telemetry"` 
}

// Alert representa a notificação gerada quando a regra da Janela de Tempo é violada
type Alert struct {
	AlertID               string     `json:"alert_id"`
	RobotID               string     `json:"robot_id"`
	RuleName              string     `json:"rule_name"`
	Severity              string     `json:"severity"`
	TriggeredEventsCount  int        `json:"triggered_events_count"`
	TimeWindow            TimeWindow `json:"time_window"`
	GeneratedAt           int64      `json:"generated_at"`
}

// TimeWindow detalha o intervalo analisado para a emissão do alerta
type TimeWindow struct {
	StartTimestamp  int64 `json:"start_timestamp"`
	EndTimestamp    int64 `json:"end_timestamp"`
	DurationSeconds int   `json:"duration_seconds"`
}

// DLQEnvelope empacota dados corrompidos para serem enviados à Dead Letter Queue
type DLQEnvelope struct {
	RawPayload  string `json:"raw_payload"`
	ErrorReason string `json:"error_reason"`
	FailedAt    int64  `json:"failed_at"`
}

// Constantes com as regras de negócio
const (
	StatusError               = "ERROR"
	RuleTooManyErrorsInWindow = "TOO_MANY_ERRORS_IN_WINDOW"
	SeverityCritical          = "CRITICAL"
)