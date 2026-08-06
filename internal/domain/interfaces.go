package domain

// Validator é responsável por validar e fazer o parse dos dados brutos recebidos
type Validator interface {
	Validate(rawPayload []byte) (*Event, error)
}

// Aggregator gerencia a Janela de Tempo e a verificação de Idempotência
type Aggregator interface {
	// Process recebe um evento válido. Retorna um *Alert caso a regra seja disparada,
	// ou nil caso o evento seja apenas processado sem gerar alertas.
	Process(event *Event) (*Alert, error)
}