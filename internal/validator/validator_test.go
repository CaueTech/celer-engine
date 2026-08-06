package validator_test

import (
	"testing"

	"github.com/CaueTech/celer-engine/internal/validator"
)

func TestValidate_Success(t *testing.T) {
	v := validator.NewJSONValidator()

	// JSON válido simulando a mensagem do Kafka
	validPayload := []byte(`{
		"event_id": "evt-123",
		"robot_id": "bot-99",
		"timestamp": 1700000000,
		"status": "ERROR"
	}`)

	event, err := v.Validate(validPayload)

	if err != nil {
		t.Fatalf("Esperava sucesso, mas obteve erro: %v", err)
	}

	if event.RobotID != "bot-99" {
		t.Errorf("Esperava robot_id 'bot-99', mas veio '%s'", event.RobotID)
	}
}

func TestValidate_InvalidJSON(t *testing.T) {
	v := validator.NewJSONValidator()

	// JSON quebrado (faltando aspa)
	brokenPayload := []byte(`{"event_id": "evt-123", "robot_id":}`)

	_, err := v.Validate(brokenPayload)

	if err == nil {
		t.Fatal("Esperava erro para JSON corrompido, mas passou direto!")
	}
}