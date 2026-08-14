package validator_test

import (
	"testing"

	"github.com/CaueTech/celer-engine/internal/proto/pb"
	"github.com/CaueTech/celer-engine/internal/validator"
	"google.golang.org/protobuf/proto"
)

func TestValidate_Success(t *testing.T) {
	v := validator.NewProtoValidator()

	// 1. Create a Protobuf message and serialize it to binary bytes
	protoMsg := &pb.EventProto{
		EventId:   "evt-123",                                                                                                                                                                                
		RobotId:   "bot-99",                                                                                                                                                                                 
		Timestamp: 1700000000,                                                                                                                                                                               
		Status:    "ERROR",                                                                                                                                                                                  
	}

	validPayload, err := proto.Marshal(protoMsg)
	if err != nil {
		t.Fatalf("Erro ao serializar proto: %v", err)                                                                                                                                                        
	}

	// 2. Pass the binary payload to the validator
	event, err := v.Validate(validPayload)
	if err != nil {
		t.Fatalf("Esperava sucesso, mas obteve erro: %v", err)
	}

	if event.RobotID != "bot-99" {
		t.Errorf("Esperava robot_id 'bot-99', mas veio '%s'", event.RobotID)
	}
	if event.EventID != "evt-123" {
		t.Errorf("Esperava event_id 'evt-123', mas veio '%s'", event.EventID)
	}
}

func TestValidate_InvalidPayload(t *testing.T) {
	v := validator.NewProtoValidator()

	// Corrupted binary payload (0xFF is not a valid Protobuf field header)
	corruptedPayload := []byte{0xFF, 0xFF, 0xFF, 0xFF}

	_, err := v.Validate(corruptedPayload)
	if err == nil {
		t.Fatal("Esperava erro para payload binário corrompido, mas passou direto!")
	}
}

func TestValidate_EmptyRequiredFields(t *testing.T) {
	v := validator.NewProtoValidator()

	// Protobuf message with missing required fields
	protoMsg := &pb.EventProto{
		EventId: "",
		RobotId: "",
	}

	payload, err := proto.Marshal(protoMsg)
	if err != nil {
		t.Fatalf("Erro ao serializar proto: %v", err)
	}

	_, err = v.Validate(payload)
	if err != validator.ErrEmptyFields {
		t.Fatalf("Esperava ErrEmptyFields, mas veio: %v", err)
	}
}