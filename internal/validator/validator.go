package validator

import (
	"errors"
	"fmt"

	"github.com/CaueTech/celer-engine/internal/domain"
	"github.com/CaueTech/celer-engine/internal/proto/pb"
	"google.golang.org/protobuf/proto"
)

var (
	ErrInvalidProtobuf = errors.New("payload is not a valid protobuf message")
	ErrEmptyFields     = errors.New("event_id and robot_id are required fields")
)

// ProtoValidator implements domain.Validator for Protobuf payloads
type ProtoValidator struct{}

// NewProtoValidator creates a new instance of ProtoValidator
func NewProtoValidator() *ProtoValidator {
	return &ProtoValidator{}
}

// Validate decodes binary Protobuf bytes and maps them to domain.Event
func (v *ProtoValidator) Validate(rawPayload []byte) (*domain.Event, error) {
	var protoEvent pb.EventProto

	// 1. Decoding: Binary bytes -> Generated Proto Struct
	if err := proto.Unmarshal(rawPayload, &protoEvent); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidProtobuf, err)                                                                                                                                            
	}

	// 2. Business validation rules
	if protoEvent.GetEventId() == "" || protoEvent.GetRobotId() == "" {
		return nil, ErrEmptyFields                                                                                                                                                                           
	}

	// 3. Mapping: Transport DTO (pb.EventProto) -> Domain Entity (domain.Event)
	event := &domain.Event{
		EventID:   protoEvent.GetEventId(),                                                                                                                                                                  
		RobotID:   protoEvent.GetRobotId(),                                                                                                                                                                  
		Timestamp: protoEvent.GetTimestamp(),                                                                                                                                                                
		Status:    protoEvent.GetStatus(),                                                                                                                                                                   
	}

	if protoEvent.GetTelemetry() != nil {
		event.Telemetry = protoEvent.GetTelemetry().AsMap()                                                                                                                                                  
	}

	return event, nil
}