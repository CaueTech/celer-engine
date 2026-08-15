package chaos_test

import (
	"testing"
	"time"

	"github.com/CaueTech/celer-engine/internal/chaos"
	"github.com/CaueTech/celer-engine/internal/domain"
	"github.com/CaueTech/celer-engine/internal/proto/pb"
	"github.com/CaueTech/celer-engine/internal/validator"
	"google.golang.org/protobuf/proto"
)

func TestGenerator_ValidPayloads(t *testing.T) {
	// With ChaosRate = 0, all payloads should be 100% valid
	cfg := domain.ChaosConfig{
		ChaosRate:        0.0,
		EmissionInterval: 100 * time.Millisecond,
		RobotPool:        []string{"bot-001", "bot-002"},
	}

	g := chaos.NewGenerator(cfg)
	v := validator.NewProtoValidator()

	for i := 0; i < 20; i++ {
		payload, err := g.GeneratePayload()
		if err != nil {
			t.Fatalf("Unexpected error generating payload: %v", err)
		}

		event, err := v.Validate(payload)
		if err != nil {
			t.Fatalf("Expected valid payload with 0%% chaos rate, got error: %v", err)
		}

		if event.EventID == "" {
			t.Error("Expected non-empty EventID")
		}
		if event.RobotID != "bot-001" && event.RobotID != "bot-002" {
			t.Errorf("Unexpected RobotID: %s", event.RobotID)
		}
	}
}

func TestGenerator_ChaosInjection(t *testing.T) {
	// With ChaosRate = 1.0, anomalies should be injected
	cfg := domain.ChaosConfig{
		ChaosRate:        1.0,
		EmissionInterval: 50 * time.Millisecond,
		RobotPool:        []string{"bot-001"},
	}

	g := chaos.NewGenerator(cfg)

	hasCorruptedBytes := false
	hasEmptyFields := false
	hasStatusError := false

	for i := 0; i < 50; i++ {
		payload, err := g.GeneratePayload()
		if err != nil {
			t.Fatalf("Unexpected error generating payload: %v", err)
		}

		var protoMsg pb.EventProto
		if err := proto.Unmarshal(payload, &protoMsg); err != nil {
			hasCorruptedBytes = true
			continue
		}

		if protoMsg.GetEventId() == "" || protoMsg.GetRobotId() == "" {
			hasEmptyFields = true
		}
		if protoMsg.GetStatus() == domain.StatusError {
			hasStatusError = true
		}
	}

	if !hasCorruptedBytes && !hasEmptyFields && !hasStatusError {
		t.Error("Expected at least one type of chaos anomaly to be generated when ChaosRate is 1.0")
	}
}
