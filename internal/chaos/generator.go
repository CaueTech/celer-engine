package chaos

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"github.com/CaueTech/celer-engine/internal/domain"
	"github.com/CaueTech/celer-engine/internal/proto/pb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type Generator struct {
	config      domain.ChaosConfig
	statusPool  []string
	lastEventID string
}

func NewGenerator(cfg domain.ChaosConfig) *Generator {
	if len(cfg.RobotPool) == 0 {
		cfg.RobotPool = make([]string, 20)
		for i := 0; i < 20; i++ {
			cfg.RobotPool[i] = fmt.Sprintf("bot-%03d", i+1)
		}
	}

	return &Generator{
		config: cfg,
		statusPool: []string{
			domain.StatusOK,
			domain.StatusOK,
			domain.StatusOK,
			domain.StatusWarning,
			domain.StatusError,
		},
	}
}

// GeneratePayload generates serialized Protobuf bytes (or corrupted data) ready for transmission.
func (g *Generator) GeneratePayload() ([]byte, error) {
	// 1. Probabilistic decision for chaos injection
	isChaos := randFloat() < g.config.ChaosRate

	// 2. Extreme chaos: completely corrupted binary payload
	if isChaos && randFloat() < 0.20 { // 20% of chaos is binary corruption
		return []byte{0xFF, 0xFE, 0x00, 0xAA}, nil
	}

	// 3. Base event assembly
	eventID := uuid.NewString()
	robotID := g.config.RobotPool[randInt(len(g.config.RobotPool))]
	status := g.statusPool[randInt(len(g.statusPool))]
	timestamp := time.Now().Unix()

	// 4. Anomaly injection into fields
	if isChaos {
		switch randInt(4) {
		case 0:
			eventID = "" // Violation: empty event_id
		case 1:
			robotID = "" // Violation: empty robot_id
		case 2:
			if g.lastEventID != "" {
				eventID = g.lastEventID // Duplicate ID anomaly
			}
		case 3:
			status = domain.StatusError // Force error status
		}
	} else {
		g.lastEventID = eventID
	}

	// 5. Dynamic telemetry
	telemetryMap := map[string]interface{}{
		"battery_level": randInt(100),
		"cpu_temp":      40.0 + (randFloat() * 45.0),
	}
	telemetryStruct, _ := structpb.NewStruct(telemetryMap)

	protoMsg := &pb.EventProto{
		EventId:   eventID,                                                                             
		RobotId:   robotID,                                                                             
		Timestamp: timestamp,                                                                           
		Status:    status,
		Telemetry: telemetryStruct,
	}

	return proto.Marshal(protoMsg)
}

func randInt(max int) int {
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(max)))
	return int(n.Int64())
}

func randFloat() float64 {
	n, _ := rand.Int(rand.Reader, big.NewInt(10000))
	return float64(n.Int64()) / 10000.0
}