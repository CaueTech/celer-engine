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

// GeneratePayload gera os bytes do Protobuf (ou corrompidos) prontos para envio
func (g *Generator) GeneratePayload() ([]byte, error) {
	// 1. Decisão probabilística de caos
	isChaos := randFloat() < g.config.ChaosRate

	// 2. Caos extremo: payload de bytes completamente corrompido
	if isChaos && randFloat() < 0.20 { // 20% do caos é corrupção binária
		return []byte{0xFF, 0xFE, 0x00, 0xAA}, nil
	}

	// 3. Montagem do evento base
	eventID := uuid.NewString()
	robotID := g.config.RobotPool[randInt(len(g.config.RobotPool))]
	status := g.statusPool[randInt(len(g.statusPool))]
	timestamp := time.Now().Unix()

	// 4. Injeção de anomalias nos campos
	if isChaos {
		switch randInt(4) {                                                                             
		case 0:                                                                                         
			eventID = "" // Violação: event_id vazio                                                    
		case 1:                                                                                         
			robotID = "" // Violação: robot_id vazio                                                    
		case 2:                                                                                         
			if g.lastEventID != "" {                                                                    
				eventID = g.lastEventID // Duplicação de ID                                             
			}                                                                                           
		case 3:                                                                                         
			status = domain.StatusError // Força status de erro                                         
		}                                                                                               
	} else {
		g.lastEventID = eventID                                                                         
	}

	// 5. Telemetria dinâmica
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