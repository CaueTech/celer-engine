package domain

import "time"

// ChaosConfig define as configurações para simulação e injeção de anomalias
type ChaosConfig struct {
	ChaosRate        float64       // Ex: 0.05 (5% de chance de gerar evento caótico)
	EmissionInterval time.Duration // Intervalo entre emissões (ex: 200ms)
	RobotPool        []string      // Ex: ["bot-01", "bot-02", ..., "bot-20"]
}