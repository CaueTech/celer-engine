package domain

import"time"

// ChaosConfig defines settings for anomaly injection and simulation.
type ChaosConfig struct {
	ChaosRate        float64       // E.g., 0.05 (5% chance of generating a chaotic event)
	EmissionInterval time.Duration // Interval between emissions (e.g., 200ms)
	RobotPool        []string      // E.g., ["bot-01", "bot-02", ..., "bot-20"]
}