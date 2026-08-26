package domain

import "time"

// ChaosConfig defines settings for anomaly injection and telemetry simulation.
type ChaosConfig struct {
	ChaosRate        float64       // E.g., 0.05 (5% chance of generating a chaotic event)
	EmissionInterval time.Duration // Interval between emissions (e.g., 200ms)
	RobotPool        []string      // E.g., ["bot-01", "bot-02", ..., "bot-20"]
}

// AggregatorConfig defines settings for the sliding time-window aggregator.
type AggregatorConfig struct {
	WindowDurationSeconds int // Duration of sliding window in seconds (e.g., 60)
	Threshold             int // Event count threshold to trigger an alert (e.g., 5)
}

// PipelineConfig defines settings for channel buffering and concurrency.
type PipelineConfig struct {
	BufferSize int // Buffer size for ingestionChan and outboundChan (e.g., 100)
}
