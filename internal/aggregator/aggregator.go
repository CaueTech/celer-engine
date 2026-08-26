package aggregator

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/CaueTech/celer-engine/internal/domain"
	"github.com/google/uuid"
)

var (
	ErrNilEvent     = errors.New("cannot process nil event")
	ErrEmptyRobotID = errors.New("event must contain a valid robot_id")
)

// WindowAggregator tracks event frequency per robot within a sliding time window.
type WindowAggregator struct {
	mu                    sync.Mutex
	windowDurationSeconds int64
	threshold             int
	// history maps RobotID -> sorted list of event timestamps (in seconds)
	history map[string][]int64
}

// NewWindowAggregator creates a WindowAggregator with settings provided in domain.AggregatorConfig.
func NewWindowAggregator(cfg domain.AggregatorConfig) *WindowAggregator {
	if cfg.WindowDurationSeconds <= 0 {
		cfg.WindowDurationSeconds = 60
	}
	if cfg.Threshold <= 0 {
		cfg.Threshold = 10
	}

	return &WindowAggregator{
		windowDurationSeconds: int64(cfg.WindowDurationSeconds),
		threshold:             cfg.Threshold,
		history:               make(map[string][]int64),
	}
}

// Process receives a domain Event and updates the sliding time window for event's RobotID.
// If the event count in the window reaches or exceeds threshold, an *domain.Alert is returned.
func (a *WindowAggregator) Process(event *domain.Event) (*domain.Alert, error) {
	if event == nil {
		return nil, ErrNilEvent
	}
	if event.RobotID == "" {
		return nil, ErrEmptyRobotID
	}

	a.mu.Lock()

	// This schedules the Unlock() to when the functio exits
	defer a.mu.Unlock()

	cutoff := event.Timestamp - a.windowDurationSeconds

	// Filter out expired timestamps for this robot
	timestamps := a.history[event.RobotID]
	validIndex := 0
	for _, ts := range timestamps {
		if ts >= cutoff {
			break
		}
		validIndex++
	}
	if validIndex > 0 {
		timestamps = timestamps[validIndex:]
	}

	// Add current event timestamp
	timestamps = append(timestamps, event.Timestamp)
	a.history[event.RobotID] = timestamps

	// Check if count exceeds threshold
	count := len(timestamps)
	if count >= a.threshold {
		startTS := timestamps[0]
		alert := &domain.Alert{
			AlertID:              fmt.Sprintf("alert-%s", uuid.New().String()),
			RobotID:              event.RobotID,
			RuleName:             domain.RuleTooManyErrorsInWindow,
			Severity:             domain.SeverityCritical,
			TriggeredEventsCount: count,
			TimeWindow: domain.TimeWindow{
				StartTimestamp:  startTS,
				EndTimestamp:    event.Timestamp,
				DurationSeconds: int(a.windowDurationSeconds),
			},
			GeneratedAt: time.Now().Unix(),
		}
		return alert, nil
	}

	return nil, nil
}
