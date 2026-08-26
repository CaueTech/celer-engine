package aggregator

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/CaueTech/celer-engine/internal/domain"
)

func TestWindowAggregator_BelowThreshold(t *testing.T) {
	agg := NewWindowAggregator(60, 3)
	now := time.Now().Unix()

	event1 := &domain.Event{EventID: "e1", RobotID: "robot-1", Timestamp: now}
	event2 := &domain.Event{EventID: "e2", RobotID: "robot-1", Timestamp: now + 1}

	alert1, err1 := agg.Process(event1)
	if err1 != nil || alert1 != nil {
		t.Fatalf("expected no alert and no error for event1, got alert=%v, err=%v", alert1, err1)
	}

	alert2, err2 := agg.Process(event2)
	if err2 != nil || alert2 != nil {
		t.Fatalf("expected no alert and no error for event2, got alert=%v, err=%v", alert2, err2)
	}
}

func TestWindowAggregator_TriggerAlert(t *testing.T) {
	agg := NewWindowAggregator(60, 3)
	now := time.Now().Unix()

	event1 := &domain.Event{EventID: "e1", RobotID: "robot-1", Timestamp: now}
	event2 := &domain.Event{EventID: "e2", RobotID: "robot-1", Timestamp: now + 2}
	event3 := &domain.Event{EventID: "e3", RobotID: "robot-1", Timestamp: now + 5}

	_, _ = agg.Process(event1)
	_, _ = agg.Process(event2)

	alert, err := agg.Process(event3)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if alert == nil {
		t.Fatal("expected alert to be generated when threshold reached, got nil")
	}

	if alert.RobotID != "robot-1" {
		t.Errorf("expected RobotID 'robot-1', got: %s", alert.RobotID)
	}
	if alert.RuleName != domain.RuleTooManyErrorsInWindow {
		t.Errorf("expected RuleName '%s', got: %s", domain.RuleTooManyErrorsInWindow, alert.RuleName)
	}
	if alert.Severity != domain.SeverityCritical {
		t.Errorf("expected Severity '%s', got: %s", domain.SeverityCritical, alert.Severity)
	}
	if alert.TriggeredEventsCount != 3 {
		t.Errorf("expected TriggeredEventsCount 3, got: %d", alert.TriggeredEventsCount)
	}
	if alert.TimeWindow.DurationSeconds != 60 {
		t.Errorf("expected DurationSeconds 60, got: %d", alert.TimeWindow.DurationSeconds)
	}
}

func TestWindowAggregator_ExpiredEvents(t *testing.T) {
	agg := NewWindowAggregator(10, 3)
	baseTime := int64(1000)

	// Event at t=1000
	event1 := &domain.Event{EventID: "e1", RobotID: "robot-1", Timestamp: baseTime}
	// Event at t=1005
	event2 := &domain.Event{EventID: "e2", RobotID: "robot-1", Timestamp: baseTime + 5}
	// Event at t=1015 (event1 at t=1000 is now expired since 1015 - 10 = 1005 > 1000)
	event3 := &domain.Event{EventID: "e3", RobotID: "robot-1", Timestamp: baseTime + 15}

	_, _ = agg.Process(event1)
	_, _ = agg.Process(event2)

	alert, err := agg.Process(event3)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if alert != nil {
		t.Errorf("expected no alert because event1 expired, got: %+v", alert)
	}
}

func TestWindowAggregator_MultiRobotIsolation(t *testing.T) {
	agg := NewWindowAggregator(60, 2)
	now := time.Now().Unix()

	eventA := &domain.Event{EventID: "e1", RobotID: "robot-A", Timestamp: now}
	eventB := &domain.Event{EventID: "e2", RobotID: "robot-B", Timestamp: now}

	alertA, _ := agg.Process(eventA)
	alertB, _ := agg.Process(eventB)

	if alertA != nil || alertB != nil {
		t.Errorf("expected no alerts yet for single events per robot, got alertA=%v, alertB=%v", alertA, alertB)
	}

	// Robot A gets second event -> triggers alert
	eventA2 := &domain.Event{EventID: "e3", RobotID: "robot-A", Timestamp: now + 1}
	alertA2, err := agg.Process(eventA2)
	if err != nil || alertA2 == nil {
		t.Fatalf("expected alert for robot-A, got alert=%v, err=%v", alertA2, err)
	}
	if alertA2.RobotID != "robot-A" {
		t.Errorf("expected alert for robot-A, got: %s", alertA2.RobotID)
	}
}

func TestWindowAggregator_InvalidInputs(t *testing.T) {
	agg := NewWindowAggregator(60, 3)

	_, err1 := agg.Process(nil)
	if !errors.Is(err1, ErrNilEvent) {
		t.Errorf("expected ErrNilEvent, got: %v", err1)
	}

	eventEmptyRobot := &domain.Event{EventID: "e1", RobotID: "", Timestamp: 100}
	_, err2 := agg.Process(eventEmptyRobot)
	if !errors.Is(err2, ErrEmptyRobotID) {
		t.Errorf("expected ErrEmptyRobotID, got: %v", err2)
	}
}

func TestWindowAggregator_ThreadSafety(t *testing.T) {
	agg := NewWindowAggregator(60, 1000)
	var wg sync.WaitGroup
	now := time.Now().Unix()

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(robotIndex int) {
			defer wg.Done()
			robotID := fmt.Sprintf("robot-%d", robotIndex%5)
			for j := 0; j < 20; j++ {
				ev := &domain.Event{
					EventID:   fmt.Sprintf("e-%d-%d", robotIndex, j),
					RobotID:   robotID,
					Timestamp: now + int64(j),
				}
				_, _ = agg.Process(ev)
			}
		}(i)
	}

	wg.Wait()
}
