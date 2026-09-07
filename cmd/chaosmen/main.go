package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/CaueTech/celer-engine/internal/application/chaos"
	"github.com/CaueTech/celer-engine/internal/domain"
	"github.com/segmentio/kafka-go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	writer := &kafka.Writer{
		Addr:     kafka.TCP(env("KAFKA_BROKERS", "localhost:9092")),
		Topic:    env("KAFKA_INPUT_TOPIC", "input"),
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	generator := chaos.NewGenerator(domain.ChaosConfig{
		ChaosRate:        floatEnv("CHAOS_RATE", 0.05),
		EmissionInterval: durationEnv("CHAOS_INTERVAL", 200*time.Millisecond),
	})

	ticker := time.NewTicker(durationEnv("CHAOS_INTERVAL", 200*time.Millisecond))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			payload, err := generator.GeneratePayload()
			if err != nil {
				log.Printf("generate payload: %v", err)
				continue
			}
			if err := writer.WriteMessages(ctx, kafka.Message{Value: payload}); err != nil && ctx.Err() == nil {
				log.Printf("publish payload: %v", err)
			}
		}
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func floatEnv(key string, fallback float64) float64 {
	value, err := strconv.ParseFloat(os.Getenv(key), 64)
	if err != nil {
		return fallback
	}
	return value
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
