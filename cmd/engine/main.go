package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/CaueTech/celer-engine/internal/application/aggregator"
	"github.com/CaueTech/celer-engine/internal/application/worker"
	"github.com/CaueTech/celer-engine/internal/domain"
	"github.com/CaueTech/celer-engine/internal/infrastructure/kafka"
	"github.com/CaueTech/celer-engine/internal/infrastructure/validator"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	brokers := splitEnv("KAFKA_BROKERS", "localhost:9092")
	consumer := kafka.NewConsumer(brokers, env("KAFKA_INPUT_TOPIC", "input"), env("KAFKA_CONSUMER_GROUP", "celer-engine"))
	producer := kafka.NewProducer(
		brokers,
		env("KAFKA_ALERTS_TOPIC", "alerts"),
		env("KAFKA_DLQ_TOPIC", "dlq"),
		env("KAFKA_WARNINGS_TOPIC", "warnings"),
	)
	defer consumer.Close()
	defer producer.Close()

	processor := worker.NewWorker(
		validator.NewProtoValidator(),
		aggregator.NewWindowAggregator(domain.AggregatorConfig{}),
	)
	return worker.NewPipeline(consumer, processor, producer, domain.PipelineConfig{}).Run(ctx)
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func splitEnv(key, fallback string) []string {
	values := strings.Split(env(key, fallback), ",")
	brokers := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			brokers = append(brokers, value)
		}
	}
	return brokers
}
