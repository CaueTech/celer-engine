package kafka

import (
    "context"
    "fmt"
    "github.com/segmentio/kafka-go"
)

type Reader interface {
    ReadMessage(ctx context.Context) (kafka.Message, error)
    Close() error
}

type Consumer struct {
    reader Reader
}

func NewConsumer(brokers []string, topic string, groupID string) *Consumer {
    return &Consumer{
        reader: kafka.NewReader(kafka.ReaderConfig{
            Brokers:  brokers,
            Topic:    topic,
            GroupID:  groupID,
            MinBytes: 10,        // 10B
            MaxBytes: 10e6,       // 10MB
        }),
    }
}

func (c *Consumer) StartConsuming(ctx context.Context, ingestionChan chan<- []byte) error {
	/*
		ctx is used to signal the termination of consumption. In Go, ctx.Done() is a zero-memory channel that stays blocked until canceled. While blocked, the consumer reads data normally. When ctx.Done() is closed, the consumer stops reading and returns the context error (ctx.Err()).
		ingestionChan is the channel where raw bytes read from Kafka are sent. The channel is blocking, meaning if the channel is full, the consumer will wait until there is space to send the bytes.
	*/
	for {
		/* 
			The select statement is used to wait on multiple channels. In this case, it waits for ctx.Done() and sends bytes to ingestionChan. If ctx.Done() is closed, the consumer returns the context error. Otherwise, it proceeds to read from Kafka.
		*/
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// 1. Read bytes from the topic via TCP
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				return fmt.Errorf("Error while reading message from kafka: %w", err)
			}

			// 2. Transfer []byte directly to the channel (with Backpressure)
			ingestionChan <- msg.Value
		}
	}
}

func (c *Consumer) Close() error {
    return c.reader.Close()
}