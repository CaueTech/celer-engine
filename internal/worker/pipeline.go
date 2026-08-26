package worker

import (
	"context"
	"fmt"
	"sync"

	"github.com/CaueTech/celer-engine/internal/domain"
)

// Pipeline manages the 3 concurrent execution threads: Consumer, Worker, and Producer.
type Pipeline struct {
	consumer   domain.MessageConsumer
	worker     *Worker
	producer   domain.MessageProducer
	bufferSize int
}

// NewPipeline creates a new Pipeline coordinator configured via domain.PipelineConfig.
func NewPipeline(consumer domain.MessageConsumer, worker *Worker, producer domain.MessageProducer, cfg domain.PipelineConfig) *Pipeline {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 100
	}
	return &Pipeline{
		consumer:   consumer,
		worker:     worker,
		producer:   producer,
		bufferSize: cfg.BufferSize,
	}
}

// Run executes the 3 goroutines concurrently (Consumer, Worker, Producer dispatcher).
// It blocks until context is canceled or all goroutines complete.
func (p *Pipeline) Run(ctx context.Context) error {
	ingestionChan := make(chan []byte, p.bufferSize)
	outboundChan := make(chan domain.OutboundMessage, p.bufferSize)

	var wg sync.WaitGroup

	// Thread 1: Consumer
	var consumerErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(ingestionChan)
		consumerErr = p.consumer.StartConsuming(ctx, ingestionChan)
	}()

	// Thread 2: Worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(outboundChan)
		p.worker.StartProcessing(ctx, ingestionChan, outboundChan)
	}()

	// Thread 3: Producer Dispatcher
	var producerErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-outboundChan:
				if !ok {
					return
				}
				var err error
				switch msg.Type {
				case domain.OutboundTypeDLQ:
					if msg.DLQ != nil {
						err = p.producer.PublishDLQ(ctx, *msg.DLQ)
					}
				case domain.OutboundTypeWarning:
					if msg.Warning != nil {
						err = p.producer.PublishWarning(ctx, *msg.Warning)
					}
				case domain.OutboundTypeAlert:
					if msg.Alert != nil {
						err = p.producer.PublishAlert(ctx, *msg.Alert)
					}
				}
				if err != nil && producerErr == nil {
					producerErr = fmt.Errorf("producer error: %w", err)
				}
			}
		}
	}()

	wg.Wait()

	if consumerErr != nil && ctx.Err() == nil {
		return fmt.Errorf("consumer error: %w", consumerErr)
	}
	if producerErr != nil && ctx.Err() == nil {
		return fmt.Errorf("producer error: %w", producerErr)
	}

	return nil
}
