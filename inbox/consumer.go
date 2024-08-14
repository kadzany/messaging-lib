package inbox

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-pg/pg"
	"github.com/google/uuid"
	"github.com/kadzany/messaging-lib/common"
)

type MultiBatchConsumerConfig struct {
	BufferCapacity        int
	MaxBufSize            int
	TickerIntervalSeconds int
	BufChan               chan batchMessages
}

type batchMessages []*ConsumerSessionMessage

type ConsumerSessionMessage struct {
	Message *sarama.ConsumerMessage
	Session sarama.ConsumerGroupSession
}

type Opts func(*Inbox) error

func WithConsumer(consumer sarama.ConsumerGroup) Opts {
	return func(i *Inbox) error {
		i.consumer = consumer
		return nil
	}
}

func WithTopics(topics []string) Opts {
	return func(i *Inbox) error {
		i.topics = topics
		return nil
	}
}

func WithMultiBatchConfig(cfg *MultiBatchConsumerConfig) Opts {
	return func(i *Inbox) error {
		i.multiBatchConfig = cfg
		return nil
	}
}

type Inbox struct {
	inboxManager     *InboxManager
	consumer         sarama.ConsumerGroup
	topics           []string
	wg               sync.WaitGroup
	shutDownChan     chan struct{}
	multiBatchConfig *MultiBatchConsumerConfig
	ready            chan bool
	ticker           *time.Ticker
	msgBuf           batchMessages
	mu               sync.RWMutex
	readyOnce        sync.Once
}

func NewSub(db *pg.DB, opt ...Opts) *Inbox {
	i := &Inbox{
		shutDownChan: make(chan struct{}),
		ready:        make(chan bool),
	}

	i.multiBatchConfig = &MultiBatchConsumerConfig{
		BufferCapacity:        10000,
		MaxBufSize:            8000,
		TickerIntervalSeconds: 10,
		BufChan:               make(chan batchMessages, 100),
	}

	for _, o := range opt {
		o(i)
	}

	i.msgBuf = make([]*ConsumerSessionMessage, 0, i.multiBatchConfig.BufferCapacity)
	i.ticker = time.NewTicker(time.Duration(i.multiBatchConfig.TickerIntervalSeconds) * time.Second)

	i.inboxManager = NewInboxManager(db)
	return i
}

func (i *Inbox) Start(ctx context.Context, handler MessageHandler) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	i.inboxManager.handler = handler

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	i.wg.Add(1)
	go func() {
		defer i.wg.Done()
		for {
			if err = i.consumer.Consume(ctx, i.topics, i); err != nil {
				log.Println("Error from consumer: ", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	i.wg.Add(1)
	go i.processBatches(ctx)

	select {
	case <-ctx.Done():
		log.Println("Context done")
	case <-sigChan:
		log.Println("Signal received")
	}

	return i.Shutdown(ctx)
}

func (i *Inbox) processBatches(ctx context.Context) {
	defer i.wg.Done()
	for {
		select {
		case batch := <-i.multiBatchConfig.BufChan:
			err := i.inboxManager.db.RunInTransaction(func(tx *pg.Tx) error {
				for _, msg := range batch {
					var payload map[string]interface{}
					if err := json.Unmarshal(msg.Message.Value, &payload); err != nil {
						return err
					}
					inboxMsg := &Inboxes{
						ID:        uuid.NewString(),
						Payload:   payload,
						Topic:     msg.Message.Topic,
						Status:    "pending",
						CreatedAt: time.Now(),
					}

					if _, err := tx.Model(inboxMsg).Insert(); err != nil {
						return err
					}

					commonMsg := common.Message{
						Topic:   msg.Message.Topic,
						Key:     string(msg.Message.Key),
						Payload: msg.Message.Value,
					}
					// create request
					if err := i.inboxManager.ProcessMessage(ctx, commonMsg); err != nil {
						inboxMsg.Status = "failed"
					} else {
						inboxMsg.Status = "processed"
					}

					// response from request
					msg.Session.MarkMessage(msg.Message, "")
					inboxMsg.ProcessedAt = time.Now()
					if _, err := tx.Model(inboxMsg).WherePK().Update(); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				log.Println("Error processing batch: ", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (i *Inbox) Shutdown(ctx context.Context) error {
	close(i.shutDownChan)
	i.ticker.Stop()

	shutdownCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		i.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("Graceful shutdown completed")
	case <-shutdownCtx.Done():
		log.Println("Forced shutdown after timeout")
	}

	if err := i.consumer.Close(); err != nil {
		log.Printf("Error closing consumer: %v", err)
		return err
	}

	return nil
}

func (i *Inbox) Setup(sarama.ConsumerGroupSession) error {
	i.readyOnce.Do(func() {
		close(i.ready)
	})
	return nil
}

func (i *Inbox) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (i *Inbox) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			i.insertMessage(&ConsumerSessionMessage{
				Message: message,
				Session: session,
			})
		case <-i.ticker.C:
			i.mu.Lock()
			i.flushBuffer()
			i.mu.Unlock()
		case <-session.Context().Done():
			return nil
		case <-i.shutDownChan:
			return nil
		}
	}
}

func (i *Inbox) insertMessage(msg *ConsumerSessionMessage) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.msgBuf = append(i.msgBuf, msg)
	if len(i.msgBuf) >= i.multiBatchConfig.MaxBufSize {
		i.flushBuffer()
	}
}

func (i *Inbox) flushBuffer() {
	if len(i.msgBuf) > 0 {
		i.multiBatchConfig.BufChan <- i.msgBuf
		i.msgBuf = make([]*ConsumerSessionMessage, 0, i.multiBatchConfig.BufferCapacity)
	}
}
