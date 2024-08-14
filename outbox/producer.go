package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-pg/pg"
	"github.com/google/uuid"
)

type Opts func(*Outbox) error

func WithProducer(producer sarama.SyncProducer) Opts {
	return func(p *Outbox) error {
		p.producer = producer
		return nil
	}
}

func WithWorkerCount(workerCount int) Opts {
	return func(p *Outbox) error {
		p.workerCount = workerCount
		return nil
	}
}

func WithBatchSize(batchSize int) Opts {
	return func(p *Outbox) error {
		p.batchSize = batchSize
		return nil
	}
}

type Outbox struct {
	db           *pg.DB
	producer     sarama.SyncProducer
	workerCount  int
	batchSize    int
	shutdownChan chan struct{}
	wg           sync.WaitGroup
}

func NewPub(db *pg.DB, opt ...Opts) *Outbox {
	p := &Outbox{db: db}
	for _, o := range opt {
		o(p)
	}

	if p.workerCount == 0 {
		p.workerCount = 1
	}

	if p.batchSize == 0 {
		p.batchSize = 100
	}

	p.shutdownChan = make(chan struct{})
	return p
}

func (p *Outbox) Save(topic string, key string, data []byte) error {
	var payload map[string]interface{}
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}

	outbox := &Outboxes{
		ID:        uuid.NewString(),
		Payload:   payload,
		Key:       key,
		Topic:     topic,
		CreatedAt: time.Now(),
		IsDeliver: false,
	}
	_, err := p.db.Model(outbox).Insert()
	return err
}

func (p *Outbox) SaveTx(tx *sql.Tx, topic string, key string, data []byte) error {
	var payload map[string]interface{}
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}

	outbox := &Outboxes{
		ID:        uuid.NewString(),
		Payload:   payload,
		Key:       key,
		Topic:     topic,
		CreatedAt: time.Now(),
		IsDeliver: false,
	}

	stmt, err := tx.Prepare("INSERT INTO outboxes (id, payload, key, topic, created_at, is_deliver) VALUES ($1, $2, $3, $4, $5, $6)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(outbox.ID, outbox.Payload, outbox.Key, outbox.Topic, outbox.CreatedAt, outbox.IsDeliver)
	return err
}

func (p *Outbox) Start(ctx context.Context) {
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}
}

func (p *Outbox) Stop() {
	close(p.shutdownChan)
	p.wg.Wait()
}

func (p *Outbox) Close() {
	p.producer.Close()
}

func (p *Outbox) worker(ctx context.Context) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.shutdownChan:
			return
		default:
			err := p.processBatch()
			if err != nil {
				log.Printf("Error processing batch: %v", err)
			}
			time.Sleep(2 * time.Second)
		}
	}
}

func (p *Outbox) processBatch() error {
	var outboxes []Outboxes
	return p.db.RunInTransaction(func(tx *pg.Tx) (err error) {
		err = tx.Model(&outboxes).
			Where("processed_at IS NULL").
			Order("created_at ASC").
			Limit(p.batchSize).
			For("UPDATE SKIP LOCKED").
			Select()
		if err != nil {
			return err
		}

		if len(outboxes) == 0 {
			return nil
		}

		var messages []*sarama.ProducerMessage
		var ids []string

		for _, outbox := range outboxes {
			payload, err := json.Marshal(outbox.Payload)
			if err != nil {
				return err
			}
			messages = append(messages, &sarama.ProducerMessage{
				Topic: outbox.Topic,
				Key:   sarama.StringEncoder(outbox.Key),
				Value: sarama.ByteEncoder(payload),
			})
			ids = append(ids, outbox.ID)
		}

		err = p.sendMessageInBatches(messages)
		if err != nil {
			return err
		}

		_, err = tx.Model(&Outboxes{}).
			Where("id IN (?)", pg.In(ids)).
			Set("processed_at = ?", time.Now()).
			Set("is_deliver = ?", true).
			Update()
		if err != nil {
			return err
		}

		return nil
	})
}

func (p *Outbox) sendMessageInBatches(messages []*sarama.ProducerMessage) error {
	for i := 0; i < len(messages); i += p.batchSize {
		end := i + p.batchSize
		if end > len(messages) {
			end = len(messages)
		}

		err := p.producer.SendMessages(messages[i:end])
		if err != nil {
			return err
		}
	}
	return nil
}
