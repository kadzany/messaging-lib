package internal

import (
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-pg/pg"
	m "github.com/kadzany/messaging-lib/model"
)

func ProduceMessage(brokers []string, topic string, message []byte) error {
	producer, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		return err
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}

	_, _, err = producer.SendMessage(msg)
	return err
}

func ProcessOutboxMessages(db *pg.DB, brokers []string) {
	var outboxMessages []m.Outbox
	err := db.Model(&outboxMessages).
		Where("status = ?", "WAITING").
		Select()
	if err != nil {
		log.Fatalf("Error fetching outbox messages: %v", err)
	}

	for _, outboxMessage := range outboxMessages {
		err = ProduceMessage(brokers, outboxMessage.TopicName, []byte(outboxMessage.MessagePayload))
		if err != nil {
			log.Printf("Error producing Kafka message: %v", err)
			continue
		}

		outboxMessage.Status = "SUCCESS"
		outboxMessage.UpdatedAt = time.Now()
		_, err = db.Model(&outboxMessage).WherePK().Update()
		if err != nil {
			log.Printf("Error updating outbox message: %v", err)
		}
	}
}
