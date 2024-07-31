package internal

import (
	"context"
	"log"

	"github.com/IBM/sarama"
	"github.com/go-pg/pg"
	m "github.com/kadzany/messaging-lib/model"
)

func ConsumeMessages(db *pg.DB, brokers []string, topics []string, groupID string) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Version = sarama.V2_1_0_0

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}
	defer consumerGroup.Close()

	consumer := Consumer{
		db: db,
	}

	ctx := context.Background()
	for {
		if err := consumerGroup.Consume(ctx, topics, &consumer); err != nil {
			log.Fatalf("Error consuming messages: %v", err)
		}
	}
}

type Consumer struct {
	db *pg.DB
}

func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		inboxMessage := &m.Inbox{
			MessagePayload: string(message.Value),
			Status:         "WAITING",
			TopicName:      message.Topic,
			Publisher:      string(message.Key),
		}
		_, err := c.db.Model(inboxMessage).Insert()
		if err != nil {
			log.Printf("Error inserting inbox message: %v", err)
		} else {
			sess.MarkMessage(message, "")
		}
	}
	return nil
}
