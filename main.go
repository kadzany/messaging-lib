package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	i "github.com/kadzany/messaging-lib/internal"
	m "github.com/kadzany/messaging-lib/model"
	p "github.com/kadzany/messaging-lib/pkg"
)

func main() {
	db := p.ConnectDB()
	defer db.Close()

	err := p.CreateSchema(db)
	if err != nil {
		log.Fatalf("Error creating schema: %v", err)
	}

	// Example of inserting an outbox message
	outboxMessage := &m.Outbox{
		MessagePayload: "Sample outbound message",
		Status:         "WAITING",
		TopicName:      "sample-outbound-topic",
	}
	_, err = db.Model(outboxMessage).Insert()
	if err != nil {
		log.Fatalf("Error inserting outbox message: %v", err)
	}

	// Process outbox messages
	brokers := []string{"localhost:9092"}
	i.ProcessOutboxMessages(db, brokers)

	// Start consuming inbound messages
	i.ConsumeMessages(db, brokers, []string{"sample-inbound-topic"}, "group1")

	// Handle OS signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	log.Println("Shutting down gracefully")
}
