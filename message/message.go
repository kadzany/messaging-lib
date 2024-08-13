package message

import (
	"bytes"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
	"github.com/kadzany/messaging-lib/inbox"
	"github.com/kadzany/messaging-lib/outbox"
	"github.com/kadzany/messaging-lib/scram"
)

type Config struct {
	Sasl        bool
	WorkerCount int
	BatchSize   int
	GroupID     string
}

type Message struct {
	Outbox    *outbox.Outbox
	Inbox     *inbox.Inbox
	db        *pg.DB
	srmConfig *sarama.Config

	WebhookUrl  *string   `env:"WEBHOOK_BASE_URL" json:"webhook_url"`
	Host        *string   `env:"PG_HOST" json:"db_host"`
	Port        *string   `env:"PG_PORT" json:"db_port"`
	User        *string   `env:"PG_USER" json:"db_user"`
	Pass        *string   `env:"PG_PASSWORD" json:"db_pass"`
	Name        *string   `env:"PG_DATABASE" json:"db_name"`
	Topics      []*string `env:"KAFKA_TOPICS" json:"kafka_topics"`
	KfkUsername *string   `env:"KAFKA_USERNAME" json:"kafka_username"`
	KfkPassword *string   `env:"KAFKA_PASSWORD" json:"kafka_password"`

	sasl    bool
	brokers []string
	groupId string

	producerOpts []outbox.Opts
}

func Open(brokers []string, config *Config) (*Message, error) {
	m := &Message{
		brokers:   brokers,
		groupId:   config.GroupID,
		srmConfig: sarama.NewConfig(),
		sasl:      config.Sasl,
	}

	m.producerOpts = append(m.producerOpts, outbox.WithWorkerCount(config.WorkerCount))
	m.producerOpts = append(m.producerOpts, outbox.WithBatchSize(config.BatchSize))

	if err := m.initEnv(); err != nil {
		return nil, err
	}

	if err := m.initDB(); err != nil {
		return nil, err
	}

	if err := m.initKafka(); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *Message) initEnv() error {
	return EnvLoad(m)
}

func (m *Message) initDB() error {
	var addr bytes.Buffer
	addr.WriteString(*m.Host)
	addr.WriteString(":")
	addr.WriteString(*m.Port)
	m.db = pg.Connect(&pg.Options{
		Addr:     addr.String(),
		User:     *m.User,
		Password: *m.Pass,
		Database: *m.Name,
	})

	models := []interface{}{
		(*inbox.Inboxes)(nil),
		(*outbox.Outboxes)(nil),
	}

	for _, model := range models {
		_, err := m.db.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`)
		if err != nil {
			log.Fatalf("Error creating uuid-ossp extension: %v", err)
		}

		err = m.db.Model(model).CreateTable(&orm.CreateTableOptions{
			IfNotExists:   true,
			FKConstraints: true,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Message) initKafka() error {
	if m.sasl {
		m.srmConfig.Net.SASL.Enable = true
		m.srmConfig.Net.SASL.User = *m.KfkUsername
		m.srmConfig.Net.SASL.Password = *m.KfkPassword
		m.srmConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		m.srmConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &scram.XDGSCRAMClient{HashGeneratorFcn: scram.SHA512}
		}
		m.srmConfig.Net.SASL.Handshake = true
		m.srmConfig.Net.TLS.Enable = true
	}
	m.srmConfig.Producer.Return.Successes = true
	m.srmConfig.Producer.Return.Errors = true
	m.srmConfig.Consumer.Return.Errors = true
	m.srmConfig.Net.DialTimeout = 10 * time.Second
	m.srmConfig.Consumer.Offsets.AutoCommit.Enable = false
	m.srmConfig.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	m.srmConfig.Consumer.Offsets.Initial = sarama.OffsetNewest

	producer, err := sarama.NewSyncProducer(m.brokers, m.srmConfig)
	if err != nil {
		return err
	}
	m.Outbox = outbox.NewPub(m.db, append(m.producerOpts, outbox.WithProducer(producer))...)

	consumer, err := sarama.NewConsumerGroup(m.brokers, m.groupId, m.srmConfig)
	if err != nil {
		return err
	}
	m.Inbox = inbox.NewSub(m.db, inbox.WithConsumer(consumer), inbox.WithTopics(converPointerToStrings(m.Topics)))
	return nil
}

func converPointerToStrings(values []*string) []string {
	result := make([]string, len(values))
	for i, v := range values {
		if v != nil {
			result[i] = *v
		}
	}
	return result
}
