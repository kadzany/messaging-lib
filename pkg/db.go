package pkg

import (
	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
	"github.com/kadzany/messaging-lib/inbox"
	"github.com/kadzany/messaging-lib/outbox"
)

// deprecated function to connect to database
func ConnectDB() *pg.DB {
	db := pg.Connect(&pg.Options{
		Addr:     "localhost:5432",
		User:     "user",
		Password: "password",
		Database: "dbname",
	})
	return db
}

// deprecated function to create schema in database
func CreateSchema(db *pg.DB) error {
	models := []interface{}{
		(*inbox.Inboxes)(nil),
		(*outbox.Outboxes)(nil),
	}
	for _, model := range models {
		err := db.Model(model).CreateTable(&orm.CreateTableOptions{
			IfNotExists: true,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
