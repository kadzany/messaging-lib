package pkg

import (
	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
	m "github.com/kadzany/messaging-lib/model"
)

func ConnectDB() *pg.DB {
	db := pg.Connect(&pg.Options{
		Addr:     "localhost:5432",
		User:     "user",
		Password: "password",
		Database: "dbname",
	})
	return db
}

func CreateSchema(db *pg.DB) error {
	models := []interface{}{
		(*m.Inbox)(nil),
		(*m.Outbox)(nil),
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
