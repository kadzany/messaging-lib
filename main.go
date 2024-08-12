package main

import (
	"encoding/json"

	message "github.com/kadzany/messaging-lib/message"
)

type User struct {
	Name  string `json:"name"`
	Age   int    `json:"age"`
	Phone string `json:"phone"`
}
type Data struct {
	Test string `json:"test"`
	Data User   `json:"user"`
}

func main() {
	cfg := message.Config(message.Cfg{
		Sasl:    false,
		Topics:  []string{"test"},
		GroupID: "test-inbox",
		Conn: message.DSNConnection{
			Host: "localhost",
			Port: "5432",
			User: "admin",
			Pass: "password",
			Name: "sandbox_pii",
		},
	})

	message, err := message.Open([]string{"localhost:9092"}, cfg)
	if err != nil {
		panic(err)
	}

	data := Data{
		Test: "test",
		Data: User{
			Name:  "sample",
			Age:   20,
			Phone: "08123456789",
		},
	}

	payload, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	if err := message.Outbox.Save("test", "key-test", payload); err != nil {
		panic(err)
	}
}
