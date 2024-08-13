package main

import (
	"time"

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
	m, err := message.Open([]string{"localhost:9092"}, &message.Config{
		Sasl:        false,
		WorkerCount: 1,
		BatchSize:   1,
	})
	if err != nil {
		panic(err)
	}

	m.SendWebhook(message.SendWebhookPayload{
		Action:      "test",
		ModuleType:  "test",
		ProductId:   "test",
		SendingDate: time.Now(),
		Data:        nil,
	}, message.V4)

	// data := Data{
	// 	Test: "test",
	// 	Data: User{
	// 		Name:  "sample",
	// 		Age:   20,
	// 		Phone: "08123456789",
	// 	},
	// }

	// payload, err := json.Marshal(data)
	// if err != nil {
	// 	panic(err)
	// }

	// if err := message.Outbox.Save("test", "key-test", payload); err != nil {
	// 	panic(err)
	// }
}
