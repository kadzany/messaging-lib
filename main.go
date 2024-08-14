package main

import (
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

type Provisioning struct {
	Citem          string                 `json:"citem"`
	Customer       map[string]interface{} `json:"customer"`
	ServiceId      string                 `json:"service_id"`
	OrderNumber    string                 `json:"order_number"`
	ServiceName    string                 `json:"service_name"`
	SourceOrder    string                 `json:"source_order"`
	ActivationId   string                 `json:"activation_id"`
	SerialNumbers  []string               `json:"serial_numbers"`
	ActivationData string                 `json:"activation_data"`
}

func main() {
	_, err := message.Open([]string{"localhost:9092"}, &message.Config{
		Sasl:        false,
		WorkerCount: 1,
		BatchSize:   1,
	})
	if err != nil {
		panic(err)
	}

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
