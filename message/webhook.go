package message

import (
	"fmt"
	"time"
)

type ApiVersioning int

const (
	V1 ApiVersioning = iota + 1
	V2
	V3
	V4
)

func (a ApiVersioning) String() string {
	return [...]string{"v1", "v2", "v3", "v4"}[a-1]
}

type CreateWebhookEntity struct {
	AppUserId   string
	AppUserCode string
	ModuleType  string
	ProductId   string
	ProductName string
	Method      string
	Url         string
	AuthType    string
	BaUsername  string
	BaPassword  string
	AppName     string
}

func (m Message) CreateWebhook(payload CreateWebhookEntity) (err error) {
	return nil
}

type SendWebhookPayload struct {
	Action      string    `json:"action"`
	ModuleType  string    `json:"module_type"`
	ProductId   string    `json:"product_id"`
	SendingDate time.Time `json:"sending_date"`
	Data        any       `json:"data"`
}

func (m Message) SendWebhook(payload SendWebhookPayload, version ApiVersioning) (err error) {
	fmt.Println("Send webhook")
	fmt.Println("Action: ", payload.Action)
	fmt.Println("url", *m.WebhookUrl)
	fmt.Println(version.String())
	return nil
}
