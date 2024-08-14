package message

import (
	"context"
	"fmt"

	httpclient "github.com/kadzany/messaging-lib/http-client"
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

type SendWebhookPayload struct {
	Action      string `json:"action"`
	ModuleType  string `json:"module_type"`
	ProductId   string `json:"product_id"`
	SendingDate string `json:"sending_date"`
	Data        any    `json:"data"`
}

func (m Message) SendWebhook(ctx context.Context, payload SendWebhookPayload, version ApiVersioning) (data []byte, err error) {
	path := fmt.Sprintf("/%s/%s", version.String(), "webhook-requests")
	client := httpclient.NewClient(*m.WebhookUrl)
	data, err = client.Post(ctx, path,
		httpclient.WithAuthorization("Authorization", fmt.Sprintf("apiKey %s", *m.ApiKey)),
		httpclient.WithJSONBody(payload),
	)
	if err != nil {
		return
	}
	return
}
