package httpclient

import (
	"net/http"

	"github.com/cenkalti/backoff/v4"
	"github.com/sony/gobreaker"
)

type Client struct {
	httpClient *http.Client
	baseUrl    string
	breaker    *gobreaker.CircuitBreaker
	backOff    backoff.BackOff
}
