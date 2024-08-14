package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sony/gobreaker"
)

type Client struct {
	httpClient *http.Client
	baseUrl    string
	breaker    *gobreaker.CircuitBreaker
	retrier    backoff.BackOff
}

type ClientOption func(*Client)

func WithTimeOut(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.httpClient.Timeout = timeout
	}
}

func WithRetry(maxRetries uint64) ClientOption {
	return func(c *Client) {
		c.retrier = backoff.WithMaxRetries(backoff.NewExponentialBackOff(), maxRetries)
	}
}

func NewClient(baseUrl string, options ...ClientOption) (c *Client) {
	c = &Client{
		baseUrl:    baseUrl,
		httpClient: createHTTPClient(),
		breaker:    createCircuitBreaker(),
		retrier:    backoff.NewExponentialBackOff(),
	}

	for _, option := range options {
		option(c)
	}

	return c
}

func createHTTPClient() *http.Client {
	return &http.Client{
		Transport: createHTTPTransport(),
		Timeout:   30 * time.Second,
	}
}

func createHTTPTransport() *http.Transport {
	return &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   100,
		MaxConnsPerHost:       100,
	}
}

func createCircuitBreaker() *gobreaker.CircuitBreaker {
	return gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:    "HTTP Client Circuit Breaker",
		Timeout: 30 * time.Second,
	})
}

type RequestOption func(*http.Request) error

func WithAuthorization(key, value string) RequestOption {
	return func(req *http.Request) error {
		req.Header.Set(key, value)
		return nil
	}
}

func WithJSONBody(body interface{}) RequestOption {
	return func(r *http.Request) error {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return err
		}

		r.Body = io.NopCloser(bytes.NewBuffer(jsonBody))
		return nil
	}
}

func (c *Client) executeRequest(ctx context.Context, method, url string, options ...RequestOption) (r *http.Response, err error) {
	resp, err := c.breaker.Execute(func() (interface{}, error) {
		req, err := http.NewRequestWithContext(ctx, method, url, nil)
		if err != nil {
			return nil, err
		}

		req.Header.Set("Content-Type", "application/json")
		for _, option := range options {
			if err := option(req); err != nil {
				return nil, err
			}
		}

		return c.httpClient.Do(req)
	})
	if err != nil {
		return nil, err
	}

	return resp.(*http.Response), nil
}

func (c *Client) Request(ctx context.Context, method, path string, options ...RequestOption) (b []byte, err error) {
	url := c.baseUrl + path
	var body []byte

	operation := func() error {
		resp, err := c.executeRequest(ctx, method, url, options...)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 500 {
			return fmt.Errorf("server error: %d", resp.StatusCode)
		}

		body, err = io.ReadAll(resp.Body)
		return err
	}

	if err = backoff.Retry(operation, c.retrier); err != nil {
		return nil, err
	}

	return body, nil
}

func (c *Client) Get(ctx context.Context, path string, options ...RequestOption) (b []byte, err error) {
	return c.Request(ctx, http.MethodGet, path, options...)
}

func (c *Client) Post(ctx context.Context, path string, options ...RequestOption) (b []byte, err error) {
	return c.Request(ctx, http.MethodPost, path, options...)
}
