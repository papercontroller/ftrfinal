package resilience

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

type RetryClient struct {
	HTTPClient *http.Client
}

func NewRetryClient() *RetryClient {
	return &RetryClient{
		HTTPClient: &http.Client{},
	}
}

func (c *RetryClient) GetWithRetry(ctx context.Context, targetURL string, maxRetries int, initialBackoff time.Duration) ([]byte, error) {
	backoff := initialBackoff

	for attempt := 0; attempt < maxRetries; attempt++ {
		reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, targetURL, nil)
		if err != nil {
			cancel()
			return nil, err
		}

		resp, err := c.HTTPClient.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			defer resp.Body.Close()
			body, readErr := io.ReadAll(resp.Body)
			cancel()
			if readErr == nil {
				return body, nil
			}
		}
		cancel()

		if attempt == maxRetries-1 {
			break
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
		}
	}

	return nil, fmt.Errorf("operation failed after %d retries", maxRetries)
}