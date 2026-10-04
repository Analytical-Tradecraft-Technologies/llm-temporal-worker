package clientconfig

import (
	"net/http"
	"time"
)

// MessagesRequestTimeout carries the endpoint's HTTP timeout into the Messages
// SDK's non-streaming preflight check. Without an explicit SDK timeout, that
// check rejects large output caps before the bounded HTTP client can run.
// Standalone clients retain the SDK's ten-minute default. Caller deadlines and
// HTTP-client timeouts still apply independently.
func MessagesRequestTimeout(client *http.Client) time.Duration {
	if client.Timeout > 0 {
		return client.Timeout
	}
	return 10 * time.Minute
}
