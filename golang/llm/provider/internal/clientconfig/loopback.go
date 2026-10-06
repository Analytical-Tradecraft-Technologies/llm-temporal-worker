package clientconfig

import "net/http"

// LoopbackBearerClient returns a copy of the guarded client that sends the
// API key itself. The OpenAI SDK allows credentials over plain HTTP only
// through its own direct loopback transport, which would bypass the guarded
// client's egress policy, response-size limit and pre-dispatch evidence. A
// loopback development endpoint therefore gets no SDK credential and this
// client adds the bearer header on the way out.
func LoopbackBearerClient(client *http.Client, key string) *http.Client {
	next := *client
	inner := client.Transport
	if inner == nil {
		inner = http.DefaultTransport
	}
	next.Transport = bearerTransport{inner: inner, key: key}
	return &next
}

type bearerTransport struct {
	inner http.RoundTripper
	key   string
}

func (transport bearerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.Header.Set("Authorization", "Bearer "+transport.key)
	return transport.inner.RoundTrip(clone)
}
