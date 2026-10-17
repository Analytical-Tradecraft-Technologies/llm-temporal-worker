package responsesapi

import (
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// identityComponentMaxCodePoints matches the v1 request context bounds.
const identityComponentMaxCodePoints = 256

// identityFromRequest reads the caller identity from the OpenAI API key. The
// key is not a secret: it is the literal "tenant/project/actor" triple, split
// on the first two slashes so the actor may itself contain slashes. It selects
// the request context and the storage scope, and is never an authorization
// decision on its own; the worker's authorization policy still applies.
func identityFromRequest(request *http.Request) (llm.RequestContext, bool) {
	header := request.Header.Get("Authorization")
	scheme, key, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return llm.RequestContext{}, false
	}
	return parseAPIKey(strings.TrimSpace(key))
}

func parseAPIKey(key string) (llm.RequestContext, bool) {
	parts := strings.SplitN(key, "/", 3)
	if len(parts) != 3 {
		return llm.RequestContext{}, false
	}
	for _, part := range parts {
		if !validIdentityComponent(part) {
			return llm.RequestContext{}, false
		}
	}
	return llm.RequestContext{Tenant: parts[0], Project: parts[1], Actor: parts[2]}, true
}

func validIdentityComponent(value string) bool {
	if value == "" || !utf8.ValidString(value) || utf8.RuneCountInString(value) > identityComponentMaxCodePoints {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}
