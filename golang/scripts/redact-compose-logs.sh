#!/bin/sh
set -eu

: "${LLMTW_LOG_REDACT_REDIS_PASSWORD:?LLMTW_LOG_REDACT_REDIS_PASSWORD is required}"
: "${LLMTW_LOG_REDACT_POSTGRES_PASSWORD:?LLMTW_LOG_REDACT_POSTGRES_PASSWORD is required}"
: "${LLMTW_LOG_REDACT_MOCK_API_KEY:?LLMTW_LOG_REDACT_MOCK_API_KEY is required}"
: "${LLMTW_LOG_REDACT_CONTINUATION_HMAC:?LLMTW_LOG_REDACT_CONTINUATION_HMAC is required}"

# Read the secrets from the environment instead of awk -v arguments so custom
# passwords are matched literally and never exposed in the redactor command line.
exec awk '
BEGIN {
	secrets[1] = ENVIRON["LLMTW_LOG_REDACT_REDIS_PASSWORD"]
	secrets[2] = ENVIRON["LLMTW_LOG_REDACT_POSTGRES_PASSWORD"]
	secrets[3] = ENVIRON["LLMTW_LOG_REDACT_MOCK_API_KEY"]
	secrets[4] = ENVIRON["LLMTW_LOG_REDACT_CONTINUATION_HMAC"]
}
{
	line = $0
	for (i = 1; i <= 4; i++) {
		secret = secrets[i]
		if (!length(secret)) {
			continue
		}
		# Scan only the unscanned remainder so an inserted marker is never
		# revisited, even when the secret itself occurs inside "[REDACTED]".
		redacted = ""
		rest = line
		while ((position = index(rest, secret)) > 0) {
			redacted = redacted substr(rest, 1, position - 1) "[REDACTED]"
			rest = substr(rest, position + length(secret))
		}
		line = redacted rest
	}
	print line
}
'
