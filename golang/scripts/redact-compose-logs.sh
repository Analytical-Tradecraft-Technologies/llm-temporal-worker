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
	# The marker must not itself contain any configured secret, or the
	# redacted output would still expose it. Fall back to deleting the
	# secret when every candidate marker contains one.
	candidates[1] = "[REDACTED]"
	candidates[2] = "<secret removed>"
	candidates[3] = "***"
	candidates[4] = "###"
	marker = ""
	for (c = 1; c <= 4 && marker == ""; c++) {
		safe = 1
		for (i = 1; i <= 4; i++) {
			if (length(secrets[i]) && index(candidates[c], secrets[i])) {
				safe = 0
			}
		}
		if (safe) {
			marker = candidates[c]
		}
	}
}
{
	line = $0
	for (i = 1; i <= 4; i++) {
		secret = secrets[i]
		if (!length(secret)) {
			continue
		}
		# Scan only the unscanned remainder so an inserted marker is never
		# revisited.
		redacted = ""
		rest = line
		while ((position = index(rest, secret)) > 0) {
			redacted = redacted substr(rest, 1, position - 1) marker
			rest = substr(rest, position + length(secret))
		}
		line = redacted rest
	}
	print line
}
'
