# AWS Bedrock Converse provider

The `bedrock_converse` endpoint family uses the official AWS SDK for Go and
the Bedrock Runtime `Converse` operation. It is a single request/response
boundary intended for Temporal Activities; this adapter intentionally does not
implement live token streaming or `ConverseStream`.

## Configuration

Configure the AWS default credential chain explicitly. The region selects the
Bedrock Runtime endpoint, while `base_url` and `outbound_hosts` keep egress
allow-listing explicit.

```yaml
bedrock-converse-us-east-1:
  family: bedrock_converse
  base_url: https://bedrock-runtime.us-east-1.amazonaws.com
  outbound_hosts: [bedrock-runtime.us-east-1.amazonaws.com]
  region: us-east-1
  auth:
    kind: aws_default_chain
  timeout: 115s
  service_classes:
    economy: {provider_value: flex}
    standard: {provider_value: default}
    priority: {provider_value: priority}
  capability_profile: bedrock-converse-v1
  price_catalog: catalog-2026-07-13
```

The public service class is always one of `economy`, `standard`, or
`priority`; it is never a provider-default value. The profile maps those
classes to Bedrock's `flex`, `default`, and `priority` service tiers and maps
the response tier back to the public class. Model identifiers remain opaque
strings, so foundation model IDs, inference profiles, provisioned throughput
ARNs, and future identifiers can be configured without changing this library.

## Capability boundary

The generic Converse adapter currently verifies text, client tool calls, and
usage accounting. Image/document content, provider structured-output modes,
provider reasoning blocks, and provider-hosted continuation state are rejected
in strict mode until a profile documents a lossless lowering. Streaming is
always marked unsupported because a Temporal Activity receives one durable
result rather than an open response stream.

Requests are lowered to `ConverseInput` and responses are lifted to the
provider-neutral response model. AWS SigV4 signing and the default credential
chain remain inside the SDK client; credentials never enter the normalized
request or persisted call metadata. SDK retries are disabled at this boundary,
leaving retry ownership with the Temporal/routing layer.

The deterministic unit and contract tests exercise tier mapping, strict
instruction hierarchy rejection, tool JSON documents, and response lifting.
Credentialed live-provider runs are intentionally not part of this change and
must be executed and retained separately as protected release evidence.
