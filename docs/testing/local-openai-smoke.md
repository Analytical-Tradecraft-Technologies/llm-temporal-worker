# Local OpenAI workflow smoke

Run from the repository root:

```sh
make openai-smoke-check # offline runner tests and Go gate compilation
make openai-smoke       # explicitly authorizes one small paid request
```

Create a local `.env` containing `OPENAI_API_KEY=...`. Never commit this file.
`.gitignore` excludes `.env` and `.env.*`, and the Docker build context excludes
them too. For a separate worktree, pass
`OPENAI_SMOKE_ENV_FILE=/absolute/path/to/your/.env`. The runner reads only the
literal API key assignment; it does not source the file as shell code.

Requires Python 3, Go, and a running Docker daemon with Compose. The runner
starts the existing pinned Temporal and Redis services under a random Compose
project, with dynamic loopback ports. It runs the production workflow and
activities in the Go test process, using the real OpenAI Responses adapter.
Temporal's local fixture uses PostgreSQL; worker persistence does not use SQL.

The smoke submits `llm.generate.workflow.v1` with a tiny fixed prompt through
Temporal, checks a completed answer and checkpoint, then submits an independent
request and requires a cache hit. The generic KV/blob test substrate is in
memory, behind the production encrypted cloud repository. This is not evidence
of DynamoDB/S3, containerized worker startup, asynchronous polling, compaction,
or restore qualification.

The fixed model is `gpt-6-luna`, with low reasoning, standard service, and a
256-output-token cap. The HTTP transport permits **one POST total per run** and
rejects redirects and other destinations. Automatic SDK retries are disabled.
A failed or ambiguous submission requires an explicit rerun, which can incur
another charge. The fixture uses a USD 0.01 budget window. The catalog rates
are USD 0.10/M input tokens and USD 0.50/M output tokens, from the
[model documentation](https://developers.openai.com/api/docs/models/gpt-6-luna)
on 2026-10-04. This tiny standard request is expected to cost well below a cent;
pricing remains provider-controlled.

The key is passed only to the Go subprocess, never as a command-line argument,
Compose environment, Docker build argument, or committed fixture. Provider
response bodies and captured test logs are not printed. Success emits a short
result. Failure emits a generic diagnostic to avoid leaking provider content.
The runner removes only its own Compose project and volumes on exit. It does
not run in CI; `openai-smoke-check` is safe offline.
