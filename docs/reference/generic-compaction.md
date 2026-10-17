# Generic compaction safeguards

The `golang/compaction` package contains the provider-independent safety
boundary for generic conversation compaction. It is intentionally separate
from the engine and storage layers so the same checks can be reused by an
explicit `llm.compact.v1` Activity and by an automatic compaction
sub-operation.

`compaction.Policy` is versioned and validated before cache, budget, or
provider work. It requires a lower `target_tokens` than `trigger_tokens`, a
bounded recent-turn window, an output reserve, and explicit prompt/version
identifiers. The package does not claim that these limits are provider context
windows; route-specific capability data remains authoritative.

`compaction.PrepareRequest` copies routing and sampling settings while stripping
application tools, tool choice, continuation state, reasoning, and structured
output. It injects the versioned repository prompt and selected summary style
as policy instructions, forces `ToolChoiceNone` and a text output format, and
does not mutate the original request. The prefix is not replayed as provider
turns. `PrepareRequest` renders it into a single human message: a statement
that the quoted text is data to summarize and not a conversation to continue,
the transcript between `-----BEGIN TRANSCRIPT <id>-----` and
`-----END TRANSCRIPT <id>-----` marker lines, and a closing instruction to
write the summary that states the `output_reserve_tokens` length budget. Each
entry is numbered and labelled (`human message`, `model message`,
`model tool call id=... name=...`, `tool result call_id=... is_error=...`) on a
line of its own, with every content line indented, so transcript text cannot
forge an entry boundary. Before indentation, every other line terminator
(CRLF, CR, vertical tab, form feed, the FS/GS/RS separators U+001C to
U+001E, NEL, U+2028 and U+2029) is normalized to
LF, so no line a model or tokenizer might split on escapes the indentation;
tool arguments and results are serialized as text,
images and documents are named by reference (media type plus URL, blob digest
or byte count) without their bytes, and opaque provider state is left out.
Pointer-form items render as their values. The marker identifier is derived
from the quoted text, so it is stable across retries and transcript content
cannot contain its own closing marker. The summarizer therefore never receives
a trailing assistant turn, tool blocks without tool definitions, or media, on
any endpoint family.

### Settings the summarizer inherits

The summarizer runs on the parent checkpoint's settings, filtered as follows.
A Compact request's optional `model` and `service_class` route the summarizer
instead of the parent's model and service class; a `service_class` override is
exact and drops the parent's fallback classes. Automatic compaction before a
Generate sets them from the Generate's `settings_patch`, so it routes the
summarizer on the model and service class the turn itself is planned on. A
model retired from the catalog after the parent was written therefore does not
strand the lineage once the caller patches a routable one. Neither override
changes the settings the compaction child checkpoint inherits.

| Setting | Summarizer | Reason |
| --- | --- | --- |
| `model`, `service_class`, fallback classes, `portability` | Inherited unless the request overrides `model` or `service_class` | Routing stays the application's choice. |
| Application instructions | Inherited, below the summarizer's policy instructions | They describe the domain being summarized. |
| `temperature`, `top_p` (and the other distribution controls `top_k`, presence and frequency penalties) | Inherited | They shape how tokens are sampled, not where output ends. `top_p` follows `temperature`, which compaction has always inherited, so cache keys for parents that set it are unchanged. |
| `stop_sequences` | Dropped | An application stop sequence could truncate the summary. |
| `seed` | Dropped | A seed chosen to reproduce an application answer has no meaning for a different prompt. |
| `reasoning_mode`, `reasoning_token_budget`, reasoning effort and summary | Dropped | The summarizer is a bounded plain-text call; reasoning would reserve tokens outside `output_reserve_tokens`. |
| Tools, tool policy, web search/fetch, code execution, structured output, output limit, continuation | Dropped or replaced | The call is forced to `ToolChoiceNone` and text output capped at `output_reserve_tokens`. |
| Extensions | Inherited | Output-affecting extensions stay part of the request. |

A sampling spec left with no control after this filtering is omitted, so a
parent that set only `stop_sequences` or `seed` produces the same summarizer
request as one that set neither.

The compaction cache fingerprint hashes the filtered summarizer request.
Parents that set none of `stop_sequences` or `seed` keep their existing cache
keys. Before issue #1245 the summarizer also received the parent's stop
sequences and seed, so a compaction under a parent that set either now misses
any summary cached before that change, and is summarized again once.

`SummaryItem` is the transcript form of an accepted summary: a human-role
message that opens with a fixed heading identifying it as a summary of the
compacted conversation. A compaction checkpoint starts with it, so the next
Generate opens with the user role that Anthropic Messages and Bedrock Converse
require. `PlainTextSummary` accepts only a
completed model message containing text parts, with a caller-provided byte
bound; tool calls, tool results, multimodal content, empty text, and
oversized output are rejected. Provider state (such as the reasoning item a
Responses reasoning model emits before its message) and references (such as
Exa citations) are dropped, so only the text becomes the summary.

The embedded prompt is identified as `v1`. Any prompt change must advance the
prompt version so cache fingerprints cannot reuse a summary generated by a
different prompt. The rendered transcript message is part of the summarizer
request that the cloud cache fingerprint hashes in full, so a change to that
rendering cannot reuse an earlier summary either. Normalizing line terminators
(issue #1456) changed that rendering only for transcripts containing one of
those terminators, so only such compactions miss a summary cached
before the change; the prompt version is unchanged because the embedded
prompt is.

The engine wiring is implemented: the Compact Activity runner is
`golang/storage/durable/compact_runner.go`, parent checkpoints are
materialized by `state.DurableCheckpointMaterializer`, and the compaction
child checkpoint is published as described in
[cloud checkpoint publication](cloud-checkpoint-publication.md).

## Automatic trigger decision

`Policy.EvaluateTrigger` is the provider-independent decision boundary for an
automatic compaction checkpoint. The engine supplies projected counters and
route capability limits in `TriggerInput`; it does not use Temporal payload
size as a trigger. A zero provider limit disables that provider-specific check.
The decision reserves `output_reserve_tokens` and any adapter-owned
`ReservedReasoningTokens` from the provider context window, then evaluates
limits in a stable order: provider context tokens, policy trigger tokens,
provider item count, provider lineage depth, materialized bytes, and expired
continuation. The first crossed limit is recorded as `TriggerReason` for
durable observability.

The policy trigger compares `trigger_tokens` with the whole projected request,
but compaction can only remove the selected prefix. `TriggerInput.RetainedTokens`
is the part of that count a summary leaves in place: instructions, tool
schemas, the retained recent window and the turn being appended, plus the
summary itself, counted at `output_reserve_tokens`. When it
already reaches `trigger_tokens`, the policy trigger is skipped, because the
compacted request would still be over the trigger and every following turn
would buy another summarizer call that re-summarizes the previous summary. The
remaining limits are still evaluated for such a request. A caller whose fixed
prompt and tools exceed the default trigger therefore needs a higher
`trigger_tokens`; the provider context limit still compacts the transcript
when a route's window is reached. Typed OCaml callers set it with
`Compaction_policy.make_settings` and `Settings.make ~compaction_settings` or
`Settings.Patch.set_compaction_settings`.

`target_tokens` must stay below the effective `trigger_tokens`. A Generate's
`settings_patch.compaction_policy` replaces the inherited policy, and its
omitted fields take the defaults (`trigger_tokens` 12000, `target_tokens`
8000), so the rule is decided by the patch alone; the OCaml client refuses a
patch that breaks it when the request is encoded. A Compact request's
`policy.target_tokens` is merged into the parent checkpoint's policy instead,
so its wire bound (1 to 10000000) cannot express the rule: a target at or
above the checkpoint's trigger is wire-valid, and request preparation fails
the compaction with `invalid_argument` before any provider work.

When compaction is required, the decision returns the policy's lower
`target_tokens` hysteresis target. The prefix selector materializes a
checkpoint at or below that target; a checkpoint already below the target does
not retrigger by itself. Deadline and clock interpretation remain engine
responsibilities, so the trigger API accepts the durable
`ContinuationExpired` result rather than reading wall-clock time.

`compaction.SelectPrefix` supplies the deterministic boundary for that
selection. It retains at least `Policy.RecentTurns` logical turns and returns
the earlier complete prefix separately. A turn starts whenever the tool
frontier is empty; a tool call and all matching results remain one atomic turn.
An unresolved final tool frontier is retained in full even when the requested
window is zero, so a summary can never split a tool exchange. Provider-state
items are also retained as whole semantic items. An empty prefix means there is
no safe work to compact yet and must not be passed to `PrepareRequest`.

`compaction.SelectRequestPrefix` applies the policy to a whole request and is
the selection the runtime uses for planning and compaction alike. It retains
`recent_turns` when the request that remains after compaction (instructions,
tools and the retained window) plus the summary, counted at
`output_reserve_tokens`, fits `target_tokens`, and otherwise the longest
shorter window that fits, never fewer than one turn; a window the policy sets
to zero stays empty. The size is the serialized request's UTF-8 bytes divided
by four, the same provider-independent baseline as budget reservation's
fallback estimate, so the boundary does not depend on which route later serves
the summarizer. An oversized turn, such as an inline image or a large tool
result, thus leaves the window instead of being carried into every later
checkpoint and compacted again on each Generate.

The token estimate excludes inline media, so the runtime selects with
`compaction.SelectBoundedRequestPrefix`, which also shortens the window, never
below one turn, until the compacted checkpoint (an empty summary plus the
retained window) is at most half of the extendable-parent bound. The other half
is left for the summary and the next turn's append, so a screenshot or
computer-use loop keeps extending after compaction. Generation planning
compacts when the next turn would pass the extendable-parent byte bound or the
lineage item bound, and projects the compacted parent first (the summary
counted at four bytes per `output_reserve_tokens`): a turn that would not fit
even then fails with an unpaid `invalid_argument` instead of buying a summary
that changes nothing.

The trigger contract is covered by property-style tests: every projected
counter boundary is checked, zero provider limits remain disabled, and raising
tokens, items, lineage, bytes, reserved reasoning, or continuation age cannot
turn an already-required compaction back into a non-compaction decision. These
tests keep the exported decision boundary deterministic.

The compaction request is therefore an isolated internal call, not a replacement
for the caller's Generate settings. A subsequent Generate must materialize the
new checkpoint and compile from the parent settings again, changing only its
operation key, input transcript, and checkpoint continuation. The regression
coverage in `compaction/generic_test.go` asserts this boundary across routing,
fallback classes, portability, instructions, sampling, extensions, tools, and
structured output: compaction sees only its bounded plain-text contract while
the following Generate retains the application's settings exactly.
