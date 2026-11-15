# Workflow versioning and version-marker retirement

This page records every Temporal `GetVersion` change ID the worker uses, says
when to guard a workflow change with `GetVersion` and when Worker Versioning
with `Pinned` behaviour is enough, and gives the procedure for retiring a
marker. `TestWorkflowVersioningPageListsEveryChangeID` in
`golang/internal/documentationtest` (run by `make docs-verify`) fails when a
`GetVersion` call in non-test Go code is missing from the inventory below, when
its supported range differs from the one listed, when a listed ID is no longer
used, or when a retired ID is used again.

## Why the markers exist

All six workflows (see [the v1 Activity runtime boundary](activity-runtime.md))
are registered on one task queue. By default the worker polls unversioned
(`temporal.worker.versioning.enabled: false`; see
[optional Temporal worker versioning](configuration.md#optional-temporal-worker-versioning)),
so after a rolling deploy any worker build can pick up any open run and replays
it from its history. A workflow code change that would emit a different command
sequence for an existing history is a non-determinism error. The worker sets
`WorkflowPanicPolicy: FailWorkflow` (`golang/internal/app/worker.go`, #1324), so
such an error fails the run rather than blocking it.

`workflow.GetVersion(ctx, id, workflow.DefaultVersion, 1)` keeps old histories
on the old path. On first execution it records a `Version` marker with
version 1 and takes the new path. On replay of a history recorded before the
call existed it finds no marker, returns `DefaultVersion`, and takes the old
path. Each marker also upserts the `TemporalChangeVersion` search attribute.
The pinned Go SDK skips that upsert and logs a warning once the serialized
value would reach 2048 bytes, so marker count cannot fail a run: later markers
are only missing from visibility.

## Current change IDs

Rows are in the order the change IDs were added. **Versions** is the supported
`min..max` range passed to `GetVersion`. A trailing `<n>` marks a change-ID
family whose suffix is a per-run sequence number (see
[per-check change-ID families](#per-check-change-id-families)). **Workflow
types** are the types whose runs can evaluate the call. **Covered by** names
the tests that replay or simulate each path; a test that exercises the
`DefaultVersion` path is marked *(old)*, and must be deleted or rewritten when
that branch is retired.

| Change ID | Versions | Code | Added | Workflow types | New path (version 1) | Old path (`DefaultVersion`) | Covered by |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `langfuse-export-v1` | `DefaultVersion..1` | `ExecuteRequest` deferred export, `golang/workflows/internal.go` | #1232 | `llm.request.execute.v1` | Exports the operation trace when the run ends without Continue-As-New | No export | `TestLangfuseOldHistorySkipsExport` *(old)*, `TestRecordedHistoriesReplay` |
| `public-generation-binding-v1` | `DefaultVersion..1` | `Generate`, `golang/workflows/public.go` | #1292 | `llm.generate.workflow.v1` | Keeps the planning and compaction bindings (`OriginalGenerate`) | Child input without the original parent | `TestPublicGenerateBindingVersionMigration` *(old and new)*, `TestRecordedHistoriesReplay` |
| `reencoded-payload-limit-v1` | `DefaultVersion..1` | `reencoded`, `golang/workflows/public.go` | #1406 | `llm.generate.workflow.v1`, `llm.compact.workflow.v1`, `llm.query.workflow.v1` | Rejects a request whose worker re-encoding exceeds the inline limit | No re-encoding check | `TestGenerateRejectsUnescapedHTMLThatReencodesPastTheLimit`, `TestRecordedHistoriesReplay`; no test replays the old path |
| `reference-capability-v1-<n>` | `DefaultVersion..1` | `ExecuteRequest` `authorize`, `golang/workflows/internal.go` | #1436 (#1362) | `llm.request.execute.v1` | Prepares again to obtain the reference capability before a reference step | Reference step without a capability | `TestExecuteRequestObtainsCapabilityForLegacyPreparation`, `TestLegacyCapabilityHistoryReplays` *(old)*, `TestLegacyBudgetChildReturnsToParent` *(old)*, `TestCapabilityChangeIDsAreBoundedPerRun` |
| `budget-reference-capability-v1-<n>` | `DefaultVersion..1` | `WaitForBudget`, `golang/workflows/internal.go` | #1436 (#1362) | `llm.budget.wait.v1` | A child started without a capability hands the request back to its parent | Acquires without a capability | `TestLegacyBudgetChildReturnsToParent`; no test replays the old path |
| `langfuse-export-async-v1` | `DefaultVersion..1` | `ExecuteRequest` deferred export, `golang/workflows/internal.go` | #1438 (#1351) | `llm.request.execute.v1` | Starts the export as an abandoned `llm.langfuse.export.v1` child | Runs the export Activity inline | `TestLangfuseExportRunsInAbandonedChild`, `TestAsyncExportHistoryCompletesBeforeChild`, `TestLangfuseInlineExportHistoryKeepsActivity` *(old)*, `TestInlineExportHistoryReplays` *(old)* |
| `langfuse-export-retry-v1` | `DefaultVersion..1` | `ExportLangfuse`, `golang/workflows/internal.go` | #1471 (#1459) | `llm.langfuse.export.v1` | Export Activity options that retry a saturated in-flight limit | The request workflow's bounded export options | `TestLangfuseExportWorkflowRetriesSaturation` *(old and new)* |
| `resolved-generation-v1` | `DefaultVersion..1` | `Generate`, `golang/workflows/public.go` | #1781 | `llm.generate.workflow.v1` (evaluated only when `public-generation-binding-v1` is version 1) | Plans with the resolve Activity; a complete request skips the request workflow | Plan Activity and request child | `TestPublicGenerateResolvedSkipsRequestWorkflow`, `TestPublicGenerateBeforeResolutionKeepsRequestWorkflow` *(old)*, `TestRecordedHistoriesReplay` *(old and new)* |
| `pinned-inline-payload-limit-v1` | `DefaultVersion..1` | `pinPayloadLimits`, `golang/workflows/internal.go` | #1806 (#1511) | All six workflows | Records the inline payload limit at start and bounds the run's payloads by it | Uses the worker's current limit | `TestPinnedLimitHistoriesReplayUnderSmallerLimit`, `TestRecordedHistoriesReplay` *(old)* |

## Recorded replay histories

The checked-in histories under `golang/workflows/testdata/replay` were recorded
on a real Temporal server and pin these paths:

| History | Workflow type | Markers recorded | Old paths it exercises |
| --- | --- | --- | --- |
| `before-resolution-generate-hit`, `before-resolution-generate-miss` | `llm.generate.workflow.v1` | `reencoded-payload-limit-v1`, `public-generation-binding-v1` | `resolved-generation-v1`, `pinned-inline-payload-limit-v1` |
| `before-resolution-request-hit`, `before-resolution-request-miss` | `llm.request.execute.v1` | `langfuse-export-v1`, `langfuse-export-async-v1` | `pinned-inline-payload-limit-v1` |
| `resolved-generate-hit` | `llm.generate.workflow.v1` | `reencoded-payload-limit-v1`, `public-generation-binding-v1`, `resolved-generation-v1` | `pinned-inline-payload-limit-v1` |
| `pinned-limit-generate`, `pinned-limit-generate-complete` | `llm.generate.workflow.v1` | `pinned-inline-payload-limit-v1`, `reencoded-payload-limit-v1`, `public-generation-binding-v1`, `resolved-generation-v1` | none |
| `pinned-limit-request` | `llm.request.execute.v1` | `pinned-inline-payload-limit-v1` | none (the run is still open at its Prepare Activity) |

`TestRecordedHistoriesReplay` replays the first five and
`TestPinnedLimitHistoriesReplayUnderSmallerLimit` replays the pinned-limit
histories under smaller limits. The request-capability and inline-export
histories are built in code by `legacyHistory` in
`golang/workflows/legacy_capability_test.go`.

## Per-check change-ID families

`reference-capability-v1-<n>` and `budget-reference-capability-v1-<n>`
(#1362) append a sequence number to a constant prefix, so each check is its own
change ID. Histories recorded before the capability existed ran reference steps
without one, and only the first check past the end of such a history may take
the new path; a single change ID would return its first recorded value for
every later check. They are kept rather than replaced: collapsing them into one
ID would change how those recorded histories replay.

The number of markers they record is bounded:

- `ExecuteRequest` calls `GetVersion` for a check only while the run has no
  capability. The first check that runs past the old history prepares again
  and normally obtains one, so the run records one marker. If the
  re-preparation returns none (an Activity worker that predates the
  capability during a rolling deploy), the next reference step checks again.
  Each loop step makes at most one check and the run continues as new after
  `maxStepsPerRun` (128) steps, so a run that continues as new records at most
  128 of these markers, and a run that finishes sooner at most one per step
  plus one for the export. The continued run starts again at suffix 1.
  `TestCapabilityChangeIDsAreBoundedPerRun` pins this worst case.
- `WaitForBudget` returns to its parent at the first check that takes the new
  path, so a budget run records at most one.

New runs prepared by current workers carry a capability from their first
Preparation and evaluate neither family. Do not add another per-iteration
change ID: guard a new change once per run, before the loop, and carry the
decision in workflow state.

## GetVersion or Pinned Worker Versioning

Guard every workflow change that alters the command sequence for an existing
history with `GetVersion`, whether or not the deployment uses Worker
Versioning. The code must stay correct for the default unversioned deployment,
and even a versioned deployment replays a history on a build that did not
record it when:

- the run started unversioned, before versioning was enabled (enabling it does
  not migrate those executions);
- an operator moves a run with a versioning override
  (`temporal workflow update-options`), for example during a rollback;
- the deployment turns versioning off again; or
- a run is reset, or replayed by a built-in query, on a later build.

With `temporal.worker.versioning.enabled: true` the worker uses `Pinned` as the
default behaviour for every workflow it registers. A pinned run stays on the
build that started it. Temporal keeps the pinned version across the
Continue-As-New chain and starts a child on its parent's version when the
child's task queue belongs to that version, which holds here because all six
workflows share one task queue. Pinned versioning is therefore the tool for
rolling out, ramping and rolling back a build without replaying open runs on
new code, and it shortens marker retirement: once Temporal reports every build
older than a marker drained, no pinned run can still need its `DefaultVersion`
branch. It is not a substitute for the guard in shared code.

Choose by the change:

| Change | Use |
| --- | --- |
| Activity implementation, configuration, or anything outside workflow code | Neither; an ordinary deploy |
| Workflow code whose commands are unchanged for every existing history | Neither, but replay the checked-in histories (`make verify`) to confirm |
| Workflow code that emits different commands for an existing history | A new `GetVersion` change ID, plus a replay history recorded before the change |
| A risky workflow build you want to ramp, observe and be able to roll back | Pinned Worker Versioning, in addition to the `GetVersion` guard |

### Promote, ramp and roll back a pinned build

These steps use Temporal CLI `worker deployment` commands. With the Temporal
Worker Controller, the controller performs the equivalent routing changes
from the `WorkerDeployment` rollout settings; use the commands to inspect state
or in an emergency. `llm-temporal-worker` stands for the configured
`deployment_name`, and `OLD` and `NEW` for build IDs.

1. Start workers with the new `build_id` alongside the current build and check
   that they poll:
   `temporal worker deployment describe-version --deployment-name llm-temporal-worker --build-id NEW`.
2. Ramp a share of new runs to it:
   `temporal worker deployment set-ramping-version --deployment-name llm-temporal-worker --build-id NEW --percentage 10`.
   Watch the workflow failure and Activity error metrics, then raise the
   percentage.
3. Promote it:
   `temporal worker deployment set-current-version --deployment-name llm-temporal-worker --build-id NEW`.
   New runs start on `NEW`; runs pinned to `OLD` finish on `OLD`.
4. Keep `OLD` workers polling until
   `temporal worker deployment describe-version --deployment-name llm-temporal-worker --build-id OLD`
   reports it drained (Temporal refreshes drainage status periodically), then
   stop them within the graceful shutdown deadline and optionally run
   `temporal worker deployment delete-version --deployment-name llm-temporal-worker --build-id OLD`.
   `temporal worker deployment describe --name llm-temporal-worker` shows the
   current and ramping versions.
5. To roll back, run `set-current-version` with `--build-id OLD` (or
   `set-ramping-version ... --delete` to stop a ramp). This works only while
   `OLD` workers still poll, which is why step 4 waits. New runs start on
   `OLD`; runs already pinned to `NEW` stay there. Move such a run with
   `temporal workflow update-options --workflow-id ID --versioning-override-behavior pinned --versioning-override-deployment-name llm-temporal-worker --versioning-override-build-id OLD`
   only after replaying its history against the `OLD` build (see
   [capture a replay history](#capture-a-replay-history)); a run that has
   taken a path `OLD` does not have fails there with a non-determinism
   error. Otherwise fix forward with a guarded change in a newer build and
   move the runs to that build.

## Retiring a marker

A marker is retired in two stages. The Go SDK's guidance applies: keep the
`GetVersion` call after its old branch is gone, so a run that should not exist
fails at the call instead of diverging later.

### Retention horizon

The `DefaultVersion` branch of a change ID may be deleted only after all of
these hold for every workflow type in its row:

1. **Rollout complete.** Every worker build older than the change has stopped
   polling. Record that time as `T`. Under pinned versioning, `T` is when
   `describe-version` reports every older build drained.
2. **No open run predates `T`.** Continue-As-New starts a new run that
   evaluates the call afresh, so only current runs count. This query must
   return zero for each type:
   `temporal workflow count --query 'WorkflowType="llm.request.execute.v1" AND ExecutionStatus="Running" AND StartTime < "<T as RFC 3339>"'`.
3. **Closed runs aged out.** One namespace retention period
   (`temporal operator namespace describe`) has passed since condition 2
   first held, so no closed run from before `T` can still be reset or replayed
   by a built-in query on the new code.

For the per-check families, condition 2 counts only request and budget runs
whose Preparation recorded no capability; any run started before `T` for
#1436 is a candidate.

### Order of removal

1. **Check the horizon** for the change ID, using the release that added it
   (the **Added** column). Retire change IDs oldest first. A nested call such
   as `resolved-generation-v1` (evaluated only on the new path of
   `public-generation-binding-v1`) or `langfuse-export-async-v1` (inside
   `langfuse-export-v1`) was added later, so its horizon is never earlier than
   the outer one's.
2. **Stage 1, in one pull request:**
   - change the call to `GetVersion(ctx, id, 1, 1)` and delete the
     `DefaultVersion` branch;
   - delete or rewrite the tests marked *(old)* for that row, and remove from
     `golang/workflows/testdata/replay` every history listed above as
     exercising its old path (those histories now fail by design);
   - keep every history that records the marker and confirm it still replays;
   - change the row's **Versions** to `1..1` (the documentation test checks
     it) and its old-path cell to "Retired in #...".
3. **Release stage 1** and leave it in service. A run that reaches the call
   without a marker now fails with an unsupported-version error, which is the
   intended tripwire.
4. **Stage 2, optional:** remove the call itself only when the code it guards
   is being deleted or rewritten. The SDK skips `Version` markers when it
   checks determinism, so histories that recorded the marker still replay as
   long as the remaining commands match. Move the row to
   [retired change IDs](#retired-change-ids) and never reuse the ID; a later
   change to the same code takes a new ID such as `<name>-v2`.

## Adding a change ID

1. Name it `<what-changes>-v1` and declare it as a package constant or a
   string literal; the documentation test resolves only constant change IDs
   (with an optional runtime suffix).
2. Call `GetVersion(ctx, id, workflow.DefaultVersion, 1)` once per run where
   possible, before any loop.
3. Check in a history recorded by the previous release that reaches the
   changed code, and replay it in `golang/workflows/replay_history_test.go`;
   add a test of the new path.
4. Add a row to [current change IDs](#current-change-ids) and run
   `make docs-verify`.

### Capture a replay history

Record histories on a local Temporal server driven by fixture requests, never
from production: a history contains the request payloads. Export one with
`temporal workflow show --workflow-id ID --run-id RUN --output json > golang/workflows/testdata/replay/<name>.json`
and load it with `worker.NewWorkflowReplayer` as `TestRecordedHistoriesReplay`
does.

## Retired change IDs

No change ID has been retired. When one is, add a row here
(`` | `id` | retired in #... | reason | ``) so the documentation test rejects
its reuse.

| Change ID | Retired | Reason |
| --- | --- | --- |
