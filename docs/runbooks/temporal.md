# Temporal

Runbooks for stuck Activities, task-queue backlog and a Temporal namespace
outage. Temporal owns durable retries: every Activity step replays from cloud
request state, so a retried step never dispatches a provider call twice.
Activities have a ten-minute start-to-close limit, a 30-second heartbeat
timeout and a 24-hour retry horizon; see
[internal execution workflows](../reference/internal-workflows.md) and the
[Temporal worker](../architecture/temporal-worker.md). Start from
[on-call escalation and severity](on-call.md) for the rules that apply to
every incident.

Commands below that start with `temporal` are the Temporal CLI, not this
worker's binary.

## Stuck Activities

- **Signal:** `llmtw_heartbeat_age_seconds` stays high. Each Activity heartbeat
  sets it to the age of the most recent progress event that heartbeat reports,
  so a rising value means Activities are still heartbeating but making no
  progress, for example a provider call that has not returned. It is not the
  Activity's elapsed time, and it does not move when no Activity heartbeats:
  an idle or wedged worker leaves its last value. Pair it with
  `llmtw_execution_step_duration_seconds{step}` and
  `llmtw_provider_duration_seconds{endpoint,model,class}`, and with
  Temporal's own Activity heartbeat timeouts.
- **Readiness reason:** none.
- **Default severity:** SEV3; SEV2 when many Activities on one endpoint stall
  together.
- **Safe action:**
  1. Find the step and endpoint. A stall in `generate` or `compact` with high
     provider duration on one endpoint is a slow or hung provider: follow
     [provider failures](provider-and-routing.md#provider-failures).
  2. In Temporal, list the request's pending Activities
     (`temporal workflow describe --workflow-id <id>`) and their attempt
     counts and last heartbeat.
  3. Let the timeouts work. A call that never returns is cut off at the
     endpoint's `timeout` or the ten-minute start-to-close limit, and the step
     retries from durable state. An attempt that may have reached the provider
     becomes `outcome_unknown` rather than being resubmitted; see
     [stuck ambiguous attempt](stuck-ambiguous-attempt.md).
  4. Restart a pod only when it is wedged (heartbeats stopped, liveness still
     `200`). Its in-flight provider attempts may become ambiguous and be
     charged at their full reservation, so prefer waiting for the timeouts.
- **Forbidden:** completing or failing an Activity by hand from Temporal
  (`temporal activity complete` or `temporal activity fail`), and terminating
  or resetting the request workflow. Each skips the worker's durable state
  transitions and can leave a paid attempt with no owner.

## Task queue backlog

- **Signal:** Temporal's task-queue backlog metrics for `temporal.task_queue`
  grow (the Temporal server reports them, not the worker). The Temporal CLI's
  `temporal task-queue describe --task-queue <temporal.task_queue>` shows the
  pollers and backlog.
- **Readiness reason:** none directly. Check whether replicas have stopped
  polling: `llmtw_worker_polling` at `0` with `/health/ready` `503` means a
  dependency check or the Temporal SDK paused that replica; follow
  [state stores](state-stores.md) or
  [Temporal namespace outage](#temporal-namespace-outage).
- **Default severity:** SEV2 when latency objectives are at risk; SEV3 for a
  short burst.
- **Safe action:**
  1. Count polling replicas: `sum(llmtw_worker_polling)` against the
     deployment's replica count.
  2. If every replica polls and is busy, scale out the Deployment. Per-pod
     concurrency (`temporal.worker.max_concurrent_activities`) is a
     process-lifetime setting that needs a rollout.
  3. If replicas run different configurations, requests prepared under one
     configuration wait for a worker with a compatible one; finish the
     rollout instead of leaving mixed replicas.
  4. Budget waits do not hold worker slots; a backlog of waiting requests is a
     [budget denial](budget-and-cost.md#budget-denials) issue, not a capacity
     one.
- **Forbidden:** changing `temporal.task_queue` or `temporal.namespace` to
  drain the backlog elsewhere, or terminating queued workflows. Both strand
  in-flight requests.

## Temporal namespace outage

- **Signal:** Temporal is unreachable or the namespace is unavailable or
  missing. Running replicas keep retrying their polls. If the SDK stops a
  worker on a non-retriable error, such as `NamespaceNotFound` past its grace
  period, that replica sets `/health/ready` to `503` and `llmtw_worker_polling`
  to `0`, and the next monitor check starts a fresh poller. New pods fail at
  startup, because the Temporal client dials eagerly, and Kubernetes restarts
  them with back-off. Temporal server metrics and the task-queue backlog show
  the same outage from the other side.
- **Readiness reason:** none of the dependency checks fail; readiness is `503`
  only on replicas whose Temporal worker has stopped polling.
- **Default severity:** SEV1 when no replica can poll.
- **What keeps money safe:** an Activity that is mid-call when Temporal goes
  away keeps running within its drain window and saves its provider result to
  cloud request storage before reporting it. When Temporal returns, the step
  is retried and replays the saved result; it does not call the provider
  again.
- **Safe action:**
  1. Confirm the outage with the Temporal platform owner and the namespace's
     status (`temporal operator namespace describe --namespace <namespace>`).
  2. Do not restart healthy pods during the outage: they recover on their own,
     and a restart cannot start until Temporal accepts the dial.
  3. After recovery, confirm `llmtw_worker_polling` returns to `1` on every
     replica and the backlog drains. Requests resume from durable state.
  4. If the namespace was deleted and recreated, its workflow histories are
     gone. Running requests cannot resume; escalate to the service owner with
     the time window, and treat their paid attempts as described in
     [stuck ambiguous attempt](stuck-ambiguous-attempt.md#finding-a-stuck-attempt).
- **Forbidden:** pointing workers at another namespace to "fail over".
  `temporal.namespace` is part of every caller's durable scope: a new
  namespace invalidates every checkpoint handle, makes every cached response
  a miss, and needs a restart. See
  [durable scope identity](../reference/configuration.md#durable-scope-identity).
