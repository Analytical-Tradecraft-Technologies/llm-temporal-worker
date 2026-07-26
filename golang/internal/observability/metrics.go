package observability

import (
	"context"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

var safeLabelPattern = regexp.MustCompile(`^[A-Za-z0-9_.:/-]{1,96}$`)

type metricsContextKey struct{}

// WithMetrics binds the process metrics implementation to one request path.
// A nil metrics value is deliberately retained as a no-op binding.
func WithMetrics(ctx context.Context, metrics *Metrics) context.Context {
	return context.WithValue(ctx, metricsContextKey{}, metrics)
}

// MetricsFromContext returns the request-bound metrics implementation, if any.
func MetricsFromContext(ctx context.Context) *Metrics {
	metrics, _ := ctx.Value(metricsContextKey{}).(*Metrics)
	return metrics
}

// AllowedValues contains configured identifiers that are safe to expose as
// metric labels. Tenant IDs are deliberately absent: tenant labels are never
// accepted by this package.
type AllowedValues struct {
	Endpoints             []string
	Models                []string
	Policies              []string
	Windows               []string
	ErrorClasses          []string
	Phases                []string
	Statuses              []string
	Outcomes              []string
	Methods               []string
	OperationStates       []string
	ContinuationDecisions []string
}

type Metrics struct {
	registry *prometheus.Registry
	allowed  labelAllowList
	mu       sync.RWMutex

	activityTotal        *prometheus.CounterVec
	activityFailureTotal *prometheus.CounterVec
	activityDuration     *prometheus.HistogramVec
	providerAttemptTotal *prometheus.CounterVec
	providerDuration     *prometheus.HistogramVec
	serviceClassActual   *prometheus.CounterVec
	budgetAdmission      *prometheus.CounterVec
	budgetReserved       *prometheus.GaugeVec
	costTotal            *prometheus.CounterVec
	costExactTotal       *prometheus.CounterVec
	operationState       *prometheus.CounterVec
	ambiguousTotal       *prometheus.CounterVec
	continuationTotal    *prometheus.CounterVec
	configReloadTotal    *prometheus.CounterVec
	maintenanceRows      *prometheus.CounterVec
	maintenanceFailures  *prometheus.CounterVec
	maintenanceDuration  *prometheus.HistogramVec
	postgresPool         *prometheus.GaugeVec
	postgresLatency      *prometheus.HistogramVec
	postgresTableTuples  *prometheus.GaugeVec
	cacheEvents          *prometheus.CounterVec
	pendingPolls         *prometheus.CounterVec
	costStatus           *prometheus.CounterVec
	workerPolling        prometheus.Gauge
	heartbeatAge         prometheus.Gauge
}

type labelAllowList struct {
	endpoints, models, policies, windows, errors, phases, statuses,
	outcomes, methods, operationStates, continuationDecisions map[string]struct{}
}

var activityFailureOrigins = map[string]struct{}{
	"worker":   {},
	"provider": {},
	"caller":   {},
	"budget":   {},
}

func values(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		if safeLabelPattern.MatchString(value) {
			result[value] = struct{}{}
		}
	}
	return result
}

func (allowed AllowedValues) lists() labelAllowList {
	return labelAllowList{
		endpoints: values(allowed.Endpoints), models: values(allowed.Models),
		policies: values(allowed.Policies), windows: values(allowed.Windows),
		errors: values(allowed.ErrorClasses), phases: values(allowed.Phases),
		statuses: values(allowed.Statuses), outcomes: values(allowed.Outcomes),
		methods: values(allowed.Methods), operationStates: values(allowed.OperationStates),
		continuationDecisions: values(allowed.ContinuationDecisions),
	}
}

func NewMetrics(allowed AllowedValues) (*Metrics, error) {
	m := &Metrics{registry: prometheus.NewRegistry(), allowed: allowed.lists()}
	m.activityTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmtw_activity_total", Help: "Completed and failed Temporal activity calls."}, []string{"status", "error_class"})
	m.activityFailureTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmtw_activity_failure_total", Help: "Failed Temporal activity attempts by bounded error origin."}, []string{"origin"})
	m.activityDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "llmtw_activity_duration_seconds", Help: "Activity duration by lifecycle phase."}, []string{"phase"})
	m.providerAttemptTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmtw_provider_attempt_total", Help: "Provider attempts by configured route."}, []string{"endpoint", "model", "class", "outcome"})
	m.providerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "llmtw_provider_duration_seconds", Help: "Provider attempt duration by configured route."}, []string{"endpoint", "model", "class"})
	m.serviceClassActual = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmtw_service_class_actual_total", Help: "Requested and actual public service classes."}, []string{"requested", "actual", "endpoint"})
	m.budgetAdmission = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmtw_budget_admission_total", Help: "Budget admission outcomes."}, []string{"policy", "outcome"})
	m.budgetReserved = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmtw_budget_reserved_micro_usd", Help: "Currently reserved budget in microUSD."}, []string{"policy", "window"})
	m.costTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmtw_cost_micro_usd_total", Help: "Accounted cost in integer microUSD."}, []string{"endpoint", "model", "class", "method"})
	m.costExactTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmtw_cost_usd_total", Help: "Count of accounted exact-USD cost events; amount remains in the durable ledger."}, []string{"endpoint", "model", "class", "method"})
	m.operationState = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmtw_operation_state_total", Help: "Operation state transitions."}, []string{"state"})
	m.ambiguousTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmtw_ambiguous_total", Help: "Operations whose provider dispatch is unresolved."}, []string{"endpoint"})
	m.continuationTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmtw_continuation_total", Help: "Continuation decisions."}, []string{"decision"})
	m.configReloadTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmtw_config_reload_total", Help: "Configuration reload outcomes."}, []string{"outcome"})
	m.maintenanceRows = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmtw_maintenance_rows_total", Help: "Bounded maintenance rows by resource and outcome."}, []string{"resource", "outcome"})
	m.maintenanceFailures = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmtw_maintenance_failures_total", Help: "Bounded maintenance pass failures by resource."}, []string{"resource"})
	m.maintenanceDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "llmtw_maintenance_duration_seconds", Help: "Duration of bounded maintenance passes by resource."}, []string{"resource"})
	m.postgresPool = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmtw_postgres_pool_connections", Help: "PostgreSQL pool connections by bounded state."}, []string{"state"})
	m.postgresLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "llmtw_postgres_latency_seconds", Help: "PostgreSQL pool, lock, query, and maintenance boundary latency."}, []string{"kind"})
	m.postgresTableTuples = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmtw_postgres_table_tuples", Help: "Approximate PostgreSQL table tuples by bounded resource and liveness state."}, []string{"resource", "state"})
	m.cacheEvents = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmtw_cache_events_total", Help: "Response cache hits, uses, fills, misses, and bounded failures."}, []string{"event"})
	m.pendingPolls = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmtw_provider_poll_total", Help: "Provider-owned operation poll outcomes."}, []string{"outcome"})
	m.costStatus = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmtw_cost_status_total", Help: "Exact and unknown cost accounting events; amounts remain in the durable ledger."}, []string{"endpoint", "model", "class", "status", "method"})
	m.workerPolling = prometheus.NewGauge(prometheus.GaugeOpts{Name: "llmtw_worker_polling", Help: "Whether the Temporal worker is polling."})
	m.heartbeatAge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "llmtw_heartbeat_age_seconds", Help: "Age of the most recent Activity heartbeat."})
	collectors := []prometheus.Collector{
		m.activityTotal, m.activityFailureTotal, m.activityDuration, m.providerAttemptTotal, m.providerDuration,
		m.serviceClassActual, m.budgetAdmission, m.budgetReserved, m.costTotal, m.costExactTotal,
		m.operationState, m.ambiguousTotal, m.continuationTotal, m.configReloadTotal,
		m.maintenanceRows, m.maintenanceFailures, m.maintenanceDuration, m.postgresPool, m.postgresLatency,
		m.postgresTableTuples, m.cacheEvents, m.pendingPolls, m.costStatus,
		m.workerPolling, m.heartbeatAge,
	}
	for _, collector := range collectors {
		if err := m.registry.Register(collector); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (metrics *Metrics) Handler() http.Handler {
	if metrics == nil || metrics.registry == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(metrics.registry, promhttp.HandlerOpts{EnableOpenMetrics: true})
}

func (metrics *Metrics) Gather() ([]*dto.MetricFamily, error) {
	if metrics == nil || metrics.registry == nil {
		return nil, nil
	}
	return metrics.registry.Gather()
}

func (metrics *Metrics) allow(value string, allowed map[string]struct{}) string {
	if !safeLabelPattern.MatchString(value) {
		return "other"
	}
	if len(allowed) == 0 {
		return "other"
	}
	if _, ok := allowed[value]; !ok {
		return "other"
	}
	return value
}

func (metrics *Metrics) builtIn(value string, allowed map[string]struct{}) string {
	if !safeLabelPattern.MatchString(value) {
		return "other"
	}
	if len(allowed) == 0 {
		return value
	}
	if _, ok := allowed[value]; !ok {
		return "other"
	}
	return value
}

func (metrics *Metrics) RecordActivity(status, errorClass string, duration time.Duration, phase string) {
	if metrics == nil {
		return
	}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	metrics.activityTotal.WithLabelValues(metrics.builtIn(status, metrics.allowed.statuses), metrics.builtIn(errorClass, metrics.allowed.errors)).Inc()
	metrics.activityDuration.WithLabelValues(metrics.builtIn(phase, metrics.allowed.phases)).Observe(duration.Seconds())
}

// RecordActivityFailure records a terminal failed Activity attempt. Origin is
// intentionally restricted to the fixed vocabulary.
func (metrics *Metrics) RecordActivityFailure(origin string) {
	if metrics == nil {
		return
	}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	metrics.activityFailureTotal.WithLabelValues(metrics.builtIn(origin, activityFailureOrigins)).Inc()
}

func (metrics *Metrics) RecordProviderAttempt(endpoint, model, class, outcome string, duration time.Duration) {
	if metrics == nil {
		return
	}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	metrics.providerAttemptTotal.WithLabelValues(metrics.allow(endpoint, metrics.allowed.endpoints), metrics.allow(model, metrics.allowed.models), metrics.builtIn(class, map[string]struct{}{"economy": {}, "standard": {}, "priority": {}}), metrics.builtIn(outcome, metrics.allowed.outcomes)).Inc()
	metrics.providerDuration.WithLabelValues(metrics.allow(endpoint, metrics.allowed.endpoints), metrics.allow(model, metrics.allowed.models), metrics.builtIn(class, map[string]struct{}{"economy": {}, "standard": {}, "priority": {}})).Observe(duration.Seconds())
}

func (metrics *Metrics) RecordServiceClass(requested, actual, endpoint string) {
	if metrics == nil {
		return
	}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	classes := map[string]struct{}{"economy": {}, "standard": {}, "priority": {}}
	metrics.serviceClassActual.WithLabelValues(metrics.builtIn(requested, classes), metrics.builtIn(actual, classes), metrics.allow(endpoint, metrics.allowed.endpoints)).Inc()
}

func (metrics *Metrics) RecordBudgetAdmission(policy, outcome string) {
	if metrics == nil {
		return
	}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	metrics.budgetAdmission.WithLabelValues(metrics.allow(policy, metrics.allowed.policies), metrics.builtIn(outcome, metrics.allowed.outcomes)).Inc()
}

func (metrics *Metrics) SetBudgetReserved(policy, window string, microUSD float64) {
	if metrics == nil {
		return
	}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	metrics.budgetReserved.WithLabelValues(metrics.allow(policy, metrics.allowed.policies), metrics.allow(window, metrics.allowed.windows)).Set(microUSD)
}

func (metrics *Metrics) RecordCost(endpoint, model, class, method string, microUSD float64) {
	if metrics == nil {
		return
	}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	classes := map[string]struct{}{"economy": {}, "standard": {}, "priority": {}}
	metrics.costTotal.WithLabelValues(metrics.allow(endpoint, metrics.allowed.endpoints), metrics.allow(model, metrics.allowed.models), metrics.builtIn(class, classes), metrics.allow(method, metrics.allowed.methods)).Add(microUSD)
}

// RecordExactCost records an exact-USD accounting event without converting
// the amount to float. The exact decimal remains in the response/ledger;
// Prometheus only receives a bounded event count.
func (metrics *Metrics) RecordExactCost(endpoint, model, class, method string) {
	if metrics == nil {
		return
	}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	classes := map[string]struct{}{"economy": {}, "standard": {}, "priority": {}}
	metrics.costExactTotal.WithLabelValues(metrics.allow(endpoint, metrics.allowed.endpoints), metrics.allow(model, metrics.allowed.models), metrics.builtIn(class, classes), metrics.allow(method, metrics.allowed.methods)).Inc()
}

// RecordCostStatus records whether a completed operation has an exact or
// unknown cost. The amount and unknown reason remain in the durable ledger;
// this counter deliberately exposes only bounded dimensions.
func (metrics *Metrics) RecordCostStatus(endpoint, model, class, status, method string) {
	if metrics == nil {
		return
	}
	classes := map[string]struct{}{"economy": {}, "standard": {}, "priority": {}}
	statuses := map[string]struct{}{"exact": {}, "unknown": {}}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	metrics.costStatus.WithLabelValues(
		metrics.allow(endpoint, metrics.allowed.endpoints),
		metrics.allow(model, metrics.allowed.models),
		metrics.builtIn(class, classes),
		metrics.builtIn(status, statuses),
		metrics.allow(method, metrics.allowed.methods),
	).Inc()
}

func (metrics *Metrics) RecordOperationState(state string) {
	if metrics == nil {
		return
	}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	metrics.operationState.WithLabelValues(metrics.allow(state, metrics.allowed.operationStates)).Inc()
}

func (metrics *Metrics) RecordAmbiguous(endpoint string) {
	if metrics == nil {
		return
	}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	metrics.ambiguousTotal.WithLabelValues(metrics.allow(endpoint, metrics.allowed.endpoints)).Inc()
}

func (metrics *Metrics) RecordContinuation(decision string) {
	if metrics == nil {
		return
	}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	metrics.continuationTotal.WithLabelValues(metrics.allow(decision, metrics.allowed.continuationDecisions)).Inc()
}

func (metrics *Metrics) RecordConfigReload(outcome string) {
	if metrics == nil {
		return
	}
	metrics.configReloadTotal.WithLabelValues(metrics.builtIn(outcome, metrics.allowed.outcomes)).Inc()
}

// RecordMaintenance records bounded row progress.  The resource and outcome
// vocabularies are deliberately fixed so table names and operator input never
// become unbounded Prometheus labels.
func (metrics *Metrics) RecordMaintenance(resource, outcome string, rows int, duration time.Duration) {
	if metrics == nil {
		return
	}
	if rows < 0 {
		rows = 0
	}
	resources := map[string]struct{}{
		"cache": {}, "status": {}, "inventory": {}, "query_execution": {},
		"checkpoint": {}, "blob": {}, "outbox": {}, "budget": {},
	}
	outcomes := map[string]struct{}{
		"eligible": {}, "tombstoned": {}, "deleted": {}, "skipped": {},
	}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	metrics.maintenanceRows.WithLabelValues(metrics.builtIn(resource, resources), metrics.builtIn(outcome, outcomes)).Add(float64(rows))
	// A maintenance pass reports one row counter for each outcome. Observe its
	// duration only on the single eligible event so one pass contributes one
	// histogram sample rather than multiplying latency by outcome count.
	if outcome == "eligible" {
		metrics.maintenanceDuration.WithLabelValues(metrics.builtIn(resource, resources)).Observe(nonNegativeDuration(duration).Seconds())
	}
}

// RecordMaintenanceFailure records a failed bounded pass without exposing
// database error text or caller-controlled table names.
func (metrics *Metrics) RecordMaintenanceFailure(resource string) {
	if metrics == nil {
		return
	}
	resources := map[string]struct{}{
		"cache": {}, "status": {}, "inventory": {}, "query_execution": {},
		"checkpoint": {}, "blob": {}, "outbox": {}, "budget": {},
	}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	metrics.maintenanceFailures.WithLabelValues(metrics.builtIn(resource, resources)).Inc()
}

// RecordPostgresPool records only numeric pool state; it never accepts a
// database, namespace, or tenant as a label.
func (metrics *Metrics) RecordPostgresPool(total, acquired, idle, max int32) {
	if metrics == nil {
		return
	}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	metrics.postgresPool.WithLabelValues("total").Set(float64(maxInt32(total)))
	metrics.postgresPool.WithLabelValues("acquired").Set(float64(maxInt32(acquired)))
	metrics.postgresPool.WithLabelValues("idle").Set(float64(maxInt32(idle)))
	metrics.postgresPool.WithLabelValues("max").Set(float64(maxInt32(max)))
}

// RecordPostgresLatency records a bounded database boundary. Callers should
// use one of pool, lock, query, or maintenance for kind.
func (metrics *Metrics) RecordPostgresLatency(kind string, duration time.Duration) {
	if metrics == nil {
		return
	}
	kinds := map[string]struct{}{"pool": {}, "lock": {}, "query": {}, "maintenance": {}}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	metrics.postgresLatency.WithLabelValues(metrics.builtIn(kind, kinds)).Observe(nonNegativeDuration(duration).Seconds())
}

// RecordPostgresTableTuples records approximate pg_stat_user_tables values.
// Resource names are fixed logical names, never physical relation names.
func (metrics *Metrics) RecordPostgresTableTuples(resource string, live, dead int64) {
	if metrics == nil {
		return
	}
	resources := map[string]struct{}{
		"cache": {}, "status": {}, "inventory": {}, "operation": {},
		"budget": {}, "checkpoint": {}, "query_execution": {}, "outbox": {}, "blob": {},
	}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	metrics.postgresTableTuples.WithLabelValues(metrics.builtIn(resource, resources), "live").Set(float64(maxInt64(live)))
	metrics.postgresTableTuples.WithLabelValues(metrics.builtIn(resource, resources), "dead").Set(float64(maxInt64(dead)))
}

// RecordCache records one bounded response-cache lifecycle event. Use and hit
// are deliberately separate: a replayed operation can be a hit without
// inserting a second use row.
func (metrics *Metrics) RecordCache(event string) {
	if metrics == nil {
		return
	}
	events := map[string]struct{}{
		"hit": {}, "use": {}, "miss": {}, "fill": {}, "fill_existing": {},
		"fill_busy": {}, "fill_failed": {}, "error": {},
	}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	metrics.cacheEvents.WithLabelValues(metrics.builtIn(event, events)).Inc()
}

// RecordPendingPoll records a provider-owned poll boundary. The worker may
// retry a pending operation many times, so this is a counter rather than an
// in-memory gauge that could be lost on restart.
func (metrics *Metrics) RecordPendingPoll(outcome string) {
	if metrics == nil {
		return
	}
	outcomes := map[string]struct{}{"started": {}, "completed": {}, "retry": {}, "failed": {}}
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	metrics.pendingPolls.WithLabelValues(metrics.builtIn(outcome, outcomes)).Inc()
}

func nonNegativeDuration(duration time.Duration) time.Duration {
	if duration < 0 {
		return 0
	}
	return duration
}

func maxInt32(value int32) int32 {
	if value < 0 {
		return 0
	}
	return value
}

func maxInt64(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func (metrics *Metrics) SetWorkerPolling(polling bool) {
	if metrics == nil {
		return
	}
	if polling {
		metrics.workerPolling.Set(1)
	} else {
		metrics.workerPolling.Set(0)
	}
}

func (metrics *Metrics) SetHeartbeatAge(age time.Duration) {
	if metrics == nil {
		return
	}
	if age < 0 {
		age = 0
	}
	metrics.heartbeatAge.Set(age.Seconds())
}
