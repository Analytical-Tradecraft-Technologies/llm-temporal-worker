package activity

import (
	"fmt"
	"math"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type ActivityPolicy struct {
	StartToClose     time.Duration
	ScheduleToClose  time.Duration
	HeartbeatTimeout time.Duration
	// HeartbeatKeepaliveInterval must match the worker's configured bounded
	// provider-wait cadence. It is independent of ProviderTimeout.
	HeartbeatKeepaliveInterval time.Duration
	InitialRetry               time.Duration
	BackoffCoefficient         float64
	MaximumRetry               time.Duration
	MaximumAttempts            int32
	// RetryHorizon may declare a longer application retry window. It cannot
	// replace the emitted ScheduleToClose bound when validating retention.
	RetryHorizon       time.Duration
	OperationRetention time.Duration
	ProviderTimeout    time.Duration
}

func (policy ActivityPolicy) Validate() error {
	if policy.StartToClose <= 0 || policy.ScheduleToClose <= 0 {
		return fmt.Errorf("start-to-close and schedule-to-close must be positive")
	}
	if policy.ScheduleToClose <= policy.StartToClose {
		return fmt.Errorf("schedule-to-close must exceed start-to-close")
	}
	if policy.HeartbeatTimeout <= 0 || policy.HeartbeatTimeout >= policy.StartToClose {
		return fmt.Errorf("heartbeat timeout must be positive and shorter than start-to-close")
	}
	if policy.HeartbeatKeepaliveInterval < 0 {
		return fmt.Errorf("heartbeat keepalive interval must not be negative")
	}
	if policy.KeepaliveInterval() > policy.HeartbeatTimeout/3 {
		return fmt.Errorf("heartbeat keepalive interval must be no more than one third of heartbeat timeout")
	}
	if policy.InitialRetry < 0 || policy.MaximumRetry < 0 {
		return fmt.Errorf("retry intervals must not be negative")
	}
	if policy.BackoffCoefficient != 0 && (math.IsNaN(policy.BackoffCoefficient) || math.IsInf(policy.BackoffCoefficient, 0) || policy.BackoffCoefficient < 1) {
		return fmt.Errorf("backoff coefficient must be at least one")
	}
	if policy.MaximumAttempts < 1 || policy.MaximumAttempts > 100 {
		return fmt.Errorf("maximum attempts must be between 1 and 100")
	}
	if policy.OperationRetention <= 0 {
		return fmt.Errorf("operation retention must be positive")
	}
	// ScheduleToClose includes queue delays, all attempts and retry backoff.
	// Comparing durations directly also avoids overflow in retry-sum arithmetic.
	if policy.OperationRetention < policy.ScheduleToClose {
		return fmt.Errorf("operation retention must cover schedule-to-close")
	}
	if policy.RetryHorizon < 0 || policy.RetryHorizon > policy.OperationRetention {
		return fmt.Errorf("retry horizon must not exceed operation retention")
	}
	if policy.ProviderTimeout > 0 && policy.ProviderTimeout >= policy.StartToClose {
		return fmt.Errorf("provider timeout must leave time for Activity finalization")
	}
	return nil
}

// KeepaliveInterval returns the expected provider-wait cadence. Workflow
// callers set HeartbeatKeepaliveInterval to their worker configuration; zero
// uses the documented default.
func (policy ActivityPolicy) KeepaliveInterval() time.Duration {
	if policy.HeartbeatKeepaliveInterval == 0 {
		return DefaultHeartbeatKeepaliveInterval
	}
	return policy.HeartbeatKeepaliveInterval
}

func (policy ActivityPolicy) TemporalOptions() (workflow.ActivityOptions, error) {
	if err := policy.Validate(); err != nil {
		return workflow.ActivityOptions{}, err
	}
	backoff := policy.BackoffCoefficient
	if backoff == 0 {
		backoff = 2
	}
	return workflow.ActivityOptions{
		StartToCloseTimeout:    policy.StartToClose,
		ScheduleToCloseTimeout: policy.ScheduleToClose,
		HeartbeatTimeout:       policy.HeartbeatTimeout,
		WaitForCancellation:    true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        policy.InitialRetry,
			BackoffCoefficient:     backoff,
			MaximumInterval:        policy.MaximumRetry,
			MaximumAttempts:        policy.MaximumAttempts,
			NonRetryableErrorTypes: []string{ErrorTypeInvalidArgument, ErrorTypeAuthentication, ErrorTypeAmbiguous, ErrorTypeOperationConflict, ErrorTypeStateCorrupt},
		},
	}, nil
}
