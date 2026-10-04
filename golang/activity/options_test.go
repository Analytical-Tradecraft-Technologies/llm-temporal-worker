package activity

import (
	"testing"
	"time"
)

func validPolicy() ActivityPolicy {
	return ActivityPolicy{StartToClose: 2 * time.Minute, ScheduleToClose: 10 * time.Minute, HeartbeatTimeout: 20 * time.Second, InitialRetry: time.Second, BackoffCoefficient: 2, MaximumRetry: time.Minute, MaximumAttempts: 4, RetryHorizon: 30 * time.Minute, OperationRetention: time.Hour, ProviderTimeout: time.Minute}
}

func TestActivityPolicyValidationAndTemporalOptions(t *testing.T) {
	policy := validPolicy()
	options, err := policy.TemporalOptions()
	if err != nil {
		t.Fatal(err)
	}
	if options.StartToCloseTimeout != policy.StartToClose || options.ScheduleToCloseTimeout != policy.ScheduleToClose || options.RetryPolicy == nil || options.RetryPolicy.MaximumAttempts != policy.MaximumAttempts {
		t.Fatalf("Temporal options = %#v", options)
	}
	for name, mutate := range map[string]func(*ActivityPolicy){
		"schedule before start": func(value *ActivityPolicy) { value.ScheduleToClose = value.StartToClose },
		"heartbeat not shorter": func(value *ActivityPolicy) { value.HeartbeatTimeout = value.StartToClose },
		"retry horizon":         func(value *ActivityPolicy) { value.RetryHorizon = value.OperationRetention + time.Second },
		"provider timeout":      func(value *ActivityPolicy) { value.ProviderTimeout = value.StartToClose },
		"attempt bound":         func(value *ActivityPolicy) { value.MaximumAttempts = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := validPolicy()
			mutate(&invalid)
			if err := invalid.Validate(); err == nil {
				t.Fatal("invalid policy unexpectedly accepted")
			}
		})
	}
}

func TestActivityPolicyHeartbeatKeepaliveInterval(t *testing.T) {
	policy := validPolicy()
	if got, want := policy.KeepaliveInterval(), DefaultHeartbeatKeepaliveInterval; got != want {
		t.Fatalf("default keepalive interval = %s, want %s", got, want)
	}
	policy.HeartbeatKeepaliveInterval = 5 * time.Second
	if err := policy.Validate(); err != nil {
		t.Fatal(err)
	}
	if got := policy.KeepaliveInterval(); got != 5*time.Second {
		t.Fatalf("configured keepalive interval = %s", got)
	}

	for name, mutate := range map[string]func(*ActivityPolicy){
		"negative": func(value *ActivityPolicy) { value.HeartbeatKeepaliveInterval = -time.Second },
		"too slow": func(value *ActivityPolicy) {
			value.HeartbeatKeepaliveInterval = value.HeartbeatTimeout/3 + time.Nanosecond
		},
	} {
		t.Run(name, func(t *testing.T) {
			invalid := validPolicy()
			mutate(&invalid)
			if err := invalid.Validate(); err == nil {
				t.Fatal("invalid keepalive policy unexpectedly accepted")
			}
		})
	}
}

func TestActivityPolicyRetentionCoversEmittedDeadline(t *testing.T) {
	policy := ActivityPolicy{
		StartToClose: 20 * time.Second, ScheduleToClose: time.Hour,
		HeartbeatTimeout: 6 * time.Second, HeartbeatKeepaliveInterval: time.Second,
		InitialRetry: 2 * time.Minute, MaximumRetry: 2 * time.Minute, MaximumAttempts: 4,
		RetryHorizon: 30 * time.Second, OperationRetention: time.Minute,
	}
	if _, err := policy.TemporalOptions(); err == nil {
		t.Fatal("retry schedule outlasts operation retention but was accepted")
	}
	// The scheduled deadline also covers queue delays, including for one attempt.
	for _, attempts := range []int32{1, 4, 100} {
		policy.MaximumAttempts = attempts
		policy.InitialRetry = 0
		policy.MaximumRetry = 0
		policy.RetryHorizon = 0
		policy.OperationRetention = policy.ScheduleToClose - time.Nanosecond
		if _, err := policy.TemporalOptions(); err == nil {
			t.Fatalf("attempts=%d: short retention accepted", attempts)
		}
		policy.OperationRetention = policy.ScheduleToClose
		options, err := policy.TemporalOptions()
		if err != nil {
			t.Fatal(err)
		}
		if options.ScheduleToCloseTimeout > policy.OperationRetention {
			t.Fatal("emitted deadline exceeds retention")
		}
	}
}
