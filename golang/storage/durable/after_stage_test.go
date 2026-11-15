package durable

import (
	"context"
	"errors"
	"testing"
)

func TestAfterStageNamesPortErrorsAndEndedContexts(t *testing.T) {
	cause := errors.New("port failed")
	if err := afterStage(context.Background(), "replay", cause); !errors.Is(err, cause) || err.Error() != stageError("replay", cause).Error() {
		t.Fatalf("port error = %v", err)
	}
	if err := afterStage(context.Background(), "replay", nil); err != nil {
		t.Fatalf("live context = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := afterStage(canceled, "replay", nil); err == nil || err.Error() != contextErr(canceled).Error() {
		t.Fatalf("canceled context = %v", err)
	}
}
