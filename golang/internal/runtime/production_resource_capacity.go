package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	redisstore "github.com/mfow/llm-temporal-worker/golang/storage/redis"
)

const resourceCapacityLeaseDomain = "llmtw/resource-capacity-lease/v1\x00"

func (binding *productionPhaseBinding) acquireResourceCapacity(ctx context.Context, request llm.ResourceCapacityAcquireRequestV1) (llm.ResourceCapacityLeaseV1, error) {
	if err := request.Validate(); err != nil {
		return llm.ResourceCapacityLeaseV1{}, err
	}
	capacity := binding.cap.ResourceCapacity
	if capacity.GenerationID == "" || binding.cap.CapacityLeases == nil {
		return llm.ResourceCapacityLeaseV1{}, errors.New("verified resource capacity is unavailable")
	}
	if request.GenerationID != capacity.GenerationID || request.ManifestSHA256 != capacity.ManifestSHA256 {
		return llm.ResourceCapacityLeaseV1{}, errors.New("resource capacity request does not match the verified manifest")
	}
	limit, queueBound, err := resourceClassLimit(capacity.Limits, request.ResourceClass)
	if err != nil {
		return llm.ResourceCapacityLeaseV1{}, err
	}
	now := binding.now()
	request, err = boundResourceCapacityRequest(request, now, queueBound)
	leaseID := resourceCapacityLeaseID(request)
	if errors.Is(err, context.DeadlineExceeded) {
		existing, lookupErr := binding.cap.CapacityLeases.Lookup(ctx, leaseID)
		if errors.Is(lookupErr, redisstore.ErrThrottleNotFound) {
			return llm.ResourceCapacityLeaseV1{}, context.DeadlineExceeded
		}
		if lookupErr != nil {
			return llm.ResourceCapacityLeaseV1{}, lookupErr
		}
		return binding.renewRecoveredResourceCapacityLease(ctx, request, existing)
	}
	if err != nil {
		return llm.ResourceCapacityLeaseV1{}, err
	}
	deadline := request.QueueDeadline
	acquired, err := binding.cap.CapacityLeases.AcquireFair(ctx, leaseID, capacity.GenerationID+":"+request.ResourceClass, deadline.Sub(now), 25*time.Millisecond, []redisstore.ThrottleLimit{{Kind: redisstore.ThrottleConcurrency, Scope: capacity.GenerationID + ":" + request.ResourceClass, Amount: 1, Limit: int64(limit), Window: binding.cap.ReservationLease}})
	if err != nil {
		return llm.ResourceCapacityLeaseV1{}, fmt.Errorf("acquire %s capacity: %w", request.ResourceClass, err)
	}
	if acquired.Reservation.ID != leaseID {
		return llm.ResourceCapacityLeaseV1{}, errors.New("resource capacity lease identity mismatch")
	}
	if acquired.Existing {
		return binding.renewRecoveredResourceCapacityLease(ctx, request, acquired.Reservation)
	}
	return binding.resourceCapacityLeaseResponse(request), nil
}

func (binding *productionPhaseBinding) renewRecoveredResourceCapacityLease(ctx context.Context, request llm.ResourceCapacityAcquireRequestV1, reservation redisstore.ThrottleReservation) (llm.ResourceCapacityLeaseV1, error) {
	if reservation.ID != resourceCapacityLeaseID(request) {
		return llm.ResourceCapacityLeaseV1{}, errors.New("resource capacity lease lookup identity mismatch")
	}
	if err := binding.cap.CapacityLeases.Renew(ctx, reservation, binding.cap.ReservationLease); err != nil {
		return llm.ResourceCapacityLeaseV1{}, err
	}
	return binding.resourceCapacityLeaseResponse(request), nil
}

func (binding *productionPhaseBinding) resourceCapacityLeaseResponse(request llm.ResourceCapacityAcquireRequestV1) llm.ResourceCapacityLeaseV1 {
	acquiredAt := binding.now()
	return llm.ResourceCapacityLeaseV1{APIVersion: llm.ResourceCapacityAcquireAPIVersion, Context: request.Context, ResourceClass: request.ResourceClass, LeaseKey: request.LeaseKey, LeaseID: resourceCapacityLeaseID(request), GenerationID: request.GenerationID, ManifestSHA256: request.ManifestSHA256, QueueDeadline: request.QueueDeadline, AcquiredAt: acquiredAt, ExpiresAt: acquiredAt.Add(binding.cap.ReservationLease)}
}
func (binding *productionPhaseBinding) renewResourceCapacity(ctx context.Context, request llm.ResourceCapacityRenewRequestV1) (llm.ResourceCapacityLeaseV1, error) {
	if err := request.Validate(); err != nil {
		return llm.ResourceCapacityLeaseV1{}, err
	}
	if err := binding.validateResourceCapacityLease(request.Lease); err != nil {
		return llm.ResourceCapacityLeaseV1{}, err
	}
	now := binding.now()
	if !request.Lease.ExpiresAt.After(now) {
		return llm.ResourceCapacityLeaseV1{}, errors.New("resource capacity lease expired before renewal")
	}
	reservation, err := binding.cap.CapacityLeases.Lookup(ctx, request.Lease.LeaseID)
	if err != nil {
		return llm.ResourceCapacityLeaseV1{}, err
	}
	if reservation.ID != request.Lease.LeaseID {
		return llm.ResourceCapacityLeaseV1{}, errors.New("resource capacity lease lookup identity mismatch")
	}
	if err := binding.cap.CapacityLeases.Renew(ctx, reservation, binding.cap.ReservationLease); err != nil {
		return llm.ResourceCapacityLeaseV1{}, err
	}
	response := request.Lease
	response.ExpiresAt = now.Add(binding.cap.ReservationLease)
	return response, nil
}

func (binding *productionPhaseBinding) releaseResourceCapacity(ctx context.Context, request llm.ResourceCapacityReleaseRequestV1) (llm.ResourceCapacityReleaseResponseV1, error) {
	if err := request.Validate(); err != nil {
		return llm.ResourceCapacityReleaseResponseV1{}, err
	}
	if err := binding.validateResourceCapacityLease(request.Lease); err != nil {
		return llm.ResourceCapacityReleaseResponseV1{}, err
	}
	releaseCtx := context.Background()
	if ctx != nil {
		releaseCtx = context.WithoutCancel(ctx)
	}
	reservation, err := binding.cap.CapacityLeases.Lookup(releaseCtx, request.Lease.LeaseID)
	if errors.Is(err, redisstore.ErrThrottleNotFound) {
		return llm.ResourceCapacityReleaseResponseV1{APIVersion: llm.ResourceCapacityReleaseAPIVersion, LeaseID: request.Lease.LeaseID, Released: true}, nil
	}
	if err != nil {
		return llm.ResourceCapacityReleaseResponseV1{}, err
	}
	if reservation.ID != request.Lease.LeaseID {
		return llm.ResourceCapacityReleaseResponseV1{}, errors.New("resource capacity lease lookup identity mismatch")
	}
	if err := binding.cap.CapacityLeases.Release(releaseCtx, reservation); err != nil {
		return llm.ResourceCapacityReleaseResponseV1{}, err
	}
	return llm.ResourceCapacityReleaseResponseV1{APIVersion: llm.ResourceCapacityReleaseAPIVersion, LeaseID: request.Lease.LeaseID, Released: true}, nil
}

func (binding *productionPhaseBinding) validateResourceCapacityLease(lease llm.ResourceCapacityLeaseV1) error {
	if err := lease.Validate(); err != nil {
		return err
	}
	capacity := binding.cap.ResourceCapacity
	if binding.cap.CapacityLeases == nil || lease.GenerationID != capacity.GenerationID || lease.ManifestSHA256 != capacity.ManifestSHA256 {
		return errors.New("resource capacity lease does not match the verified manifest")
	}
	request := llm.ResourceCapacityAcquireRequestV1{APIVersion: llm.ResourceCapacityAcquireAPIVersion, Context: lease.Context, ResourceClass: lease.ResourceClass, LeaseKey: lease.LeaseKey, GenerationID: lease.GenerationID, ManifestSHA256: lease.ManifestSHA256, QueueDeadline: lease.QueueDeadline}
	if resourceCapacityLeaseID(request) != lease.LeaseID {
		return errors.New("resource capacity lease identity is invalid")
	}
	_, _, err := resourceClassLimit(capacity.Limits, lease.ResourceClass)
	return err
}

func boundResourceCapacityRequest(request llm.ResourceCapacityAcquireRequestV1, now time.Time, queueBound time.Duration) (llm.ResourceCapacityAcquireRequestV1, error) {
	request.QueueDeadline = request.QueueDeadline.UTC()
	if request.QueueDeadline.After(now.Add(queueBound)) {
		return request, errors.New("resource capacity queue_deadline exceeds the signed admission wait")
	}
	if !request.QueueDeadline.After(now) {
		return request, context.DeadlineExceeded
	}
	return request, nil
}

func resourceClassLimit(limits config.ResourceCapacityLimits, resourceClass string) (int, time.Duration, error) {
	switch resourceClass {
	case llm.ResourceClassPythonStage2:
		return limits.PythonStage2MaxInflight, time.Duration(limits.StageTwoSemaphoreWaitSeconds) * time.Second, nil
	case llm.ResourceClassForecastEvent:
		return limits.ForecastEventMaxInflight, time.Duration(limits.ForecastEventAdmissionWaitSeconds) * time.Second, nil
	default:
		return 0, 0, errors.New("resource capacity class is unsupported")
	}
}

func resourceCapacityLeaseID(request llm.ResourceCapacityAcquireRequestV1) string {
	h := sha256.New()
	for _, value := range []string{resourceCapacityLeaseDomain, request.GenerationID, request.ManifestSHA256, request.ResourceClass, request.Context.Tenant, request.Context.Project, request.Context.Actor, request.LeaseKey, request.QueueDeadline.UTC().Format(time.RFC3339Nano)} {
		_, _ = h.Write([]byte(value))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
