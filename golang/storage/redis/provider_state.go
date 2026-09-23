package redis

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/control"
	redisclient "github.com/redis/go-redis/v9"
)

const providerStateSchema = "provider-state/v1"
const providerViewTTL = 15 * time.Minute
const maxProviderRecordBytes = 4 << 20

// ProviderStateOptions shares the worker's Redis client and opaque namespace.
// Clock is used for query-view retention; observation freshness comes from the
// provider's ObservedAt/ExpiresAt fields, not from Redis key expiration.
type ProviderStateOptions struct {
	Client redisclient.UniversalClient
	Keys   KeyOptions
	Clock  func() time.Time
}

// ProviderStateStore holds operational projections, not a paid-request journal.
// Latest records have no TTL, so expiry cannot forgive a credit/billing incident
// or let an old inventory observation replace a newer one. Query views expire.
type ProviderStateStore struct {
	client       redisclient.UniversalClient
	space        keySpace
	clock        func() time.Time
	cursorCipher cipher.AEAD
}

var _ control.ProviderStatusReader = (*ProviderStateStore)(nil)
var _ control.InventoryStore = (*ProviderStateStore)(nil)

func NewProviderStateStore(options ProviderStateOptions) (*ProviderStateStore, error) {
	if options.Client == nil {
		return nil, errors.New("Redis provider state client is required")
	}
	space, err := newKeySpace(options.Keys)
	if err != nil {
		return nil, err
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	// Domain-separate cursor encryption from key-name HMACs. Changing the shared
	// key secret also changes the namespace, just as for other Redis state.
	key, _ := hex.DecodeString(space.digest("inventory-cursor-encryption"))
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &ProviderStateStore{client: options.Client, space: space, clock: options.Clock, cursorCipher: aead}, nil
}

func (store *ProviderStateStore) key(kind string, digest [32]byte) string {
	return store.space.admissionKey("provider-"+kind, hex.EncodeToString(digest[:]))
}

// RecordProviderStatus implements the engine recorder without depending on SQL.
func (store *ProviderStateStore) RecordProviderStatus(ctx context.Context, observation control.StatusObservation) error {
	event, err := control.NewStatusEvent(observation)
	if err != nil {
		return err
	}
	_, err = store.PersistStatusEvent(ctx, event)
	return err
}

type providerStatusRecord struct {
	Schema string
	Status control.RouteStatus
	// Preserve independent evidence for sticky credit and billing incidents.
	CreditEvidence  *control.StatusEvent
	BillingEvidence *control.StatusEvent
}

func applyProviderEvent(record providerStatusRecord, event control.StatusEvent) (providerStatusRecord, bool) {
	epochChanged := record.Status.ConfigEpoch != event.ConfigEpoch
	if !record.Status.Apply(event) {
		return record, false
	}
	if record.Status.Availability != control.AvailabilityAvailable || record.Status.Credit != control.CreditOK || record.Status.Billing != control.BillingOK {
		record.Status.ConsecutiveDefiniteFailures++
	}
	record.Schema = providerStateSchema
	if epochChanged {
		record.CreditEvidence = nil
		record.BillingEvidence = nil
	}
	if record.Status.Credit != control.CreditLow && record.Status.Credit != control.CreditExhausted {
		record.CreditEvidence = nil
	} else if event.Credit == record.Status.Credit && (record.CreditEvidence == nil || event.Source == control.SourceOperator || event.Source == control.SourceManagementAPI || event.ProviderCode != "") {
		record.CreditEvidence = &event
	}
	if record.Status.Billing != control.BillingIssue {
		record.BillingEvidence = nil
	} else if event.Billing == control.BillingIssue {
		record.BillingEvidence = &event
	}
	return record, true
}

func (store *ProviderStateStore) PersistStatusEvent(ctx context.Context, event control.StatusEvent) (bool, error) {
	validated, err := control.NewStatusEvent(event.StatusObservation)
	if err != nil {
		return false, err
	}
	if validated.EventDigest != event.EventDigest {
		return false, errors.New("status event digest mismatch")
	}
	key := store.key("status", event.ConfigDigest)
	field := store.space.digest("provider-route", event.RouteID)
	return store.update(ctx, key, field, func(previous string) (string, bool, error) {
		var record providerStatusRecord
		if previous != "" {
			if err := decodeProviderStatus(previous, event.ConfigDigest, &record); err != nil {
				return "", false, err
			}
		}
		next, applied := applyProviderEvent(record, event)
		if !applied {
			return "", false, nil
		}
		data, err := json.Marshal(next)
		return string(data), true, err
	})
}

func decodeProviderStatus(raw string, digest [32]byte, record *providerStatusRecord) error {
	if len(raw) > maxProviderRecordBytes || json.Unmarshal([]byte(raw), record) != nil || record.Schema != providerStateSchema || record.Status.ConfigDigest != digest {
		return ErrUnavailable
	}
	status := record.Status
	// Reuse domain validation for every read, including malformed Redis payloads.
	_, err := control.NewStatusEvent(control.StatusObservation{
		ConfigDigest: status.ConfigDigest, ConfigEpoch: status.ConfigEpoch, RouteID: status.RouteID,
		EndpointID: status.EndpointID, EndpointAccountHMAC: status.EndpointAccountHMAC, Provider: status.Provider,
		EndpointFamily: status.EndpointFamily, ObservedAt: status.ObservedAt, ExpiresAt: status.StaleAfter,
		Source: control.SourceOperator, Availability: status.Availability, Credit: status.Credit, Billing: status.Billing,
		EvidenceDigest: status.LastEventDigest, SafeErrorCode: "projection",
	})
	if err != nil || status.ConsecutiveDefiniteFailures < 0 || (status.Circuit != control.CircuitClosed && status.Circuit != control.CircuitOpen && status.Circuit != control.CircuitHalfOpen) {
		return ErrUnavailable
	}
	for _, evidence := range []*control.StatusEvent{record.CreditEvidence, record.BillingEvidence} {
		if evidence == nil {
			continue
		}
		validated, err := control.NewStatusEvent(evidence.StatusObservation)
		if err != nil || validated.EventDigest != evidence.EventDigest || evidence.ConfigDigest != digest || evidence.RouteID != status.RouteID || evidence.EndpointID != status.EndpointID || evidence.EndpointAccountHMAC != status.EndpointAccountHMAC || evidence.Provider != status.Provider || evidence.EndpointFamily != status.EndpointFamily || evidence.ConfigEpoch != status.ConfigEpoch || evidence.ObservedAt.After(status.ObservedAt) {
			return ErrUnavailable
		}
	}
	if (status.Credit == control.CreditLow || status.Credit == control.CreditExhausted) && record.CreditEvidence == nil {
		return ErrUnavailable
	}
	if status.Billing == control.BillingIssue && record.BillingEvidence == nil {
		return ErrUnavailable
	}
	return nil
}

func (store *ProviderStateStore) GetRouteStatus(ctx context.Context, digest [32]byte, route string) (control.RouteStatus, error) {
	options := control.ProviderStatusListOptions{ConfigDigest: digest, AfterRouteID: route}
	if route == "" {
		return control.RouteStatus{}, errors.New("route is required")
	}
	if err := options.Normalize(); err != nil {
		return control.RouteStatus{}, err
	}
	raw, err := store.client.HGet(ctx, store.key("status", digest), store.space.digest("provider-route", route)).Result()
	if errors.Is(err, redisclient.Nil) {
		return control.RouteStatus{}, control.ErrProviderStatusNotFound
	}
	if err != nil {
		return control.RouteStatus{}, resolveStateError(ctx, err)
	}
	var record providerStatusRecord
	if err := decodeProviderStatus(raw, digest, &record); err != nil {
		return control.RouteStatus{}, err
	}
	if record.Status.RouteID != route {
		return control.RouteStatus{}, ErrUnavailable
	}
	return record.Status, nil
}

// update applies optimistic CAS; Lua compares the exact previous bytes and
// atomically accounts for hash size. Stale/duplicate events never extend TTLs
// or mutate evidence. On contention the Go domain transition is recomputed.
func (store *ProviderStateStore) update(ctx context.Context, key, field string, build func(string) (string, bool, error)) (bool, error) {
	for attempt := 0; attempt < 64; attempt++ {
		previous, err := store.client.HGet(ctx, key, field).Result()
		if err != nil && !errors.Is(err, redisclient.Nil) {
			return false, resolveStateError(ctx, err)
		}
		next, changed, err := build(previous)
		if err != nil || !changed {
			return false, err
		}
		if len(next) > maxProviderRecordBytes {
			return false, errors.New("provider state record exceeds size limit")
		}
		applied, err := providerStateScript.Run(ctx, store.client, []string{key}, field, previous, next).Int()
		if err != nil {
			return false, resolveStateError(ctx, err)
		}
		switch applied {
		case 1:
			return true, nil
		case -1:
			return false, errors.New("provider state capacity exceeded")
		case 0:
		default:
			return false, ErrUnavailable
		}
	}
	return false, ErrUnavailable
}

type inventoryCacheRecord struct {
	Schema   string
	ID       uuid.UUID
	Snapshot control.InventorySnapshot
	Cursor   string // AES-GCM ciphertext, never a provider cursor in plaintext.
}

func inventoryIdentity(provider, endpoint string, account [32]byte) string {
	return provider + "\x00" + endpoint + "\x00" + hex.EncodeToString(account[:])
}

func validateInventorySnapshot(snapshot control.InventorySnapshot) error {
	if err := snapshot.Validate(); err != nil {
		return err
	}
	if snapshot.InventoryDigest != control.InventoryDigest(snapshot.Models) {
		return errors.New("inventory digest mismatch")
	}
	for _, model := range snapshot.Models {
		if strings.ContainsAny(model.ProviderModelID+model.DisplayName+model.OwnedBy, "\x00\r\n") || len(model.DisplayName) > 512 || len(model.OwnedBy) > 512 {
			return errors.New("inventory model contains unsafe fields")
		}
	}
	if strings.ContainsAny(snapshot.NextCursor, "\x00\r\n") {
		return errors.New("inventory cursor contains unsafe characters")
	}
	return nil
}

func (store *ProviderStateStore) PersistInventorySnapshot(ctx context.Context, snapshot control.InventorySnapshot) (bool, error) {
	// Normalize equivalent timestamp/map representations before idempotency
	// comparison, without mutating the caller's model slice.
	snapshot.ObservedAt, snapshot.ExpiresAt = snapshot.ObservedAt.UTC(), snapshot.ExpiresAt.UTC()
	snapshot.Models = append([]control.Model(nil), snapshot.Models...)
	for i := range snapshot.Models {
		snapshot.Models[i].CreatedAt = snapshot.Models[i].CreatedAt.UTC()
		if len(snapshot.Models[i].SafeMetadata) == 0 {
			snapshot.Models[i].SafeMetadata = nil
		}
	}
	if err := validateInventorySnapshot(snapshot); err != nil {
		return false, err
	}
	key := store.key("inventory", snapshot.ConfigDigest)
	field := store.space.digest("provider-inventory", inventoryIdentity(snapshot.Provider, snapshot.EndpointID, snapshot.EndpointAccountHMAC))
	record := inventoryCacheRecord{Schema: providerStateSchema, Snapshot: snapshot}
	// Stable identity makes retried writes and query positions deterministic.
	identity := key + "\x00" + field + "\x00" + snapshot.ObservedAt.UTC().Format(time.RFC3339Nano) + "\x00" + hex.EncodeToString(snapshot.InventoryDigest[:])
	record.ID = uuid.NewSHA1(uuid.NameSpaceOID, []byte(identity))
	if snapshot.NextCursor != "" {
		nonce := make([]byte, store.cursorCipher.NonceSize())
		if _, err := rand.Read(nonce); err != nil {
			return false, err
		}
		record.Cursor = base64.RawStdEncoding.EncodeToString(store.cursorCipher.Seal(nonce, nonce, []byte(snapshot.NextCursor), []byte(identity)))
		record.Snapshot.NextCursor = ""
	}
	data, err := json.Marshal(record)
	if err != nil {
		return false, err
	}
	return store.update(ctx, key, field, func(previous string) (string, bool, error) {
		if previous != "" {
			prior, err := store.decodeInventory(previous, snapshot.ConfigDigest)
			if err != nil {
				return "", false, err
			}
			if !snapshot.ObservedAt.After(prior.Snapshot.ObservedAt) {
				if snapshot.ObservedAt.Equal(prior.Snapshot.ObservedAt) {
					old, _ := json.Marshal(prior.Snapshot)
					incoming, _ := json.Marshal(snapshot)
					if string(old) != string(incoming) {
						return "", false, errors.New("conflicting inventory observation at the same timestamp")
					}
				}
				return "", false, nil
			}
		}
		return string(data), true, nil
	})
}

func (store *ProviderStateStore) decodeInventory(raw string, digest [32]byte) (inventoryCacheRecord, error) {
	var record inventoryCacheRecord
	if len(raw) > maxProviderRecordBytes || json.Unmarshal([]byte(raw), &record) != nil || record.Schema != providerStateSchema || record.Snapshot.ConfigDigest != digest || record.ID == uuid.Nil {
		return record, ErrUnavailable
	}
	snapshot := &record.Snapshot
	if snapshot.NextCursor != "" {
		return record, ErrUnavailable
	}
	key := store.key("inventory", digest)
	field := store.space.digest("provider-inventory", inventoryIdentity(snapshot.Provider, snapshot.EndpointID, snapshot.EndpointAccountHMAC))
	identity := key + "\x00" + field + "\x00" + snapshot.ObservedAt.UTC().Format(time.RFC3339Nano) + "\x00" + hex.EncodeToString(snapshot.InventoryDigest[:])
	if record.ID != uuid.NewSHA1(uuid.NameSpaceOID, []byte(identity)) {
		return record, ErrUnavailable
	}
	if record.Cursor != "" {
		encrypted, err := base64.RawStdEncoding.DecodeString(record.Cursor)
		if err != nil || len(encrypted) < store.cursorCipher.NonceSize() {
			return record, ErrUnavailable
		}
		nonce := encrypted[:store.cursorCipher.NonceSize()]
		plain, err := store.cursorCipher.Open(nil, nonce, encrypted[len(nonce):], []byte(identity))
		if err != nil {
			return record, ErrUnavailable
		}
		snapshot.NextCursor = string(plain)
	}
	if validateInventorySnapshot(*snapshot) != nil {
		return record, ErrUnavailable
	}
	return record, nil
}

func (store *ProviderStateStore) GetInventorySnapshot(ctx context.Context, digest [32]byte, provider, endpoint string, account [32]byte) (control.InventorySnapshot, error) {
	options := control.InventoryModelListOptions{ConfigDigest: digest, Provider: provider, EndpointID: endpoint}
	if err := options.Normalize(); err != nil {
		return control.InventorySnapshot{}, err
	}
	if provider == "" || endpoint == "" || account == ([32]byte{}) {
		return control.InventorySnapshot{}, errors.New("inventory endpoint/account identity is required")
	}
	raw, err := store.client.HGet(ctx, store.key("inventory", digest), store.space.digest("provider-inventory", inventoryIdentity(provider, endpoint, account))).Result()
	if errors.Is(err, redisclient.Nil) {
		return control.InventorySnapshot{}, control.ErrInventorySnapshotNotFound
	}
	if err != nil {
		return control.InventorySnapshot{}, resolveStateError(ctx, err)
	}
	record, err := store.decodeInventory(raw, digest)
	if err == nil && inventoryIdentity(record.Snapshot.Provider, record.Snapshot.EndpointID, record.Snapshot.EndpointAccountHMAC) != inventoryIdentity(provider, endpoint, account) {
		err = ErrUnavailable
	}
	return record.Snapshot, err
}

// providerView pins all current records at one atomic HGETALL read. Public
// cursor signing/authentication remains in control. A missing continuation view
// is an error, never a silently reconstructed page from newer records.
type providerView struct {
	Schema  string
	Horizon time.Time
	Records map[string]string
}

func (store *ProviderStateStore) view(ctx context.Context, kind string, digest [32]byte, horizon time.Time, continuation bool) (providerView, error) {
	if horizon.IsZero() {
		horizon = store.clock().UTC()
	}
	key := store.space.admissionKey("provider-view", kind, hex.EncodeToString(digest[:]), horizon.UTC().Truncate(time.Second).Format(time.RFC3339))
	decode := func(raw string) (providerView, error) {
		var view providerView
		if len(raw) > 64<<20 || json.Unmarshal([]byte(raw), &view) != nil || view.Schema != providerStateSchema || view.Horizon.Unix() != horizon.Unix() || len(view.Records) > 4097 {
			return view, ErrUnavailable
		}
		return view, nil
	}
	raw, err := store.client.Get(ctx, key).Result()
	if err == nil {
		return decode(raw)
	}
	if !errors.Is(err, redisclient.Nil) {
		return providerView{}, resolveStateError(ctx, err)
	}
	if continuation {
		return providerView{}, control.ErrProviderViewExpired
	}
	values, err := store.client.HGetAll(ctx, store.key(kind, digest)).Result()
	if err != nil {
		return providerView{}, resolveStateError(ctx, err)
	}
	delete(values, "__bytes")
	view := providerView{Schema: providerStateSchema, Horizon: horizon, Records: values}
	data, err := json.Marshal(view)
	if err != nil {
		return providerView{}, err
	}
	if len(data) > 64<<20 || len(values) > 4096 {
		return providerView{}, ErrUnavailable
	}
	won, err := store.client.SetNX(ctx, key, data, providerViewTTL).Result()
	if err != nil {
		return providerView{}, resolveStateError(ctx, err)
	}
	if won {
		return view, nil
	}
	raw, err = store.client.Get(ctx, key).Result()
	if err != nil {
		return providerView{}, resolveStateError(ctx, err)
	}
	return decode(raw)
}

func (store *ProviderStateStore) statusView(ctx context.Context, digest [32]byte, horizon time.Time, continuation bool) ([]providerStatusRecord, error) {
	view, err := store.view(ctx, "status", digest, horizon, continuation)
	if err != nil {
		return nil, err
	}
	result := make([]providerStatusRecord, 0, len(view.Records))
	for field, raw := range view.Records {
		var record providerStatusRecord
		if err := decodeProviderStatus(raw, digest, &record); err != nil {
			return nil, err
		}
		if field != store.space.digest("provider-route", record.Status.RouteID) {
			return nil, ErrUnavailable
		}
		if !record.Status.ObservedAt.After(view.Horizon) {
			result = append(result, record)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Status.RouteID < result[j].Status.RouteID })
	return result, nil
}

func (store *ProviderStateStore) ListRouteStatuses(ctx context.Context, options control.ProviderStatusListOptions) (control.ProviderStatusPage, error) {
	var page control.ProviderStatusPage
	if err := options.Normalize(); err != nil {
		return page, err
	}
	if options.AfterRouteID != "" && options.SnapshotHorizon.IsZero() {
		return page, errors.New("provider continuation requires horizon")
	}
	records, err := store.statusView(ctx, options.ConfigDigest, options.SnapshotHorizon, options.AfterRouteID != "")
	if err != nil {
		return page, err
	}
	for _, record := range records {
		status := record.Status
		if status.RouteID <= options.AfterRouteID || (options.Provider != "" && options.Provider != status.Provider) || (options.EndpointID != "" && options.EndpointID != status.EndpointID) || (options.Availability != "" && options.Availability != status.Availability) {
			continue
		}
		if !options.IncludeHealthy && status.Availability == control.AvailabilityAvailable && status.Credit == control.CreditOK && status.Billing == control.BillingOK && status.Circuit == control.CircuitClosed {
			continue
		}
		if len(page.Routes) == options.Limit {
			page.NextRouteID = page.Routes[len(page.Routes)-1].RouteID
			break
		}
		page.Routes = append(page.Routes, status)
	}
	return page, nil
}

func (store *ProviderStateStore) ListCreditStatuses(ctx context.Context, options control.CreditStatusListOptions) (control.CreditStatusPage, error) {
	var page control.CreditStatusPage
	if err := options.Normalize(); err != nil {
		return page, err
	}
	if options.AfterEndpointKey != "" && options.SnapshotHorizon.IsZero() {
		return page, errors.New("credit continuation requires horizon")
	}
	records, err := store.statusView(ctx, options.ConfigDigest, options.SnapshotHorizon, options.AfterEndpointKey != "")
	if err != nil {
		return page, err
	}
	latest := map[string]providerStatusRecord{}
	for _, record := range records {
		status := record.Status
		if (options.Provider != "" && options.Provider != status.Provider) || (options.EndpointID != "" && options.EndpointID != status.EndpointID) {
			continue
		}
		key := status.Provider + "\x00" + status.EndpointID
		previous, ok := latest[key]
		if !ok || !status.ObservedAt.Before(previous.Status.ObservedAt) {
			latest[key] = record
		}
	}
	for key, record := range latest {
		status := record.Status
		if key <= options.AfterEndpointKey || (!options.IncludeOK && status.Credit == control.CreditOK && status.Billing == control.BillingOK) {
			continue
		}
		// Prefer billing evidence when blocked, otherwise current credit evidence.
		evidence := record.CreditEvidence
		if record.BillingEvidence != nil {
			evidence = record.BillingEvidence
		}
		source, safe, code := control.SourceInference, "", ""
		if evidence != nil {
			source, safe, code = evidence.Source, evidence.SafeErrorCode, evidence.ProviderCode
		}
		credit, err := control.NewCreditStatus(status.Provider, status.EndpointID, status.Credit, status.Billing, status.CreditConfirmedAt, source, safe, code)
		if err != nil {
			return page, ErrUnavailable
		}
		credit.ObservedAt, credit.StaleAfter = status.ObservedAt, status.StaleAfter
		page.Endpoints = append(page.Endpoints, credit)
	}
	sort.Slice(page.Endpoints, func(i, j int) bool { return page.Endpoints[i].Key() < page.Endpoints[j].Key() })
	if len(page.Endpoints) > options.Limit {
		page.Endpoints = page.Endpoints[:options.Limit]
		page.NextEndpointKey = page.Endpoints[len(page.Endpoints)-1].Key()
	}
	return page, page.Validate()
}

func inventoryPosition(record control.InventoryModelRecord) control.InventoryModelPosition {
	return control.InventoryModelPosition{Provider: record.Snapshot.Provider, EndpointID: record.Snapshot.EndpointID, SnapshotID: record.Snapshot.ID, ProviderModelID: record.Model.ProviderModelID}
}
func inventoryPositionKey(position control.InventoryModelPosition) string {
	return position.Provider + "\x00" + position.EndpointID + "\x00" + position.SnapshotID.String() + "\x00" + position.ProviderModelID
}

func (store *ProviderStateStore) ListInventoryModels(ctx context.Context, options control.InventoryModelListOptions) (control.InventoryModelPage, error) {
	var page control.InventoryModelPage
	if err := options.Normalize(); err != nil {
		return page, err
	}
	view, err := store.view(ctx, "inventory", options.ConfigDigest, options.SnapshotHorizon, options.After != (control.InventoryModelPosition{}))
	if err != nil {
		return page, err
	}
	page.SnapshotHorizon = view.Horizon
	for field, raw := range view.Records {
		record, err := store.decodeInventory(raw, options.ConfigDigest)
		if err != nil {
			return page, err
		}
		snapshot := record.Snapshot
		if field != store.space.digest("provider-inventory", inventoryIdentity(snapshot.Provider, snapshot.EndpointID, snapshot.EndpointAccountHMAC)) {
			return page, ErrUnavailable
		}
		if snapshot.ObservedAt.After(view.Horizon) || (options.Provider != "" && options.Provider != snapshot.Provider) || (options.EndpointID != "" && options.EndpointID != snapshot.EndpointID) {
			continue
		}
		info := control.InventorySnapshotInfo{ID: record.ID, ConfigDigest: snapshot.ConfigDigest, InventoryDigest: snapshot.InventoryDigest, Provider: snapshot.Provider, EndpointID: snapshot.EndpointID, Source: snapshot.Source, ObservedAt: snapshot.ObservedAt, ExpiresAt: snapshot.ExpiresAt, Complete: snapshot.Complete}
		for _, model := range snapshot.Models {
			if !strings.HasPrefix(model.ProviderModelID, options.ModelPrefix) || (options.Lifecycle != "" && options.Lifecycle != model.Lifecycle) {
				continue
			}
			row := control.InventoryModelRecord{Snapshot: info, Model: model}
			if options.After != (control.InventoryModelPosition{}) && inventoryPositionKey(inventoryPosition(row)) <= inventoryPositionKey(options.After) {
				continue
			}
			page.Models = append(page.Models, row)
		}
	}
	sort.Slice(page.Models, func(i, j int) bool {
		return inventoryPositionKey(inventoryPosition(page.Models[i])) < inventoryPositionKey(inventoryPosition(page.Models[j]))
	})
	if len(page.Models) > options.Limit {
		page.Models = page.Models[:options.Limit]
		next := inventoryPosition(page.Models[len(page.Models)-1])
		page.Next = &next
	}
	return page, nil
}
