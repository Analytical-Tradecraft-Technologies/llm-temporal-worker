package control

// QueryService is the deliberately small control-plane boundary used by the
// llm.query.v1 Activity. It validates the closed wire request, authorizes the
// tenant scope, verifies pagination cursors, and validates the handler result
// for all five query kinds. Persisted provider/model/credit reads, budget
// reads, and spend aggregation are supplied by a later composition layer; this
// package does not perform storage or provider calls.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

var (
	ErrQueryAuthorization = errors.New("query authorization denied")
	ErrQueryCursor        = errors.New("query cursor is invalid")
	ErrQueryUnsupported   = errors.New("query kind is not implemented")
)

// Authorization is the stable, content-free input to a query authorizer.
// Query payloads are intentionally not passed to authorization callbacks.
type Authorization struct {
	Tenant  string
	Project string
	Actor   string
	Kind    llm.QueryKind
}

type AuthorizeFunc func(context.Context, Authorization) error

// Handler owns the actual read implementation. Keeping it behind this
// interface lets the Activity boundary be tested without a database and keeps
// storage-specific query plans out of the wire adapter.
type Handler interface {
	ExecuteQuery(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error)
}

// TypedHandler owns a typed control-plane read. The service validates the
// closed wire envelope before this seam is called, authenticates the optional
// cursor, and converts the typed response back to the wire envelope. Claims
// are nil for the first page and otherwise contain the authenticated storage
// position and snapshot horizon.
//
// Handler remains supported for storage adapters that have not migrated to the
// typed model yet. New adapters should implement TypedHandler so they cannot
// accidentally consume an unsigned or horizon-free cursor.
type TypedHandler interface {
	ExecuteTypedQuery(context.Context, QueryRequest, *BoundCursorClaims) (QueryResponse, error)
}

// QueryService is safe to share between Activity invocations. Clock is a test
// seam and must return UTC time. CursorKey is an HMAC key kept in worker
// configuration; cursors are never accepted without one.
type QueryService struct {
	Handler      Handler
	TypedHandler TypedHandler
	Authorize    AuthorizeFunc
	Audit        AuditFunc
	// CursorCodec is required when TypedHandler is set. It is optional for the
	// legacy Handler seam, whose CursorKey path remains source-compatible.
	CursorCodec *CursorCodec
	CursorKey   []byte
	CursorTTL   time.Duration
	Clock       func() time.Time
}

func (service *QueryService) Execute(ctx context.Context, request llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	if service == nil || (service.Handler == nil && service.TypedHandler == nil) || service.Authorize == nil {
		return llm.QueryResponseV1{}, fmt.Errorf("query service is not configured")
	}
	if service.TypedHandler != nil && service.CursorCodec == nil {
		return llm.QueryResponseV1{}, fmt.Errorf("typed query service cursor codec is not configured")
	}
	if ctx == nil {
		return llm.QueryResponseV1{}, fmt.Errorf("query context is nil")
	}
	if err := ctx.Err(); err != nil {
		return llm.QueryResponseV1{}, err
	}
	if request.APIVersion != llm.QueryAPIVersion {
		return llm.QueryResponseV1{}, fmt.Errorf("query api version %q is unsupported", request.APIVersion)
	}
	if !supportedQueryKind(request.Kind) {
		return llm.QueryResponseV1{}, fmt.Errorf("%w: %s", ErrQueryUnsupported, request.Kind)
	}
	if err := validatePageSize(request.Query); err != nil {
		return llm.QueryResponseV1{}, err
	}
	// MarshalJSON is the closed-request validator: it rejects unknown query
	// fields, non-object filters, invalid enums, and page sizes outside the
	// 1..1000 bound before a handler can touch storage.
	requestJSON, marshalErr := json.Marshal(request)
	if marshalErr != nil {
		return llm.QueryResponseV1{}, fmt.Errorf("query request is invalid: %w", marshalErr)
	}
	if err := validateScope(request.Context); err != nil {
		return llm.QueryResponseV1{}, err
	}
	if err := service.Authorize(ctx, Authorization{
		Tenant: request.Context.Tenant, Project: request.Context.Project,
		Actor: request.Context.Actor, Kind: request.Kind,
	}); err != nil {
		return llm.QueryResponseV1{}, fmt.Errorf("%w: %v", ErrQueryAuthorization, err)
	}
	now := service.now()
	var typedRequest QueryRequest
	var typedClaims *BoundCursorClaims
	if service.CursorCodec != nil {
		var err error
		typedRequest, err = DecodeQueryRequest(request)
		if err != nil {
			return llm.QueryResponseV1{}, fmt.Errorf("typed query request: %w", err)
		}
		cursor := typedQueryCursor(typedRequest.Filter)
		if cursor != nil {
			claims, err := service.CursorCodec.Decode(typedRequest, *cursor, now)
			if err != nil {
				return llm.QueryResponseV1{}, err
			}
			typedClaims = &claims
		}
	} else if cursor, ok, err := requestCursor(request.Query); err != nil {
		return llm.QueryResponseV1{}, err
	} else if ok {
		if err := service.ValidateCursor(request, cursor, now); err != nil {
			return llm.QueryResponseV1{}, err
		}
	}
	var response llm.QueryResponseV1
	var typedResponse QueryResponse
	var err error
	if service.TypedHandler != nil {
		typedResponse, err = service.TypedHandler.ExecuteTypedQuery(ctx, typedRequest, typedClaims)
		if err == nil {
			response, err = EncodeQueryResponse(typedResponse)
			if err != nil {
				// A typed handler returned a response that violates the closed
				// model contract. This is analogous to validateResponse below,
				// not a transient state outage, so preserve the contract error
				// instead of mapping it to provider.CodeStateUnavailable.
				return llm.QueryResponseV1{}, fmt.Errorf("typed query response: %w", err)
			}
		}
	} else {
		response, err = service.Handler.ExecuteQuery(ctx, request)
	}
	if err != nil {
		var providerErr *provider.Error
		if errors.As(err, &providerErr) {
			return llm.QueryResponseV1{}, err
		}
		// Preserve caller cancellation or deadline only when the query context
		// itself has reached that terminal state. A child storage timeout may
		// also return context.DeadlineExceeded while the Activity context is
		// still live; that remains a retryable durable-state outage.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return llm.QueryResponseV1{}, ctxErr
		}
		// Query handlers are the storage/control-plane boundary. Untyped
		// handler failures must not cross the Activity seam as caller input:
		// classify them as unavailable durable state so Temporal can retry the
		// read, while retaining the original cause for local diagnostics.
		mapped := provider.NewError(provider.CodeStateUnavailable, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetrySameOperation, "query state is unavailable")
		mapped.Cause = err
		return llm.QueryResponseV1{}, mapped
	}
	if err := validateResponse(request, response); err != nil {
		return llm.QueryResponseV1{}, err
	}
	if response.NextCursor != nil {
		// The handler may spend enough time reading storage or refreshing
		// provider state for a cursor it signs on return to be more than the
		// allowed future-skew window relative to the pre-handler timestamp.
		// Keep `now` for the incoming cursor check, but validate outgoing
		// cursors against a fresh clock sample after the handler completes.
		if service.CursorCodec != nil {
			if typedResponse.NextCursor == nil {
				decoded, err := DecodeQueryResponse(response)
				if err != nil {
					return llm.QueryResponseV1{}, fmt.Errorf("next_cursor response: %w", err)
				}
				typedResponse = decoded
			}
			cursor := typedResponse.NextCursor
			if cursor == nil {
				return llm.QueryResponseV1{}, fmt.Errorf("next_cursor: %w", ErrQueryCursor)
			}
			outgoingClaims, err := service.CursorCodec.Decode(typedRequest, *cursor, service.now())
			if err != nil {
				return llm.QueryResponseV1{}, fmt.Errorf("next_cursor: %w", err)
			}
			if typedClaims != nil && !outgoingClaims.Horizon.Equal(typedClaims.Horizon) {
				return llm.QueryResponseV1{}, fmt.Errorf("next_cursor: %w", ErrQueryCursor)
			}
		} else if err := service.ValidateCursor(request, *response.NextCursor, service.now()); err != nil {
			return llm.QueryResponseV1{}, fmt.Errorf("next_cursor: %w", err)
		}
	}
	if service.Audit != nil {
		audit, err := buildQueryAudit(request, requestJSON, response, now, service.now())
		if err != nil {
			return llm.QueryResponseV1{}, fmt.Errorf("query audit: %w", err)
		}
		if err := service.Audit(ctx, audit); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return llm.QueryResponseV1{}, ctxErr
			}
			mapped := provider.NewError(provider.CodeStateUnavailable, provider.PhaseFinalize, provider.DispatchNotDispatched, provider.RetrySameOperation, "query audit state is unavailable")
			mapped.Cause = err
			return llm.QueryResponseV1{}, mapped
		}
	}
	return response, nil
}

// typedQueryCursor extracts the cursor from the three paginated query filters.
// Budget and spend filters intentionally have no cursor field: their complete
// result is one bounded snapshot rather than a keyset page. Keep those query
// kinds explicit in the switch so adding a future paginated filter cannot
// accidentally inherit an unsigned continuation.
func typedQueryCursor(filter QueryFilter) *QueryCursor {
	switch value := filter.(type) {
	case ProviderStatusQuery:
		return value.Page.Cursor
	case *ProviderStatusQuery:
		if value != nil {
			return value.Page.Cursor
		}
	case ModelInventoryQuery:
		return value.Page.Cursor
	case *ModelInventoryQuery:
		if value != nil {
			return value.Page.Cursor
		}
	case CreditStatusQuery:
		return value.Page.Cursor
	case *CreditStatusQuery:
		if value != nil {
			return value.Page.Cursor
		}
	case BudgetStatusQuery, *BudgetStatusQuery, SpendSummaryQuery, *SpendSummaryQuery:
		return nil
	}
	return nil
}

func supportedQueryKind(kind llm.QueryKind) bool {
	return kind == llm.QueryProviderStatus || kind == llm.QueryModelInventory || kind == llm.QueryCreditStatus || kind == llm.QueryBudgetStatus || kind == llm.QuerySpendSummary
}

func validateScope(scope llm.RequestContext) error {
	for name, value := range map[string]string{"tenant": scope.Tenant, "project": scope.Project, "actor": scope.Actor} {
		if value == "" || len(value) > 256 || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("query %s scope is empty or unsafe", name)
		}
	}
	if len(scope.Tags) > 32 {
		return fmt.Errorf("query scope has too many tags")
	}
	for key, value := range scope.Tags {
		if value == "" || len(key) > 128 || len(value) > 256 || strings.TrimSpace(key) != key || strings.TrimSpace(value) != value || strings.ContainsAny(key, "\x00\r\n") || strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("query scope tag is unsafe")
		}
	}
	return nil
}

type cursorEnvelope struct {
	Kind      llm.QueryKind `json:"kind"`
	Scope     string        `json:"scope"`
	QueryHash string        `json:"query_hash"`
	Position  string        `json:"position"`
	IssuedAt  int64         `json:"issued_at"`
	ExpiresAt int64         `json:"expires_at"`
}

// CursorClaims are the authenticated, scope- and filter-bound values carried
// by a query cursor. Position remains opaque to this package; query adapters
// may encode a bounded storage key and snapshot horizon in it before signing.
type CursorClaims struct {
	Kind      llm.QueryKind
	Scope     string
	QueryHash string
	Position  string
	IssuedAt  time.Time
	ExpiresAt time.Time
}

// SignCursor creates a scope- and filter-bound cursor for a handler's opaque
// position. The position is never interpreted by this package.
func (service *QueryService) SignCursor(request llm.QueryRequestV1, position string, issuedAt time.Time) (string, error) {
	if service == nil || len(service.CursorKey) == 0 || position == "" {
		return "", ErrQueryCursor
	}
	if request.APIVersion != llm.QueryAPIVersion || !supportedQueryKind(request.Kind) || validateScope(request.Context) != nil {
		return "", ErrQueryCursor
	}
	issuedAt = issuedAt.UTC()
	ttl := service.CursorTTL
	if ttl <= 0 || ttl > 24*time.Hour {
		ttl = 15 * time.Minute
	}
	envelope := cursorEnvelope{Kind: request.Kind, Scope: scopeString(request.Context), QueryHash: queryHash(request.Query), Position: position, IssuedAt: issuedAt.Unix(), ExpiresAt: issuedAt.Add(ttl).Unix()}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return "", ErrQueryCursor
	}
	signature := hmac.New(sha256.New, service.CursorKey)
	_, _ = signature.Write(payload)
	encode := base64.RawURLEncoding
	return encode.EncodeToString(payload) + "." + encode.EncodeToString(signature.Sum(nil)), nil
}

func (service *QueryService) ValidateCursor(request llm.QueryRequestV1, token string, now time.Time) error {
	_, err := service.DecodeCursor(request, token, now)
	return err
}

// DecodeCursor authenticates a cursor and returns its claims for a typed
// query adapter. Callers must still validate and decode Position according to
// their query-specific contract; no untrusted position is returned before
// the HMAC and scope/filter checks succeed.
func (service *QueryService) DecodeCursor(request llm.QueryRequestV1, token string, now time.Time) (CursorClaims, error) {
	var claims CursorClaims
	if service == nil || len(service.CursorKey) == 0 || len(token) > 2048 {
		return claims, ErrQueryCursor
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return claims, ErrQueryCursor
	}
	decode := base64.RawURLEncoding
	payload, err := decode.DecodeString(parts[0])
	if err != nil {
		return claims, ErrQueryCursor
	}
	provided, err := decode.DecodeString(parts[1])
	if err != nil {
		return claims, ErrQueryCursor
	}
	signature := hmac.New(sha256.New, service.CursorKey)
	_, _ = signature.Write(payload)
	if !hmac.Equal(provided, signature.Sum(nil)) {
		return claims, ErrQueryCursor
	}
	var envelope cursorEnvelope
	if json.Unmarshal(payload, &envelope) != nil || envelope.Kind != request.Kind || envelope.Scope != scopeString(request.Context) || envelope.QueryHash != queryHash(request.Query) || envelope.Position == "" {
		return claims, ErrQueryCursor
	}
	now = now.UTC()
	if now.IsZero() || envelope.IssuedAt > now.Add(2*time.Minute).Unix() || envelope.ExpiresAt <= now.Unix() || envelope.ExpiresAt <= envelope.IssuedAt {
		return claims, ErrQueryCursor
	}
	return CursorClaims{Kind: envelope.Kind, Scope: envelope.Scope, QueryHash: envelope.QueryHash, Position: envelope.Position, IssuedAt: time.Unix(envelope.IssuedAt, 0).UTC(), ExpiresAt: time.Unix(envelope.ExpiresAt, 0).UTC()}, nil
}

func requestCursor(raw json.RawMessage) (string, bool, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return "", false, fmt.Errorf("query object is invalid: %w", err)
	}
	value, ok := fields["cursor"]
	if !ok || string(value) == "null" {
		return "", false, nil
	}
	var cursor string
	if err := json.Unmarshal(value, &cursor); err != nil || cursor == "" {
		return "", false, ErrQueryCursor
	}
	return cursor, true, nil
}

const (
	defaultQueryPageSize = 100
	maxQueryPageSize     = 1000
)

// queryPageSize returns the effective limit for a paginated query. Storage
// adapters use the same 100-row default, but the Activity boundary must keep
// this invariant even when a handler is supplied by a different composition
// layer (or is accidentally misconfigured).
func queryPageSize(raw json.RawMessage) (int, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return 0, fmt.Errorf("query object is invalid: %w", err)
	}
	value, ok := fields["page_size"]
	if !ok {
		return defaultQueryPageSize, nil
	}
	var pageSize int
	if err := json.Unmarshal(value, &pageSize); err != nil || pageSize < 1 || pageSize > maxQueryPageSize {
		return 0, fmt.Errorf("query page_size must be between 1 and %d", maxQueryPageSize)
	}
	return pageSize, nil
}

func validatePageSize(raw json.RawMessage) error {
	_, err := queryPageSize(raw)
	return err
}

func queryHash(raw json.RawMessage) string {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return ""
	}
	delete(fields, "cursor")
	canonical, _ := json.Marshal(fields)
	digest := sha256.Sum256(canonical)
	return base64.RawURLEncoding.EncodeToString(digest[:])
}

func scopeString(scope llm.RequestContext) string {
	canonical, _ := json.Marshal(struct {
		Tenant  string            `json:"tenant"`
		Project string            `json:"project"`
		Actor   string            `json:"actor"`
		Tags    map[string]string `json:"tags,omitempty"`
	}{scope.Tenant, scope.Project, scope.Actor, scope.Tags})
	return base64.RawURLEncoding.EncodeToString(canonical)
}

func (service *QueryService) now() time.Time {
	if service != nil && service.Clock != nil {
		return service.Clock().UTC()
	}
	return time.Now().UTC()
}

func validateResponse(request llm.QueryRequestV1, response llm.QueryResponseV1) error {
	if response.APIVersion != llm.QueryAPIVersion || response.OperationKey != request.OperationKey || response.Kind != request.Kind {
		return fmt.Errorf("query response does not match request")
	}
	if _, err := json.Marshal(response); err != nil {
		return fmt.Errorf("query response is invalid: %w", err)
	}
	if err := validateResponsePageSize(request, response); err != nil {
		return err
	}
	return nil
}

// validateResponsePageSize keeps paginated results bounded at the Activity
// boundary. A handler is an intentionally injectable seam, so relying solely
// on storage repository limits would allow a misconfigured adapter to return a
// larger page than the caller requested. Budget and spend are complete,
// non-paginated snapshots and therefore have no page-size limit.
func validateResponsePageSize(request llm.QueryRequestV1, response llm.QueryResponseV1) error {
	if request.Kind == llm.QueryBudgetStatus || request.Kind == llm.QuerySpendSummary {
		return nil
	}
	limit, err := queryPageSize(request.Query)
	if err != nil {
		return err
	}
	count, err := queryResponseRowCount(response)
	if err != nil {
		return err
	}
	if count > limit {
		return fmt.Errorf("query response page contains %d rows; limit is %d", count, limit)
	}
	return nil
}

func queryResponseRowCount(response llm.QueryResponseV1) (int, error) {
	switch result := response.Result.(type) {
	case llm.ProviderStatusPage:
		return len(result.Routes), nil
	case *llm.ProviderStatusPage:
		if result == nil {
			return 0, fmt.Errorf("query response result is nil")
		}
		return len(result.Routes), nil
	case llm.ModelInventoryPage:
		return len(result.Models), nil
	case *llm.ModelInventoryPage:
		if result == nil {
			return 0, fmt.Errorf("query response result is nil")
		}
		return len(result.Models), nil
	case llm.CreditStatusPage:
		return len(result.Endpoints), nil
	case *llm.CreditStatusPage:
		if result == nil {
			return 0, fmt.Errorf("query response result is nil")
		}
		return len(result.Endpoints), nil
	default:
		return 0, fmt.Errorf("query response result has unexpected type")
	}
}
