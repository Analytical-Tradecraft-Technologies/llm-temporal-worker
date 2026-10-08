package budget

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
)

// ErrUnusablePrice marks an otherwise valid candidate whose active catalog
// entry cannot produce a safe reservation. Callers that are evaluating an
// ordered route plan may skip this candidate and continue to the next
// authorized fallback. Tokenizer, request, and estimator configuration
// errors deliberately do not use this sentinel and remain hard failures.
var ErrUnusablePrice = errors.New("candidate price is unusable")

type Estimator struct {
	SafetyRatio  *big.Rat
	MaxOutput    int64
	MaxReasoning int64
	// Tokenizer, when configured, is the provider-specific exact token
	// counter. It receives the candidate because tokenization can vary by
	// provider family/model. A nil tokenizer uses the conservative UTF-8
	// fallback below.
	Tokenizer Tokenizer
}

// Tokenizer returns the exact input token count for one authorized candidate.
// Implementations must be deterministic and must not perform provider I/O.
type Tokenizer func(llm.Request, routing.Candidate) (int64, error)

type Estimate struct {
	CandidateID      string
	InputTokens      int64
	OutputTokens     int64
	ReasoningTokens  int64
	CacheWriteTokens int64
	MicroUSD         pricing.MicroUSD
	// CostUSD is the exact fixed-scale reservation used by new callers.
	CostUSD        pricing.USD
	CatalogVersion string
}

// PrepareRequest normalizes an independent request and materializes the output
// limit used for its reservation. Callers must compile this same request so
// provider defaults cannot exceed the authorized estimate.
func (estimator Estimator) PrepareRequest(request llm.Request) (llm.Request, error) {
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		return llm.Request{}, err
	}
	limit, err := estimator.outputLimit(normalized)
	if err != nil {
		return llm.Request{}, err
	}
	if normalized.Output == nil {
		normalized.Output = &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindText}}
	}
	value := int(limit)
	normalized.Output.MaxTokens = &value
	return normalized, nil
}

func (estimator Estimator) outputLimit(request llm.Request) (int64, error) {
	limit := estimator.MaxOutput
	if limit <= 0 {
		limit = 1000
	}
	if request.Output != nil && request.Output.MaxTokens != nil {
		limit = int64(*request.Output.MaxTokens)
	}
	// Some SDKs use int32. Zero can mean an omitted cap, not a zero-cost call.
	if limit <= 0 || limit > math.MaxInt32 {
		return 0, fmt.Errorf("output token limit must be between 1 and %d", int64(math.MaxInt32))
	}
	return limit, nil
}

func (estimator Estimator) EstimateCandidate(request llm.Request, candidate routing.Candidate, entry pricing.Entry) (Estimate, error) {
	if err := entry.ValidateUsagePricing(); err != nil {
		return Estimate{}, fmt.Errorf("%w: %v", ErrUnusablePrice, err)
	}
	inputTokens, err := estimator.estimateInput(request, candidate)
	if err != nil {
		return Estimate{}, err
	}
	outputTokens, err := estimator.outputLimit(request)
	if err != nil {
		return Estimate{}, err
	}
	reasoningTokens := int64(0)
	if request.Reasoning != nil && request.Reasoning.TokenBudget != nil {
		reasoningTokens = int64(*request.Reasoning.TokenBudget)
	}
	if estimator.MaxReasoning > reasoningTokens {
		reasoningTokens = estimator.MaxReasoning
	}
	if estimator.Tokenizer == nil {
		inputTokens = addMediaAllowance(inputTokens, mediaInputAllowance(request), candidate.ContextTokens, outputTokens)
	}
	// reasoningTokens stays a separately priced reservation component below,
	// but it is spent inside the output cap and takes no extra context room.
	if err := validateContextCounts(candidate.ContextTokens, inputTokens, outputTokens); err != nil {
		return Estimate{}, err
	}
	// Server tools add inputs that cannot be tokenized before dispatch.
	// Estimate their bounded research loop rather than reserving full contexts.
	if request.WebSearch || request.WebFetch || request.CodeExecution {
		if candidate.ContextTokens <= 0 {
			return Estimate{}, fmt.Errorf("%w: hosted tools require a known context ceiling", ErrUnusablePrice)
		}
		if request.CodeExecution {
			// Code execution has no per-request call bound yet. Retain its
			// conservative allowance until that provider loop is bounded.
			rounds := int64(4)
			if candidate.Family == "anthropic_messages" {
				rounds = 20
			}
			if candidate.ContextTokens > math.MaxInt64/rounds || outputTokens > math.MaxInt64/rounds {
				return Estimate{}, fmt.Errorf("hosted tool token allowance overflows")
			}
			inputTokens = candidate.ContextTokens * rounds
			outputTokens *= rounds
		} else {
			inputTokens = hostedResearchInput(request, candidate, inputTokens, outputTokens)
		}
	}
	cacheWrite := inputTokens
	components := []struct {
		component     pricing.PriceComponent
		price         pricing.DecimalUSD
		units         int64
		unitsPerPrice int64
		name          string
	}{
		{pricing.PriceComponentInput, entry.Prices.InputPerMillion, inputTokens, 1_000_000, "input"},
		{pricing.PriceComponentOutput, entry.Prices.OutputPerMillion, outputTokens, 1_000_000, "output"},
		{pricing.PriceComponentReasoning, entry.Prices.ReasoningPerMillion, reasoningTokens, 1_000_000, "reasoning"},
		{pricing.PriceComponentCacheWrite, entry.Prices.CacheWritePerMillion, cacheWrite, 1_000_000, "cache_write"},
		// PerRequest is already an amount in USD for this invocation. It is
		// not quoted per million units like the token components.
		{pricing.PriceComponentPerRequest, entry.Prices.PerRequest, 1, 1, "per_request"},
	}
	totalUSD := pricing.MustUSD("0")
	legacyTotal := pricing.MicroUSD(0)
	for _, component := range components {
		if component.units > 0 && entry.ComponentUnknown(component.component) {
			return Estimate{}, fmt.Errorf("%w: estimate %s has no known USD catalog price", ErrUnusablePrice, component.name)
		}
		value, err := pricing.CeilUSD(component.price, component.units, component.unitsPerPrice)
		if err != nil {
			return Estimate{}, fmt.Errorf("estimate %s: %w", component.name, err)
		}
		totalUSD, err = totalUSD.Add(value)
		if err != nil {
			return Estimate{}, err
		}
		legacy, legacyErr := pricing.CeilMicroUSD(component.price, component.units, component.unitsPerPrice)
		if legacyErr != nil {
			// USD is authoritative, but the estimator is also responsible for
			// producing the bounded compatibility reservation consumed by Redis.
			// Silently dropping an overflowing component would under-reserve.
			return Estimate{}, fmt.Errorf("%w: estimate %s microUSD compatibility conversion: %w", ErrUnusablePrice, component.name, legacyErr)
		}
		legacyTotal, err = legacyTotal.Add(legacy)
		if err != nil {
			return Estimate{}, fmt.Errorf("%w: estimate %s microUSD compatibility total: %w", ErrUnusablePrice, component.name, err)
		}
	}
	toolAllowance := llm.HostedToolReservation(request, candidate.Family)
	totalUSD, err = totalUSD.Add(toolAllowance)
	if err != nil {
		return Estimate{}, err
	}
	allowanceMicro, err := pricing.CeilMicroFromUSD(toolAllowance)
	if err != nil {
		return Estimate{}, err
	}
	legacyTotal, err = legacyTotal.Add(allowanceMicro)
	if err != nil {
		return Estimate{}, err
	}
	if estimator.SafetyRatio != nil {
		if estimator.SafetyRatio.Sign() <= 0 {
			return Estimate{}, fmt.Errorf("safety ratio must be positive")
		}
		totalUSD, err = multiplyUSD(totalUSD, estimator.SafetyRatio)
		if err != nil {
			return Estimate{}, err
		}
		legacyTotal, err = multiplyCeil(legacyTotal, estimator.SafetyRatio)
		if err != nil {
			return Estimate{}, fmt.Errorf("%w: estimate microUSD compatibility multiplier: %w", ErrUnusablePrice, err)
		}
	}
	return Estimate{CandidateID: candidate.ID, InputTokens: inputTokens, OutputTokens: outputTokens, ReasoningTokens: reasoningTokens, CacheWriteTokens: cacheWrite, CostUSD: totalUSD, MicroUSD: legacyTotal, CatalogVersion: entry.Version}, nil
}

// hostedResearchInput includes the prompt in each possible tool continuation,
// accumulated results, and an allowance for intermediate assistant output.
// Search result size is an estimate, not a provider-enforced token ceiling;
// safety margins and settlement against actual usage still apply. Fetch content
// has a 10,000-token cap in the Anthropic lowering layer.
func hostedResearchInput(request llm.Request, candidate routing.Candidate, input, output int64) int64 {
	calls, resultTokens := int64(0), int64(0)
	if request.WebSearch {
		calls += llm.MaxWebSearchCalls
		resultTokens = 8192
	}
	if request.WebFetch {
		calls += llm.MaxWebSearchCalls
		resultTokens = 10000
	}
	if candidate.Family != "anthropic_messages" {
		// Responses and OpenRouter share a max_tool_calls limit.
		calls = min(calls, llm.MaxWebSearchCalls)
	}
	total := input
	for step := int64(1); step <= calls; step++ {
		continuation := saturatingAdd(input, saturatingAdd(step*resultTokens, output))
		continuation = min(continuation, candidate.ContextTokens-output)
		total = saturatingAdd(total, continuation)
	}
	// max_tokens caps the output for this generation; do not charge the
	// entire output cap again for every possible research tool call.
	return total
}

func (estimator Estimator) EstimatePlan(request llm.Request, plan routing.Plan, entries map[string]pricing.Entry) (Estimate, error) {
	if len(plan.Candidates) == 0 {
		return Estimate{}, fmt.Errorf("cannot estimate an empty route plan")
	}
	var maximum Estimate
	for index, candidate := range plan.Candidates {
		entry, ok := entries[candidate.ID]
		if !ok {
			return Estimate{}, fmt.Errorf("price missing for candidate %s", candidate.ID)
		}
		estimate, err := estimator.EstimateCandidate(request, candidate, entry)
		if err != nil {
			return Estimate{}, err
		}
		// Select the first candidate even when its estimate is exactly zero.
		// Otherwise an all-free plan would return an empty candidate identity,
		// losing the route that established the maximum.
		if index == 0 || estimate.CostUSD.Cmp(maximum.CostUSD) > 0 {
			maximum = estimate
		}
	}
	return maximum, nil
}

// CountInputTokens shares the admission estimate with compaction planning. It
// uses the configured exact tokenizer or the existing UTF-8 estimate, without
// looking up prices or acquiring budget. The UTF-8 estimate excludes inline
// image and PDF bytes and the capped media allowance that reservation adds.
func (estimator Estimator) CountInputTokens(request llm.Request, candidate routing.Candidate) (int64, error) {
	return estimator.estimateInput(request, candidate)
}

func (estimator Estimator) estimateInput(request llm.Request, candidate routing.Candidate) (int64, error) {
	if estimator.Tokenizer != nil {
		inputTokens, err := estimator.Tokenizer(request, candidate)
		if err != nil {
			return 0, fmt.Errorf("exact provider tokenization failed: %w", err)
		}
		if inputTokens < 0 {
			return 0, fmt.Errorf("exact provider tokenization returned a negative count")
		}
		return inputTokens, nil
	}
	// Inline media bytes are serialized as base64, which says nothing about
	// what the provider bills: images and PDFs are covered by the per-part
	// media allowance, and inline text documents are counted below on their
	// decoded bytes like any other text.
	textRequest, inlineTextBytes := withoutInlineMediaBytes(request)
	data, err := llm.CanonicalJSONWithLimits(mustRequestJSON(textRequest), 16<<20, 128)
	if err != nil {
		return 0, err
	}
	// UTF-8 bytes / 4 is a conservative provider-independent baseline for
	// ordinary text. Structural overhead is bounded by the serialized request.
	// Text an adapter adds for failed tool results is counted the same way.
	input := saturatingAdd(int64((len(data)+routing.ToolResultErrorOverheadBytes(request, candidate.Family)+3)/4), textBaselineTokens(inlineTextBytes))
	if input < 1 {
		input = 1
	}
	if int64(len(data)) > int64(^uint64(0)>>1) {
		return 0, fmt.Errorf("request is too large to estimate")
	}
	return input, nil
}

// Media parts (images and documents, whether supplied by URL, inline bytes,
// or blob reference) are billed by the provider on their decoded content
// (pixels or pages), not on the bytes this worker serializes. A URL
// contributes only its string to the UTF-8 fallback, so without an explicit
// allowance an arbitrarily large remote document would be reserved at a few
// dozen tokens and bypass the admission cap. Inline bytes are excluded from
// the serialized-size estimate for the same reason: their base64 length is
// unrelated to the billed tokens. The fallback estimator therefore adds a
// conservative allowance per media part. These constants are reservation
// bounds, not billing rates; settlement still records actual usage.
const (
	// MediaImageInputTokenFloor bounds one image. Supported providers resize
	// images before tokenization: Anthropic's standard limit is about 1,600
	// tokens per image, OpenAI patch-based models about 2,500, and
	// high-resolution modes reach roughly 4,800. 6,000 keeps headroom above
	// the largest of these.
	MediaImageInputTokenFloor int64 = 6_000
	// MediaDocumentPageAssumption is the page count assumed for a document
	// whose length is unknown at admission (for example a URL). It is the
	// largest per-request PDF page limit of supported providers: Anthropic
	// accepts up to 600 pages on 1M-token-context models.
	MediaDocumentPageAssumption int64 = 600
	// MediaDocumentTextTokensPerPage bounds the extracted text of one PDF
	// page (typically 1,500-3,000 tokens for a dense page).
	MediaDocumentTextTokensPerPage int64 = 3_000
	// MediaDocumentTokensPerPage bounds one PDF page. Providers such as
	// Anthropic bill the extracted text plus a rendered image of every page,
	// so the page image is charged at the full per-image floor.
	MediaDocumentTokensPerPage = MediaDocumentTextTokensPerPage + MediaImageInputTokenFloor
	// MediaDocumentInputTokenFloor is the allowance for a document whose
	// content cannot be bounded at admission: a URL, a blob reference, or
	// inline bytes that are not plain text (see documentInputAllowance)
	// (600 pages x 9,000 tokens = 5,400,000 tokens). It deliberately exceeds
	// every supported context window: on a candidate that declares a context
	// window, addMediaAllowance caps it to the remaining input room, so a
	// document reserves the whole remaining window (the most the provider
	// can bill). Only a candidate without a declared window reserves the
	// full constant.
	MediaDocumentInputTokenFloor = MediaDocumentPageAssumption * MediaDocumentTokensPerPage
)

// mediaInputAllowance sums the per-part media allowance over every content
// location the adapters forward to a provider. Text-only requests return zero.
func mediaInputAllowance(request llm.Request) int64 {
	total := int64(0)
	add := func(parts []llm.Part) {
		for _, part := range parts {
			switch typed := part.(type) {
			case llm.ImagePart, *llm.ImagePart:
				total = saturatingAdd(total, MediaImageInputTokenFloor)
			case llm.DocumentPart:
				total = saturatingAdd(total, documentInputAllowance(typed))
			case *llm.DocumentPart:
				if typed != nil {
					total = saturatingAdd(total, documentInputAllowance(*typed))
				}
			}
		}
	}
	for _, instruction := range request.Instructions {
		add(instruction.Content)
	}
	for _, item := range request.Input {
		switch typed := item.(type) {
		case llm.Message:
			add(typed.Content)
		case *llm.Message:
			if typed != nil {
				add(typed.Content)
			}
		case llm.ToolResult:
			add(typed.Content)
		case *llm.ToolResult:
			if typed != nil {
				add(typed.Content)
			}
		}
	}
	return total
}

// documentInputAllowance bounds one document part beyond what estimateInput
// already counts for it. An inline text document is tokenized as text and a
// token covers at least one byte, so its decoded byte length bounds its
// tokens; estimateInput counts those bytes at the text baseline and the
// allowance reserves the remainder. Every other document keeps the
// unknown-size assumption, except an inline PDF whose page count can be
// proven from its bytes (inlinePDFPageBound): providers bill a rendered image
// for every page, and compressed page objects mean a small file can still
// declare the maximum page count, so the byte length alone does not bound it.
func documentInputAllowance(part llm.DocumentPart) int64 {
	if pages, ok := inlinePDFPageBound(part); ok {
		return saturatingMul(pages, MediaDocumentTokensPerPage, MediaDocumentInputTokenFloor)
	}
	if !inlineTextDocument(part) {
		return MediaDocumentInputTokenFloor
	}
	length := int64(len(part.Bytes))
	return length - textBaselineTokens(length)
}

// inlineTextDocument reports whether the part carries its own bytes and
// declares a text media type, ignoring case and parameters such as charset.
func inlineTextDocument(part llm.DocumentPart) bool {
	if part.Bytes == nil || part.URL != "" || part.Blob != nil {
		return false
	}
	mediaType, _, _ := strings.Cut(part.MediaType, ";")
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(mediaType)), "text/")
}

// textBaselineTokens is the bytes / 4 text baseline, rounded up.
func textBaselineTokens(length int64) int64 {
	return length/4 + (length%4+3)/4
}

// mediaBytesPlaceholder stands in for stripped inline media bytes. The codec
// rejects empty inline bytes, so one byte is kept; it adds at most one token
// per part to the text estimate, which only over-reserves.
var mediaBytesPlaceholder = []byte{0}

// withoutInlineMediaBytes returns a copy of the request whose inline image
// and document parts carry a one-byte placeholder instead of their bytes,
// plus the total decoded length of the inline text documents. The request
// itself is not modified.
func withoutInlineMediaBytes(request llm.Request) (llm.Request, int64) {
	textBytes := int64(0)
	strip := func(parts []llm.Part) []llm.Part {
		var stripped []llm.Part
		replace := func(index int, part llm.Part) {
			if stripped == nil {
				stripped = append([]llm.Part(nil), parts...)
			}
			stripped[index] = part
		}
		for index, part := range parts {
			switch typed := part.(type) {
			case llm.ImagePart:
				if len(typed.Bytes) > 0 {
					typed.Bytes = mediaBytesPlaceholder
					replace(index, typed)
				}
			case *llm.ImagePart:
				if typed != nil && len(typed.Bytes) > 0 {
					value := *typed
					value.Bytes = mediaBytesPlaceholder
					replace(index, value)
				}
			case llm.DocumentPart:
				if len(typed.Bytes) > 0 {
					if inlineTextDocument(typed) {
						textBytes = saturatingAdd(textBytes, int64(len(typed.Bytes)))
					}
					typed.Bytes = mediaBytesPlaceholder
					replace(index, typed)
				}
			case *llm.DocumentPart:
				if typed != nil && len(typed.Bytes) > 0 {
					value := *typed
					if inlineTextDocument(value) {
						textBytes = saturatingAdd(textBytes, int64(len(value.Bytes)))
					}
					value.Bytes = mediaBytesPlaceholder
					replace(index, value)
				}
			}
		}
		if stripped == nil {
			return parts
		}
		return stripped
	}
	instructions := append([]llm.Instruction(nil), request.Instructions...)
	for index := range instructions {
		instructions[index].Content = strip(instructions[index].Content)
	}
	input := append([]llm.Item(nil), request.Input...)
	for index, item := range input {
		switch typed := item.(type) {
		case llm.Message:
			typed.Content = strip(typed.Content)
			input[index] = typed
		case *llm.Message:
			if typed != nil {
				value := *typed
				value.Content = strip(value.Content)
				input[index] = &value
			}
		case llm.ToolResult:
			typed.Content = strip(typed.Content)
			input[index] = typed
		case *llm.ToolResult:
			if typed != nil {
				value := *typed
				value.Content = strip(value.Content)
				input[index] = &value
			}
		}
	}
	request.Instructions, request.Input = instructions, input
	return request, textBytes
}

// addMediaAllowance adds the media allowance to the serialized-size estimate.
// When the candidate declares a context window, the allowance is capped at the
// room left after the text estimate and the output cap (which contains any
// reasoning): the provider must reject input beyond its window, so billable media input cannot
// exceed that room, and the allowance alone never turns an admissible request
// into a context-limit rejection.
func addMediaAllowance(input, allowance, contextTokens, output int64) int64 {
	if allowance <= 0 {
		return input
	}
	if contextTokens > 0 {
		room := contextTokens
		for _, used := range []int64{input, output} {
			if used >= room {
				return input
			}
			room -= used
		}
		if allowance > room {
			allowance = room
		}
	}
	return saturatingAdd(input, allowance)
}

func saturatingAdd(left, right int64) int64 {
	if right > 0 && left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func multiplyCeil(value pricing.MicroUSD, ratio *big.Rat) (pricing.MicroUSD, error) {
	if value < 0 || ratio == nil {
		return 0, fmt.Errorf("invalid estimate multiplier")
	}
	numerator := new(big.Int).Mul(big.NewInt(int64(value)), ratio.Num())
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(numerator, ratio.Denom(), remainder)
	if remainder.Sign() != 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if !quotient.IsInt64() {
		return 0, fmt.Errorf("estimate multiplier overflows int64")
	}
	result := pricing.MicroUSD(quotient.Int64())
	if !result.Valid() {
		return 0, fmt.Errorf("estimate exceeds safe range")
	}
	return result, nil
}

func multiplyUSD(value pricing.USD, ratio *big.Rat) (pricing.USD, error) {
	if ratio == nil {
		return pricing.USD{}, fmt.Errorf("invalid estimate multiplier")
	}
	return value.MulRatio(ratio.Num(), ratio.Denom())
}

func mustRequestJSON(request llm.Request) []byte {
	data, _ := json.Marshal(request)
	return data
}

// inlinePDFPageBound returns an upper bound on the page count of an inline
// PDF when one can be proven from its bytes. Every page is a dictionary whose
// /Type is /Page; without object streams (/ObjStm), which can compress those
// dictionaries, each one appears in the file as written, so counting them
// bounds the pages (incremental updates only over-count). Anything that could
// hide a page dictionary, such as an object stream or a /Type name written
// with # escapes, or a file with no visible page, gives no bound and keeps
// the unknown-size allowance.
func inlinePDFPageBound(part llm.DocumentPart) (int64, bool) {
	if part.Bytes == nil || part.URL != "" || part.Blob != nil {
		return 0, false
	}
	mediaType, _, _ := strings.Cut(part.MediaType, ";")
	if !strings.EqualFold(strings.TrimSpace(mediaType), "application/pdf") {
		return 0, false
	}
	data := part.Bytes
	if !bytes.HasPrefix(data, []byte("%PDF-")) || bytes.Contains(data, []byte("/ObjStm")) || pdfHasEscapedName(data) {
		return 0, false
	}
	pages := int64(0)
	for offset := 0; ; {
		index := bytes.Index(data[offset:], []byte("/Type"))
		if index < 0 {
			break
		}
		at := offset + index + len("/Type")
		offset = at
		if at < len(data) && !pdfDelimiterOrSpace(data[at]) {
			continue // a longer name such as /TypeX
		}
		at = skipPDFSpaceAndComments(data, at)
		if at >= len(data) || data[at] != '/' {
			continue
		}
		end := at + 1
		for end < len(data) && !pdfDelimiterOrSpace(data[end]) {
			end++
		}
		if string(data[at+1:end]) == "Page" {
			pages++
		}
	}
	if pages == 0 {
		return 0, false
	}
	return pages, true
}

func pdfDelimiterOrSpace(value byte) bool {
	switch value {
	case 0, '\t', '\n', '\f', '\r', ' ', '(', ')', '<', '>', '[', ']', '{', '}', '/', '%':
		return true
	}
	return false
}

func skipPDFSpaceAndComments(data []byte, at int) int {
	for at < len(data) {
		switch data[at] {
		case 0, '\t', '\n', '\f', '\r', ' ':
			at++
		case '%':
			for at < len(data) && data[at] != '\n' && data[at] != '\r' {
				at++
			}
		default:
			return at
		}
	}
	return at
}

// saturatingMul multiplies non-negative values, returning limit when the
// product would exceed it.
func saturatingMul(left, right, limit int64) int64 {
	if left <= 0 || right <= 0 {
		return 0
	}
	if left > limit/right {
		return limit
	}
	if product := left * right; product < limit {
		return product
	}
	return limit
}

// pdfHasEscapedName reports whether any PDF name uses a # escape. An escape
// can spell any key or value (/Ty#70e is /Type, /Obj#53tm is /ObjStm), so a
// file that uses one gives no provable page bound.
func pdfHasEscapedName(data []byte) bool {
	for index := 0; index < len(data); index++ {
		if data[index] != '/' {
			continue
		}
		for end := index + 1; end < len(data) && !pdfDelimiterOrSpace(data[end]); end++ {
			if data[end] == '#' {
				return true
			}
		}
	}
	return false
}
