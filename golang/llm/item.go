package llm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

type ItemKind string

const (
	ItemKindMessage       ItemKind = "message"
	ItemKindToolCall      ItemKind = "tool_call"
	ItemKindToolResult    ItemKind = "tool_result"
	ItemKindProviderState ItemKind = "provider_state"
	ItemKindReference     ItemKind = "reference"
)

// Item is an ordered semantic interaction. Provider role names are an adapter
// concern; tools and continuation state are not disguised as messages.
type Item interface {
	item()
	ItemKind() ItemKind
}

type Actor string

const (
	ActorHuman Actor = "human"
	ActorModel Actor = "model"
)

func (actor Actor) Valid() bool {
	return actor == ActorHuman || actor == ActorModel
}

type Message struct {
	Actor   Actor
	Content []Part
}

func (Message) item()              {}
func (Message) ItemKind() ItemKind { return ItemKindMessage }

func (message Message) MarshalJSON() ([]byte, error) {
	if !message.Actor.Valid() {
		return nil, fmt.Errorf("message actor %q is invalid", message.Actor)
	}
	content := message.Content
	if content == nil {
		content = []Part{}
	}
	return marshalObject(map[string]any{
		"kind":    ItemKindMessage,
		"actor":   message.Actor,
		"content": content,
	})
}

func decodeMessage(data []byte) (Message, error) {
	fields, err := decodeVerifiedObject(data)
	if err != nil {
		return Message{}, err
	}
	if err := checkUnknownFields(fields, "kind", "actor", "content"); err != nil {
		return Message{}, err
	}
	kind, err := requiredString(fields, "kind")
	if err != nil {
		return Message{}, err
	}
	if ItemKind(kind) != ItemKindMessage {
		return Message{}, fmt.Errorf("item kind %q is not %q", kind, ItemKindMessage)
	}
	actorValue, err := requiredString(fields, "actor")
	if err != nil {
		return Message{}, err
	}
	actor := Actor(actorValue)
	if !actor.Valid() {
		return Message{}, fmt.Errorf("message actor %q is invalid", actor)
	}
	contentRaw, err := requireField(fields, "content")
	if err != nil {
		return Message{}, err
	}
	content, err := decodeParts(contentRaw)
	if err != nil {
		return Message{}, fmt.Errorf("message content: %w", err)
	}
	return Message{Actor: actor, Content: content}, nil
}

type ToolCall struct {
	ID        string
	Name      string
	Arguments json.RawMessage
}

func (ToolCall) item()              {}
func (ToolCall) ItemKind() ItemKind { return ItemKindToolCall }

func (call ToolCall) MarshalJSON() ([]byte, error) {
	if call.ID == "" {
		return nil, errorsForField("tool call", "id")
	}
	if err := validateToolName(call.Name); err != nil {
		return nil, err
	}
	if !validRawJSON(call.Arguments) {
		return nil, fmt.Errorf("tool call %q arguments must be valid JSON", call.ID)
	}
	return marshalObject(map[string]any{
		"kind":      ItemKindToolCall,
		"id":        call.ID,
		"name":      call.Name,
		"arguments": copyRaw(call.Arguments),
	})
}

func decodeToolCall(data []byte) (ToolCall, error) {
	fields, err := decodeVerifiedObject(data)
	if err != nil {
		return ToolCall{}, err
	}
	if err := checkUnknownFields(fields, "kind", "id", "name", "arguments"); err != nil {
		return ToolCall{}, err
	}
	kind, err := requiredString(fields, "kind")
	if err != nil {
		return ToolCall{}, err
	}
	if ItemKind(kind) != ItemKindToolCall {
		return ToolCall{}, fmt.Errorf("item kind %q is not %q", kind, ItemKindToolCall)
	}
	id, err := requiredString(fields, "id")
	if err != nil {
		return ToolCall{}, err
	}
	name, err := requiredString(fields, "name")
	if err != nil {
		return ToolCall{}, err
	}
	if err := validateToolName(name); err != nil {
		return ToolCall{}, err
	}
	arguments, err := requireField(fields, "arguments")
	if err != nil {
		return ToolCall{}, err
	}
	if !validRawJSON(arguments) {
		return ToolCall{}, fmt.Errorf("tool call %q arguments must be valid JSON", id)
	}
	return ToolCall{ID: id, Name: name, Arguments: copyRaw(arguments)}, nil
}

// ToolResultErrorTextPrefix starts the tool output text that endpoint
// families without a tool-result error field send for a result with
// IsError set. It lives here so admission can count it without importing
// the provider packages.
const ToolResultErrorTextPrefix = "[is_error=true] The tool call failed; its output follows.\n"

type ToolResult struct {
	CallID  string
	Name    string
	Content []Part
	IsError bool
}

func (ToolResult) item()              {}
func (ToolResult) ItemKind() ItemKind { return ItemKindToolResult }

func (result ToolResult) MarshalJSON() ([]byte, error) {
	if result.CallID == "" {
		return nil, errorsForField("tool result", "call_id")
	}
	content := result.Content
	if content == nil {
		content = []Part{}
	}
	fields := map[string]any{
		"kind":     ItemKindToolResult,
		"call_id":  result.CallID,
		"content":  content,
		"is_error": result.IsError,
	}
	if result.Name != "" {
		fields["name"] = result.Name
	}
	return marshalObject(fields)
}

func decodeToolResult(data []byte) (ToolResult, error) {
	fields, err := decodeVerifiedObject(data)
	if err != nil {
		return ToolResult{}, err
	}
	if err := checkUnknownFields(fields, "kind", "call_id", "name", "content", "is_error"); err != nil {
		return ToolResult{}, err
	}
	kind, err := requiredString(fields, "kind")
	if err != nil {
		return ToolResult{}, err
	}
	if ItemKind(kind) != ItemKindToolResult {
		return ToolResult{}, fmt.Errorf("item kind %q is not %q", kind, ItemKindToolResult)
	}
	callID, err := requiredString(fields, "call_id")
	if err != nil {
		return ToolResult{}, err
	}
	name, _, err := optionalString(fields, "name")
	if err != nil {
		return ToolResult{}, err
	}
	contentRaw, err := requireField(fields, "content")
	if err != nil {
		return ToolResult{}, err
	}
	content, err := decodeParts(contentRaw)
	if err != nil {
		return ToolResult{}, fmt.Errorf("tool result %q content: %w", callID, err)
	}
	isError, _, err := optionalBool(fields, "is_error")
	if err != nil {
		return ToolResult{}, err
	}
	return ToolResult{CallID: callID, Name: name, Content: content, IsError: isError}, nil
}

type ProviderState struct {
	Provider       string
	EndpointFamily string
	MediaType      string
	Opaque         []byte
}

func (ProviderState) item()              {}
func (ProviderState) ItemKind() ItemKind { return ItemKindProviderState }

func (state ProviderState) MarshalJSON() ([]byte, error) {
	if state.Provider == "" || state.EndpointFamily == "" || state.MediaType == "" {
		return nil, fmt.Errorf("provider state requires provider, endpoint_family, and media_type")
	}
	return marshalObject(map[string]any{
		"kind":            ItemKindProviderState,
		"provider":        state.Provider,
		"endpoint_family": state.EndpointFamily,
		"media_type":      state.MediaType,
		"opaque":          copyBytes(state.Opaque),
	})
}

func decodeProviderState(data []byte) (ProviderState, error) {
	fields, err := decodeVerifiedObject(data)
	if err != nil {
		return ProviderState{}, err
	}
	if err := checkUnknownFields(fields, "kind", "provider", "endpoint_family", "media_type", "opaque"); err != nil {
		return ProviderState{}, err
	}
	kind, err := requiredString(fields, "kind")
	if err != nil {
		return ProviderState{}, err
	}
	if ItemKind(kind) != ItemKindProviderState {
		return ProviderState{}, fmt.Errorf("item kind %q is not %q", kind, ItemKindProviderState)
	}
	provider, err := requiredString(fields, "provider")
	if err != nil {
		return ProviderState{}, err
	}
	family, err := requiredString(fields, "endpoint_family")
	if err != nil {
		return ProviderState{}, err
	}
	mediaType, err := requiredString(fields, "media_type")
	if err != nil {
		return ProviderState{}, err
	}
	opaqueRaw, err := requireField(fields, "opaque")
	if err != nil {
		return ProviderState{}, err
	}
	opaque, err := decodeBytesJSON(opaqueRaw)
	if err != nil {
		return ProviderState{}, fmt.Errorf("provider state opaque: %w", err)
	}
	return ProviderState{
		Provider:       provider,
		EndpointFamily: family,
		MediaType:      mediaType,
		Opaque:         copyBytes(opaque),
	}, nil
}

type Reference struct {
	URI      string
	Metadata map[string]json.RawMessage
}

func (Reference) item()              {}
func (Reference) ItemKind() ItemKind { return ItemKindReference }

func (reference Reference) MarshalJSON() ([]byte, error) {
	if err := validateURI(reference.URI); err != nil {
		return nil, err
	}
	metadata := make(map[string]json.RawMessage, len(reference.Metadata))
	for key, value := range reference.Metadata {
		if key == "" || !validRawJSON(value) {
			return nil, fmt.Errorf("reference metadata %q must contain valid JSON", key)
		}
		metadata[key] = copyRaw(value)
	}
	fields := map[string]any{"kind": ItemKindReference, "uri": reference.URI}
	if len(metadata) > 0 {
		fields["metadata"] = metadata
	}
	return marshalObject(fields)
}

func decodeReference(data []byte) (Reference, error) {
	fields, err := decodeVerifiedObject(data)
	if err != nil {
		return Reference{}, err
	}
	if err := checkUnknownFields(fields, "kind", "uri", "metadata"); err != nil {
		return Reference{}, err
	}
	kind, err := requiredString(fields, "kind")
	if err != nil {
		return Reference{}, err
	}
	if ItemKind(kind) != ItemKindReference {
		return Reference{}, fmt.Errorf("item kind %q is not %q", kind, ItemKindReference)
	}
	uri, err := requiredString(fields, "uri")
	if err != nil {
		return Reference{}, err
	}
	if err := validateURI(uri); err != nil {
		return Reference{}, err
	}
	var metadata map[string]json.RawMessage
	if value, ok := fields["metadata"]; ok {
		if err := decodeJSON(value, &metadata); err != nil {
			return Reference{}, fmt.Errorf("reference metadata: %w", err)
		}
		for key, value := range metadata {
			if key == "" || !validRawJSON(value) {
				return Reference{}, fmt.Errorf("reference metadata %q must contain valid JSON", key)
			}
			metadata[key] = copyRaw(value)
		}
	}
	return Reference{URI: uri, Metadata: metadata}, nil
}

// decodeItem and the item, part and blob decoders it dispatches to decode
// sub-values of a document whose entry point (DecodeItems or an UnmarshalJSON
// method) already rejected duplicate keys at every depth, so they split
// objects with decodeVerifiedObject instead of rescanning each subtree.
func decodeItem(data []byte) (Item, error) {
	fields, err := decodeVerifiedObject(data)
	if err != nil {
		return nil, err
	}
	kind, err := requiredString(fields, "kind")
	if err != nil {
		return nil, err
	}
	switch ItemKind(kind) {
	case ItemKindMessage:
		return decodeMessage(data)
	case ItemKindToolCall:
		return decodeToolCall(data)
	case ItemKindToolResult:
		return decodeToolResult(data)
	case ItemKindProviderState:
		return decodeProviderState(data)
	case ItemKindReference:
		return decodeReference(data)
	default:
		return nil, fmt.Errorf("unknown item kind %q", kind)
	}
}

func decodeItems(data []byte) ([]Item, error) {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil, fmt.Errorf("items must be an array")
	}
	values, err := decodeVerifiedArray(data)
	if err != nil {
		return nil, err
	}
	items := make([]Item, 0, len(values))
	for index, value := range values {
		item, err := decodeItem(value)
		if err != nil {
			return nil, fmt.Errorf("item %d: %w", index, err)
		}
		items = append(items, item)
	}
	return items, nil
}

// DecodeItems decodes a JSON item array through the canonical kind-dispatch
// path used by requests and responses.
func DecodeItems(data []byte) ([]Item, error) {
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return nil, err
	}
	return decodeItems(data)
}

type PartKind string

const (
	PartKindText          PartKind = "text"
	PartKindImage         PartKind = "image"
	PartKindDocument      PartKind = "document"
	PartKindJSON          PartKind = "json"
	PartKindRefusal       PartKind = "refusal"
	PartKindProviderState PartKind = "provider_state"
)

type Part interface {
	part()
	PartKind() PartKind
}

type TextPart struct {
	Text string
}

func (TextPart) part()              {}
func (TextPart) PartKind() PartKind { return PartKindText }

func (part TextPart) MarshalJSON() ([]byte, error) {
	return marshalObject(map[string]any{"kind": PartKindText, "text": part.Text})
}

func decodeTextPart(data []byte) (TextPart, error) {
	fields, err := decodeVerifiedObject(data)
	if err != nil {
		return TextPart{}, err
	}
	if err := checkUnknownFields(fields, "kind", "text"); err != nil {
		return TextPart{}, err
	}
	kind, err := requiredString(fields, "kind")
	if err != nil {
		return TextPart{}, err
	}
	if PartKind(kind) != PartKindText {
		return TextPart{}, fmt.Errorf("part kind %q is not %q", kind, PartKindText)
	}
	text, err := requiredString(fields, "text")
	if err != nil {
		return TextPart{}, err
	}
	return TextPart{Text: text}, nil
}

type BlobRef struct {
	Digest     string
	ByteLength int64
	MediaType  string
	Locator    string
}

func (blob BlobRef) MarshalJSON() ([]byte, error) {
	if blob.Digest == "" || blob.ByteLength < 0 || blob.MediaType == "" || blob.Locator == "" {
		return nil, errorsForField("blob", "digest, byte_length, media_type, and locator")
	}
	return marshalObject(map[string]any{
		"digest":      blob.Digest,
		"byte_length": blob.ByteLength,
		"media_type":  blob.MediaType,
		"locator":     blob.Locator,
	})
}

func decodeBlobRef(data []byte) (*BlobRef, error) {
	fields, err := decodeVerifiedObject(data)
	if err != nil {
		return nil, err
	}
	if err := checkUnknownFields(fields, "digest", "byte_length", "media_type", "locator"); err != nil {
		return nil, err
	}
	digest, err := requiredString(fields, "digest")
	if err != nil {
		return nil, err
	}
	byteLength, err := requiredInt64(fields, "byte_length")
	if err != nil {
		return nil, err
	}
	mediaType, err := requiredString(fields, "media_type")
	if err != nil {
		return nil, err
	}
	locator, err := requiredString(fields, "locator")
	if err != nil {
		return nil, err
	}
	blob := &BlobRef{Digest: digest, ByteLength: byteLength, MediaType: mediaType, Locator: locator}
	if blob.ByteLength < 0 {
		return nil, fmt.Errorf("blob byte_length must not be negative")
	}
	return blob, nil
}

type ImagePart struct {
	URL       string
	Bytes     []byte
	Blob      *BlobRef
	MediaType string
	Detail    string
}

func (ImagePart) part()              {}
func (ImagePart) PartKind() PartKind { return PartKindImage }

func (part ImagePart) MarshalJSON() ([]byte, error) {
	if err := validateMediaSource(part.URL, part.Bytes, part.Blob, part.MediaType, "image"); err != nil {
		return nil, err
	}
	fields := map[string]any{"kind": PartKindImage, "media_type": part.MediaType}
	if part.URL != "" {
		fields["url"] = part.URL
	}
	if part.Bytes != nil {
		fields["bytes"] = copyBytes(part.Bytes)
	}
	if part.Blob != nil {
		fields["blob"] = part.Blob
	}
	if part.Detail != "" {
		fields["detail"] = part.Detail
	}
	return marshalObject(fields)
}

func decodeImagePart(data []byte) (ImagePart, error) {
	fields, err := decodeVerifiedObject(data)
	if err != nil {
		return ImagePart{}, err
	}
	if err := checkUnknownFields(fields, "kind", "url", "bytes", "blob", "media_type", "detail"); err != nil {
		return ImagePart{}, err
	}
	kind, err := requiredString(fields, "kind")
	if err != nil {
		return ImagePart{}, err
	}
	if PartKind(kind) != PartKindImage {
		return ImagePart{}, fmt.Errorf("part kind %q is not %q", kind, PartKindImage)
	}
	urlValue, _, err := optionalString(fields, "url")
	if err != nil {
		return ImagePart{}, err
	}
	var bytesValue []byte
	if raw, ok := fields["bytes"]; ok {
		bytesValue, err = decodeBytesJSON(raw)
		if err != nil {
			return ImagePart{}, fmt.Errorf("image bytes: %w", err)
		}
	}
	var blob *BlobRef
	if raw, ok := fields["blob"]; ok {
		blob, err = decodeBlobRef(raw)
		if err != nil {
			return ImagePart{}, fmt.Errorf("image blob: %w", err)
		}
	}
	mediaType, err := requiredString(fields, "media_type")
	if err != nil {
		return ImagePart{}, err
	}
	detail, _, err := optionalString(fields, "detail")
	if err != nil {
		return ImagePart{}, err
	}
	part := ImagePart{URL: urlValue, Bytes: copyBytes(bytesValue), Blob: blob, MediaType: mediaType, Detail: detail}
	if err := validateMediaSource(part.URL, part.Bytes, part.Blob, part.MediaType, "image"); err != nil {
		return ImagePart{}, err
	}
	return part, nil
}

type DocumentPart struct {
	URL       string
	Bytes     []byte
	Blob      *BlobRef
	MediaType string
	Title     string
}

func (DocumentPart) part()              {}
func (DocumentPart) PartKind() PartKind { return PartKindDocument }

func (part DocumentPart) MarshalJSON() ([]byte, error) {
	if err := validateMediaSource(part.URL, part.Bytes, part.Blob, part.MediaType, "document"); err != nil {
		return nil, err
	}
	fields := map[string]any{"kind": PartKindDocument, "media_type": part.MediaType}
	if part.URL != "" {
		fields["url"] = part.URL
	}
	if part.Bytes != nil {
		fields["bytes"] = copyBytes(part.Bytes)
	}
	if part.Blob != nil {
		fields["blob"] = part.Blob
	}
	if part.Title != "" {
		fields["title"] = part.Title
	}
	return marshalObject(fields)
}

func decodeDocumentPart(data []byte) (DocumentPart, error) {
	fields, err := decodeVerifiedObject(data)
	if err != nil {
		return DocumentPart{}, err
	}
	if err := checkUnknownFields(fields, "kind", "url", "bytes", "blob", "media_type", "title"); err != nil {
		return DocumentPart{}, err
	}
	kind, err := requiredString(fields, "kind")
	if err != nil {
		return DocumentPart{}, err
	}
	if PartKind(kind) != PartKindDocument {
		return DocumentPart{}, fmt.Errorf("part kind %q is not %q", kind, PartKindDocument)
	}
	urlValue, _, err := optionalString(fields, "url")
	if err != nil {
		return DocumentPart{}, err
	}
	var bytesValue []byte
	if raw, ok := fields["bytes"]; ok {
		bytesValue, err = decodeBytesJSON(raw)
		if err != nil {
			return DocumentPart{}, fmt.Errorf("document bytes: %w", err)
		}
	}
	var blob *BlobRef
	if raw, ok := fields["blob"]; ok {
		blob, err = decodeBlobRef(raw)
		if err != nil {
			return DocumentPart{}, fmt.Errorf("document blob: %w", err)
		}
	}
	mediaType, err := requiredString(fields, "media_type")
	if err != nil {
		return DocumentPart{}, err
	}
	title, _, err := optionalString(fields, "title")
	if err != nil {
		return DocumentPart{}, err
	}
	part := DocumentPart{URL: urlValue, Bytes: copyBytes(bytesValue), Blob: blob, MediaType: mediaType, Title: title}
	if err := validateMediaSource(part.URL, part.Bytes, part.Blob, part.MediaType, "document"); err != nil {
		return DocumentPart{}, err
	}
	return part, nil
}

type JSONPart struct {
	Value json.RawMessage
}

func (JSONPart) part()              {}
func (JSONPart) PartKind() PartKind { return PartKindJSON }

func (part JSONPart) MarshalJSON() ([]byte, error) {
	if !validRawJSON(part.Value) {
		return nil, errorsForField("json part", "value")
	}
	return marshalObject(map[string]any{"kind": PartKindJSON, "value": copyRaw(part.Value)})
}

func decodeJSONPart(data []byte) (JSONPart, error) {
	fields, err := decodeVerifiedObject(data)
	if err != nil {
		return JSONPart{}, err
	}
	if err := checkUnknownFields(fields, "kind", "value"); err != nil {
		return JSONPart{}, err
	}
	kind, err := requiredString(fields, "kind")
	if err != nil {
		return JSONPart{}, err
	}
	if PartKind(kind) != PartKindJSON {
		return JSONPart{}, fmt.Errorf("part kind %q is not %q", kind, PartKindJSON)
	}
	value, err := requireField(fields, "value")
	if err != nil {
		return JSONPart{}, err
	}
	if !validRawJSON(value) {
		return JSONPart{}, errorsForField("json part", "value")
	}
	return JSONPart{Value: copyRaw(value)}, nil
}

type RefusalPart struct {
	Text         string
	ProviderCode string
}

func (RefusalPart) part()              {}
func (RefusalPart) PartKind() PartKind { return PartKindRefusal }

func (part RefusalPart) MarshalJSON() ([]byte, error) {
	fields := map[string]any{"kind": PartKindRefusal, "text": part.Text}
	if part.ProviderCode != "" {
		fields["provider_code"] = part.ProviderCode
	}
	return marshalObject(fields)
}

func decodeRefusalPart(data []byte) (RefusalPart, error) {
	fields, err := decodeVerifiedObject(data)
	if err != nil {
		return RefusalPart{}, err
	}
	if err := checkUnknownFields(fields, "kind", "text", "provider_code"); err != nil {
		return RefusalPart{}, err
	}
	kind, err := requiredString(fields, "kind")
	if err != nil {
		return RefusalPart{}, err
	}
	if PartKind(kind) != PartKindRefusal {
		return RefusalPart{}, fmt.Errorf("part kind %q is not %q", kind, PartKindRefusal)
	}
	text, err := requiredString(fields, "text")
	if err != nil {
		return RefusalPart{}, err
	}
	providerCode, _, err := optionalString(fields, "provider_code")
	if err != nil {
		return RefusalPart{}, err
	}
	return RefusalPart{Text: text, ProviderCode: providerCode}, nil
}

type ProviderStatePart struct {
	Provider       string
	EndpointFamily string
	MediaType      string
	Opaque         []byte
}

func (ProviderStatePart) part()              {}
func (ProviderStatePart) PartKind() PartKind { return PartKindProviderState }

func (part ProviderStatePart) MarshalJSON() ([]byte, error) {
	if part.Provider == "" || part.EndpointFamily == "" || part.MediaType == "" {
		return nil, fmt.Errorf("provider state part requires provider, endpoint_family, and media_type")
	}
	return marshalObject(map[string]any{
		"kind":            PartKindProviderState,
		"provider":        part.Provider,
		"endpoint_family": part.EndpointFamily,
		"media_type":      part.MediaType,
		"opaque":          copyBytes(part.Opaque),
	})
}

func decodeProviderStatePart(data []byte) (ProviderStatePart, error) {
	fields, err := decodeVerifiedObject(data)
	if err != nil {
		return ProviderStatePart{}, err
	}
	if err := checkUnknownFields(fields, "kind", "provider", "endpoint_family", "media_type", "opaque"); err != nil {
		return ProviderStatePart{}, err
	}
	kind, err := requiredString(fields, "kind")
	if err != nil {
		return ProviderStatePart{}, err
	}
	if PartKind(kind) != PartKindProviderState {
		return ProviderStatePart{}, fmt.Errorf("part kind %q is not %q", kind, PartKindProviderState)
	}
	provider, err := requiredString(fields, "provider")
	if err != nil {
		return ProviderStatePart{}, err
	}
	family, err := requiredString(fields, "endpoint_family")
	if err != nil {
		return ProviderStatePart{}, err
	}
	mediaType, err := requiredString(fields, "media_type")
	if err != nil {
		return ProviderStatePart{}, err
	}
	opaqueRaw, err := requireField(fields, "opaque")
	if err != nil {
		return ProviderStatePart{}, err
	}
	opaque, err := decodeBytesJSON(opaqueRaw)
	if err != nil {
		return ProviderStatePart{}, fmt.Errorf("provider state part opaque: %w", err)
	}
	return ProviderStatePart{Provider: provider, EndpointFamily: family, MediaType: mediaType, Opaque: copyBytes(opaque)}, nil
}

func decodePart(data []byte) (Part, error) {
	fields, err := decodeVerifiedObject(data)
	if err != nil {
		return nil, err
	}
	kind, err := requiredString(fields, "kind")
	if err != nil {
		return nil, err
	}
	switch PartKind(kind) {
	case PartKindText:
		return decodeTextPart(data)
	case PartKindImage:
		return decodeImagePart(data)
	case PartKindDocument:
		return decodeDocumentPart(data)
	case PartKindJSON:
		return decodeJSONPart(data)
	case PartKindRefusal:
		return decodeRefusalPart(data)
	case PartKindProviderState:
		return decodeProviderStatePart(data)
	default:
		return nil, fmt.Errorf("unknown part kind %q", kind)
	}
}

func decodeParts(data []byte) ([]Part, error) {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil, fmt.Errorf("parts must be an array")
	}
	values, err := decodeVerifiedArray(data)
	if err != nil {
		return nil, err
	}
	parts := make([]Part, 0, len(values))
	for index, value := range values {
		part, err := decodePart(value)
		if err != nil {
			return nil, fmt.Errorf("part %d: %w", index, err)
		}
		parts = append(parts, part)
	}
	return parts, nil
}

func validateToolName(name string) error {
	if len(name) == 0 || len(name) > 64 {
		return fmt.Errorf("tool name %q must contain 1 to 64 ASCII characters", name)
	}
	for _, char := range name {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_' || char == '-' {
			continue
		}
		return fmt.Errorf("tool name %q contains invalid character %q", name, char)
	}
	return nil
}

func validateMediaSource(rawURL string, data []byte, blob *BlobRef, mediaType, kind string) error {
	sources := 0
	if rawURL != "" {
		sources++
		// Only the structural check runs here because this also decodes stored
		// transcripts. The remote-media host policy is a request ingress check
		// (ValidateMediaURLs); applying it here would make checkpoints written
		// under an earlier policy undecodable.
		if err := validateURI(rawURL); err != nil {
			return fmt.Errorf("%s url: %w", kind, err)
		}
	}
	if data != nil {
		sources++
	}
	if blob != nil {
		sources++
	}
	if sources != 1 {
		return fmt.Errorf("%s must specify exactly one of url, bytes, or blob", kind)
	}
	if mediaType == "" || strings.ContainsAny(mediaType, "\r\n") {
		return fmt.Errorf("%s media_type must be a non-empty MIME value", kind)
	}
	if blob != nil && blob.MediaType != mediaType {
		return fmt.Errorf("%s media_type must match blob media_type", kind)
	}
	return nil
}

func validateURI(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" {
		return fmt.Errorf("URI %q must include a scheme", raw)
	}
	if parsed.Scheme == "javascript" || parsed.Scheme == "data" {
		return fmt.Errorf("URI scheme %q is not allowed", parsed.Scheme)
	}
	return nil
}

func requiredInt64(fields map[string]json.RawMessage, key string) (int64, error) {
	value, err := requireField(fields, key)
	if err != nil {
		return 0, err
	}
	result, err := decodeIntJSON[int64](value)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return result, nil
}

func errorsForField(prefix, field string) error {
	return fmt.Errorf("%s requires %s", prefix, field)
}
