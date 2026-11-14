package llm

import (
	"encoding/json"
	"errors"
	"strconv"
	"testing"
)

func decodeTestInt(data []byte) (int, error) {
	return strconv.Atoi(string(data))
}

func TestDecodeOptionalAndNullableFields(t *testing.T) {
	fields := map[string]json.RawMessage{"count": json.RawMessage(`7`), "none": json.RawMessage(`null`), "bad": json.RawMessage(`x`)}
	value := 1
	if err := decodeOptionalField(fields, "count", &value, decodeTestInt); err != nil || value != 7 {
		t.Fatalf("present field = %d, %v", value, err)
	}
	if err := decodeOptionalField(fields, "absent", &value, decodeTestInt); err != nil || value != 7 {
		t.Fatalf("absent field = %d, %v", value, err)
	}
	if err := decodeOptionalField(fields, "none", &value, decodeTestInt); err == nil {
		t.Fatal("decodeOptionalField accepted null for a non-nullable decoder")
	}
	if err := decodeNullableField(fields, "none", &value, decodeTestInt); err != nil || value != 7 {
		t.Fatalf("null field = %d, %v", value, err)
	}
	err := decodeNullableField(fields, "bad", &value, decodeTestInt)
	var numErr *strconv.NumError
	if err == nil || !errors.As(err, &numErr) || err.Error()[:5] != "bad: " {
		t.Fatalf("bad field error = %v", err)
	}
}

func TestDecodeEachNamesTheFailingElement(t *testing.T) {
	got, err := decodeEach([]json.RawMessage{json.RawMessage(`1`), json.RawMessage(`2`)}, "number", decodeTestInt)
	if err != nil || len(got) != 2 || got[1] != 2 {
		t.Fatalf("decodeEach = %v, %v", got, err)
	}
	if got, err := decodeEach(nil, "number", decodeTestInt); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty decodeEach = %#v, %v", got, err)
	}
	if _, err := decodeEach([]json.RawMessage{json.RawMessage(`1`), json.RawMessage(`x`)}, "number", decodeTestInt); err == nil || err.Error()[:9] != "number 1:" {
		t.Fatalf("decodeEach error = %v", err)
	}
}

func TestProviderStateItemAndPartKeepTheirLabels(t *testing.T) {
	if _, err := (ProviderState{}).MarshalJSON(); err == nil || err.Error() != "provider state requires provider, endpoint_family, and media_type" {
		t.Fatalf("item marshal error = %v", err)
	}
	if _, err := (ProviderStatePart{}).MarshalJSON(); err == nil || err.Error() != "provider state part requires provider, endpoint_family, and media_type" {
		t.Fatalf("part marshal error = %v", err)
	}
	if _, err := decodeProviderState([]byte(`{"kind":"provider_state","provider":"p","endpoint_family":"f","media_type":"m","opaque":7}`)); err == nil || err.Error()[:21] != "provider state opaque" {
		t.Fatalf("item opaque error = %v", err)
	}
	if _, err := decodeProviderStatePart([]byte(`{"kind":"provider_state","provider":"p","endpoint_family":"f","media_type":"m","opaque":7}`)); err == nil || err.Error()[:26] != "provider state part opaque" {
		t.Fatalf("part opaque error = %v", err)
	}
	data, err := (ProviderStatePart{Provider: "p", EndpointFamily: "f", MediaType: "m", Opaque: []byte("x")}).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	part, err := decodeProviderStatePart(data)
	if err != nil || part.Provider != "p" || string(part.Opaque) != "x" {
		t.Fatalf("part round trip = %+v, %v", part, err)
	}
}
