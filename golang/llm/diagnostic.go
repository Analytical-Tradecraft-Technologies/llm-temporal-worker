package llm

import (
	"bytes"
	"fmt"
)

type DiagnosticSeverity string

const (
	DiagnosticInfo    DiagnosticSeverity = "info"
	DiagnosticWarning DiagnosticSeverity = "warning"
	DiagnosticError   DiagnosticSeverity = "error"
)

type Diagnostic struct {
	Code     string
	Severity DiagnosticSeverity
	Path     string
	Message  string
	Details  map[string]string
}

func (diagnostic Diagnostic) MarshalJSON() ([]byte, error) {
	if diagnostic.Code == "" {
		return nil, fmt.Errorf("diagnostic code must not be empty")
	}
	severity, err := defaultEnum(diagnostic.Severity, DiagnosticInfo, "diagnostic severity", diagnosticSeverities...)
	if err != nil {
		return nil, err
	}
	if diagnostic.Message == "" {
		return nil, fmt.Errorf("diagnostic message must not be empty")
	}
	fields := map[string]any{
		"code":     diagnostic.Code,
		"severity": severity,
		"message":  diagnostic.Message,
	}
	setIfNotEmpty(fields, "path", diagnostic.Path)
	if len(diagnostic.Details) > 0 {
		fields["details"] = diagnostic.Details
	}
	return marshalObject(fields)
}

func decodeDiagnostic(data []byte) (Diagnostic, error) {
	fields, err := decodeClosedObject(data, "code", "severity", "path", "message", "details")
	if err != nil {
		return Diagnostic{}, err
	}
	reader := fieldReader{fields: fields}
	diagnostic := Diagnostic{
		Code:     reader.requiredString("code"),
		Severity: readEnum(&reader, "severity", DiagnosticInfo, "diagnostic severity", diagnosticSeverities...),
		Path:     reader.optionalString("path"),
		Message:  reader.requiredString("message"),
	}
	if reader.err != nil {
		return Diagnostic{}, reader.err
	}
	if err := decodeOptionalField(fields, "details", &diagnostic.Details, decodeJSONValue[map[string]string]); err != nil {
		return Diagnostic{}, fmt.Errorf("diagnostic %w", err)
	}
	return diagnostic, nil
}

var diagnosticSeverities = []DiagnosticSeverity{DiagnosticInfo, DiagnosticWarning, DiagnosticError}

func decodeDiagnostics(data []byte) ([]Diagnostic, error) {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil, fmt.Errorf("diagnostics must be an array")
	}
	return decodeArray(data, "diagnostic", decodeDiagnostic)
}
