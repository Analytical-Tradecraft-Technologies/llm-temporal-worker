package llm

import (
	"fmt"
	"net/netip"
	"net/url"
	"strings"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/netpolicy"
)

// ValidateMediaURLs applies the remote-media URL policy to every image and
// document URL in caller-supplied instructions and items. It is a request
// ingress check: the v1 Generate codec and the Activity payload boundary call
// it for new input, and it is deliberately not part of the item codec, which
// also decodes transcripts stored before the policy existed or was tightened.
func ValidateMediaURLs(instructions []Instruction, items []Item) error {
	for index, instruction := range instructions {
		if err := validateMediaURLParts(instruction.Content); err != nil {
			return fmt.Errorf("instruction %d: %w", index, err)
		}
	}
	for index, item := range items {
		var content []Part
		switch item := item.(type) {
		case Message:
			content = item.Content
		case *Message:
			if item != nil {
				content = item.Content
			}
		case ToolResult:
			content = item.Content
		case *ToolResult:
			if item != nil {
				content = item.Content
			}
		}
		if err := validateMediaURLParts(content); err != nil {
			return fmt.Errorf("item %d: %w", index, err)
		}
	}
	return nil
}

// RejectBlobMedia reports the first image or document part that references
// its bytes through an external BlobRef. No production path resolves media
// blob references and no provider adapter accepts one, so request preparation
// rejects them before routing instead of failing every candidate at compile.
func RejectBlobMedia(instructions []Instruction, items []Item) error {
	for index, instruction := range instructions {
		if err := rejectBlobMediaParts(instruction.Content); err != nil {
			return fmt.Errorf("instruction %d: %w", index, err)
		}
	}
	for index, item := range items {
		var content []Part
		switch item := item.(type) {
		case Message:
			content = item.Content
		case *Message:
			if item != nil {
				content = item.Content
			}
		case ToolResult:
			content = item.Content
		case *ToolResult:
			if item != nil {
				content = item.Content
			}
		}
		if err := rejectBlobMediaParts(content); err != nil {
			return fmt.Errorf("item %d: %w", index, err)
		}
	}
	return nil
}

func rejectBlobMediaParts(parts []Part) error {
	for index, part := range parts {
		var kind string
		switch part := part.(type) {
		case ImagePart:
			if part.Blob != nil {
				kind = "image"
			}
		case *ImagePart:
			if part != nil && part.Blob != nil {
				kind = "image"
			}
		case DocumentPart:
			if part.Blob != nil {
				kind = "document"
			}
		case *DocumentPart:
			if part != nil && part.Blob != nil {
				kind = "document"
			}
		}
		if kind != "" {
			return fmt.Errorf("part %d: %s blob references are not supported; send inline bytes or a URL", index, kind)
		}
	}
	return nil
}

func validateMediaURLParts(parts []Part) error {
	for index, part := range parts {
		var raw, kind string
		switch part := part.(type) {
		case ImagePart:
			raw, kind = part.URL, "image"
		case *ImagePart:
			if part != nil {
				raw, kind = part.URL, "image"
			}
		case DocumentPart:
			raw, kind = part.URL, "document"
		case *DocumentPart:
			if part != nil {
				raw, kind = part.URL, "document"
			}
		}
		if raw == "" {
			continue
		}
		if err := validateMediaURL(raw); err != nil {
			return fmt.Errorf("part %d: %s url: %w", index, kind, err)
		}
	}
	return nil
}

// validateMediaURL applies the remote-media policy to one caller-supplied
// image or document URL. The worker never fetches it, but providers do, inside
// their own network, so URLs that address local, private or metadata
// destinations, or that carry credentials, are rejected. A public DNS name
// that resolves to such an address cannot be detected here.
func validateMediaURL(raw string) error {
	if err := validateURI(raw); err != nil {
		return err
	}
	parsed, _ := url.Parse(raw)
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return fmt.Errorf("media URL scheme %q is not allowed; use https or http", parsed.Scheme)
	}
	if parsed.User != nil {
		return fmt.Errorf("media URL must not contain user information")
	}
	host := strings.ToLower(strings.TrimSuffix(parsed.Hostname(), "."))
	if host == "" {
		return fmt.Errorf("media URL must include a host")
	}
	if address, err := netip.ParseAddr(host); err == nil {
		// A zoned address never matches a prefix, so it is rejected outright.
		if address.Zone() != "" || netpolicy.BlockedAddress(address.Unmap()) {
			return fmt.Errorf("media URL host is not allowed")
		}
		return nil
	}
	if !mediaHostNameSyntax(host) || mediaHostEndsInNumber(host) || blockedMediaHostName(host) {
		return fmt.Errorf("media URL host is not allowed")
	}
	return nil
}

// mediaHostNameSyntax accepts only ASCII DNS name characters. Fetchers that
// follow the WHATWG URL rules map other characters before resolving (full-width
// digits and dots become ASCII, a backslash ends the host), which would turn a
// name this policy accepted into a blocked address. Internationalised names
// must be given in their ASCII (punycode) form.
func mediaHostNameSyntax(host string) bool {
	for _, char := range []byte(host) {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.' {
			continue
		}
		return false
	}
	return true
}

// mediaHostEndsInNumber reports whether common resolvers and URL parsers treat
// a host that is not a canonical IP literal as an IPv4 address: its last label
// is decimal, octal or hexadecimal, as in 127.1, 2130706433, 0x7f000001 and
// 0177.0.0.1. Names that merely contain digits, such as cdn1.example.com or
// 1password.com, end in a non-numeric label and are unaffected.
func mediaHostEndsInNumber(host string) bool {
	last := host[strings.LastIndexByte(host, '.')+1:]
	if last == "" {
		return true
	}
	digits := "0123456789"
	if strings.HasPrefix(last, "0x") {
		last, digits = last[2:], "0123456789abcdef"
	}
	return strings.Trim(last, digits) == ""
}

// blockedMediaHostName rejects names that resolve only inside a private
// network: localhost, the reserved private-use .internal zone (which holds the
// GCP and AWS metadata and instance names) and the well-known metadata aliases.
func blockedMediaHostName(host string) bool {
	for _, suffix := range []string{"localhost", "internal"} {
		if host == suffix || strings.HasSuffix(host, "."+suffix) {
			return true
		}
	}
	return mediaMetadataHostNames[host]
}

var mediaMetadataHostNames = map[string]bool{
	"metadata":      true, // GCP metadata server short name.
	"metadata.goog": true, // GCP metadata server alias.
	"instance-data": true, // AWS EC2 metadata short name.
}
