// Command sourceverify rejects checked-in credential-like material and test
// output that could expose it. It uses only the Go standard library so the
// verification target is available in an offline checkout.
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	maxSourceFileBytes = 1 << 20
	maxTestOutputBytes = 8 << 20
	maxDecodeDepth     = 3
	maxCandidates      = 256
)

var (
	privateKeyPattern            = regexp.MustCompile(`-----BEGIN(?: [A-Z]+)? PRIVATE KEY-----`)
	awsAccessKeyPattern          = regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`)
	githubTokenPattern           = regexp.MustCompile(`\b(?:gh[pousr]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,})\b`)
	slackTokenPattern            = regexp.MustCompile(`\bxox[baprs]-[A-Za-z0-9-]{10,}\b`)
	openAITokenPattern           = regexp.MustCompile(`\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b`)
	anthropicTokenPattern        = regexp.MustCompile(`\bsk-ant-[A-Za-z0-9_-]{20,}\b`)
	credentialFieldPattern       = regexp.MustCompile(`(?i)(?:authorization|api[_-]?key|access[_-]?token|secret[_-]?key|password)\s*(?:\\?["']\s*)?[:=]\s*(?:\\?["']\s*)?(?:bearer\s+)?[A-Za-z0-9_./=+-]{8,}`)
	quotedCredentialFieldPattern = regexp.MustCompile(`(?i)(?:authorization|api[_-]?key|access[_-]?token|secret[_-]?key|password)\s*(?:\\?["']\s*)?[:=]\s*(?:\\?["']\s*)(?:bearer\s+)?[A-Za-z0-9_./=+-]{8,}`)
	testOutputLeakPattern        = regexp.MustCompile(`(?i)\b(?:prompt|output|tool(?:[_ -](?:arguments?|results?))?|continuation(?:[_ -]?handle)?|authorization|provider[_ -]?state|raw provider (?:body|message))\b[^\r\n]{0,120}\b(?:leak(?:ed|age)?|emit(?:ted|ting)?|log(?:ged|ging)?|expos(?:ed|ure))\b[^\r\n]{0,256}`)
	base64TokenPattern           = regexp.MustCompile(`[A-Za-z0-9+/_-]{16,}={0,2}`)
	quotedStringPattern          = regexp.MustCompile(`"(?:\\.|[^"\\])*"`)
)

type finding struct {
	category string
	encoding string
}

type candidate struct {
	data     []byte
	encoding string
	depth    int
}

func main() {
	root := flag.String("root", ".", "repository root to verify")
	testOutput := flag.String("test-output", "", "captured go test output to verify")
	flag.Parse()

	if err := verify(*root, *testOutput); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("source safety verification passed")
}

func verify(root, testOutput string) error {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("source safety verification cannot resolve repository root")
	}
	info, err := os.Stat(absRoot)
	if err != nil || !info.IsDir() {
		return fmt.Errorf("source safety verification repository root is not a directory")
	}

	outputPath := ""
	if testOutput != "" {
		outputPath, err = filepath.Abs(testOutput)
		if err != nil {
			return fmt.Errorf("source safety verification cannot resolve test output")
		}
	}

	err = filepath.WalkDir(absRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("source safety verification cannot read a repository path")
		}
		if entry.IsDir() {
			if ignoredDirectory(entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !entry.Type().IsRegular() || path == outputPath {
			return nil
		}
		relative, err := filepath.Rel(absRoot, path)
		if err != nil {
			return fmt.Errorf("source safety verification cannot identify a repository path")
		}
		if !shouldScanSource(relative) {
			return nil
		}
		data, err := readBounded(path, maxSourceFileBytes)
		if err != nil {
			return fmt.Errorf("source safety verification cannot read %s", filepath.ToSlash(relative))
		}
		scan := scanContent
		if isSourceCode(relative) {
			scan = scanSourceContent
		}
		found, err := scan(data)
		if err != nil {
			return err
		}
		if found == nil {
			return nil
		}
		return unsafeFinding(filepath.ToSlash(relative), *found)
	})
	if err != nil {
		return err
	}

	if outputPath == "" {
		return nil
	}
	data, err := readBounded(outputPath, maxTestOutputBytes)
	if err != nil {
		return fmt.Errorf("source safety verification cannot read test output: %w", err)
	}
	found, err := scanTestOutput(data)
	if err != nil {
		return err
	}
	if found == nil {
		return nil
	}
	return unsafeFinding("test output", *found)
}

func isSourceCode(relative string) bool {
	switch strings.ToLower(filepath.Ext(relative)) {
	case ".go", ".ml", ".mli":
		return true
	default:
		return false
	}
}

func ignoredDirectory(name string) bool {
	switch name {
	case ".git", ".cache", "node_modules", "vendor":
		return true
	default:
		return false
	}
}

func shouldScanSource(relative string) bool {
	parts := strings.Split(filepath.ToSlash(relative), "/")
	fixture := false
	for _, part := range parts[:len(parts)-1] {
		switch strings.ToLower(part) {
		case "testdata", "fixture", "fixtures":
			fixture = true
		}
	}
	name := parts[len(parts)-1]
	if strings.HasSuffix(name, "_test.go") {
		return false
	}
	if fixture {
		return true
	}
	switch strings.ToLower(filepath.Ext(name)) {
	case ".go", ".ml", ".mli", ".json", ".yaml", ".yml", ".toml", ".env", ".sh", ".bash", ".zsh", ".properties", ".ini", ".cfg", ".conf":
		return true
	default:
		return false
	}
}

func readBounded(path string, limit int) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > limit {
		return nil, fmt.Errorf("file exceeds the verification size limit of %d bytes", limit)
	}
	return data, nil
}

func scanContent(data []byte) (*finding, error) {
	return scanWithCredentialFieldPattern(data, credentialFieldPattern, false, maxSourceFileBytes)
}

func scanSourceContent(data []byte) (*finding, error) {
	return scanWithCredentialFieldPattern(data, quotedCredentialFieldPattern, false, maxSourceFileBytes)
}

func scanTestOutput(data []byte) (*finding, error) {
	return scanWithCredentialFieldPattern(data, credentialFieldPattern, true, maxTestOutputBytes)
}

func scanWithCredentialFieldPattern(data []byte, fieldPattern *regexp.Regexp, detectOutputLeaks bool, limit int) (*finding, error) {
	if len(data) > limit {
		return nil, fmt.Errorf("source safety verification input exceeds the %d byte limit", limit)
	}
	candidates := []candidate{{data: data, encoding: "raw"}}
	seen := make(map[string]struct{}, maxCandidates)
	for len(candidates) > 0 {
		current := candidates[0]
		candidates = candidates[1:]
		key := string(current.data)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		if found := findCredentialLikeMaterial(current.data, current.encoding, fieldPattern, detectOutputLeaks); found != nil {
			return found, nil
		}
		if current.depth == maxDecodeDepth || len(seen) >= maxCandidates {
			continue
		}
		for _, decoded := range decodedCandidates(current.data, current.depth+1) {
			if len(decoded.data) <= limit {
				candidates = append(candidates, decoded)
			}
		}
	}
	return nil, nil
}

func findCredentialLikeMaterial(data []byte, encoding string, fieldPattern *regexp.Regexp, detectOutputLeaks bool) *finding {
	if detectOutputLeaks && testOutputLeakPattern.Match(data) {
		return &finding{category: "denied-field leak", encoding: encoding}
	}
	for _, pattern := range []*regexp.Regexp{
		privateKeyPattern,
		awsAccessKeyPattern,
		githubTokenPattern,
		slackTokenPattern,
		openAITokenPattern,
		anthropicTokenPattern,
	} {
		if pattern.Match(data) {
			return &finding{category: "credential-like material", encoding: encoding}
		}
	}
	for _, match := range fieldPattern.FindAll(data, -1) {
		if !containsRedactionMarker(match) {
			return &finding{category: "credential-like denied field", encoding: encoding}
		}
	}
	return nil
}

func containsRedactionMarker(value []byte) bool {
	normalized := strings.ToLower(string(value))
	for _, marker := range []string{"redacted", "placeholder", "example", "fixture", "local-only"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func decodedCandidates(data []byte, depth int) []candidate {
	if !utf8.Valid(data) {
		return nil
	}
	text := string(data)
	decoded := make([]candidate, 0, 8)
	add := func(value string, encoding string) {
		if value != text {
			decoded = append(decoded, candidate{data: []byte(value), encoding: encoding, depth: depth})
		}
	}

	if strings.Contains(text, "%") || strings.Contains(text, "+") {
		if value, err := url.QueryUnescape(text); err == nil {
			add(value, "URL-decoded")
		}
		if value, err := url.PathUnescape(text); err == nil {
			add(value, "URL-decoded")
		}
	}
	if strings.Contains(text, `\\`) {
		add(strings.ReplaceAll(text, `\\`, `\`), "escape-decoded")
	}
	if strings.Contains(text, `\"`) {
		add(strings.ReplaceAll(text, `\"`, `"`), "escape-decoded")
	}
	for _, quoted := range quotedStringPattern.FindAllString(text, -1) {
		if value, err := strconv.Unquote(quoted); err == nil {
			add(value, "escape-decoded")
		}
	}
	appendJSONCandidates(data, depth, &decoded)
	for _, token := range base64TokenPattern.FindAllString(text, -1) {
		for _, encoding := range []*base64.Encoding{
			base64.StdEncoding,
			base64.RawStdEncoding,
			base64.URLEncoding,
			base64.RawURLEncoding,
		} {
			value, err := encoding.DecodeString(token)
			if err == nil && len(value) > 0 && utf8.Valid(value) {
				decoded = append(decoded, candidate{data: value, encoding: "Base64-decoded", depth: depth})
			}
		}
	}
	return decoded
}

func appendJSONCandidates(data []byte, depth int, candidates *[]candidate) {
	var value any
	if json.Unmarshal(data, &value) == nil {
		appendNormalizedJSON(value, depth, candidates)
		return
	}
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if len(line) == 0 || json.Unmarshal(line, &value) != nil {
			continue
		}
		appendNormalizedJSON(value, depth, candidates)
	}
}

func appendNormalizedJSON(value any, depth int, candidates *[]candidate) {
	normalized, err := json.Marshal(value)
	if err == nil {
		*candidates = append(*candidates, candidate{data: normalized, encoding: "JSON-decoded", depth: depth})
	}
	for _, text := range jsonStrings(value) {
		*candidates = append(*candidates, candidate{data: []byte(text), encoding: "JSON-decoded", depth: depth})
	}
}

func jsonStrings(value any) []string {
	var stringsFound []string
	var visit func(any)
	visit = func(current any) {
		switch typed := current.(type) {
		case string:
			stringsFound = append(stringsFound, typed)
		case []any:
			for _, item := range typed {
				visit(item)
			}
		case map[string]any:
			for _, item := range typed {
				visit(item)
			}
		}
	}
	visit(value)
	return stringsFound
}

func unsafeFinding(location string, found finding) error {
	return fmt.Errorf("source safety verification: %s contains %s in %s form", location, found.category, found.encoding)
}
