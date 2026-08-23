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
	maxCandidates      = 1024
)

var (
	privateKeyPattern            = regexp.MustCompile(`-----BEGIN(?: [A-Z]+)? PRIVATE KEY-----`)
	awsAccessKeyPattern          = regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`)
	githubTokenPattern           = regexp.MustCompile(`\b(?:gh[pousr]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,})\b`)
	slackTokenPattern            = regexp.MustCompile(`\bxox[baprs]-[A-Za-z0-9-]{10,}\b`)
	openAITokenPattern           = regexp.MustCompile(`\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b`)
	anthropicTokenPattern        = regexp.MustCompile(`\bsk-ant-[A-Za-z0-9_-]{20,}\b`)
	credentialFieldPattern       = regexp.MustCompile(`(?i)(?:authorization|api[_-]?key|auth[_-]?token|access[_-]?token|secret[_-]?key|password)\s*(?:\\?["']\s*)?[:=]\s*(?:\\?["']\s*)?(?:bearer\s+)?[A-Za-z0-9_./=+-]{8,}`)
	quotedCredentialFieldPattern = regexp.MustCompile(`(?i)(?:authorization|api[_-]?key|auth[_-]?token|access[_-]?token|secret[_-]?key|password)\s*(?:\\?["']\s*)?[:=]\s*(?:\\?["']\s*)(?:bearer\s+)?[A-Za-z0-9_./=+-]{8,}`)
	executableCredentialPattern  = regexp.MustCompile(`(?i)(?:authorization|api[_-]?key|auth[_-]?token|access[_-]?token|secret[_-]?key|password)\s*(?:\\?["']\s*)?(?:[:][:][:]=|[:][:]=|\?=|\+=|!=|:=|:|=)\s*(?:\\?["']\s*)?(?:bearer\s+)?[A-Za-z0-9_./=+-]{8,}`)
	dockerSpaceCredentialPattern = regexp.MustCompile(`(?im)^[ \t]*env[ \t]+(?:authorization|api[_-]?key|auth[_-]?token|access[_-]?token|secret[_-]?key|password)[ \t]+(?:\\?["'][ \t]*)?(?:bearer[ \t]+)?[A-Za-z0-9_./=+-]{8,}`)
	netrcPasswordPattern         = regexp.MustCompile(`(?i)\bpassword\s+(?:\\?["']\s*)?(?:bearer\s+)?[A-Za-z0-9_./=+-]{8,}`)
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
		data, text, err := readBoundedText(path, maxSourceFileBytes)
		if err != nil {
			return fmt.Errorf("source safety verification cannot read %s", filepath.ToSlash(relative))
		}
		if !text {
			return nil
		}
		scan := scanContent
		if strings.EqualFold(filepath.Base(relative), ".netrc") {
			scan = scanNetrcContent
		} else if isDockerSource(relative) {
			scan = scanDockerSourceContent
		} else if isShellLikeSource(relative) {
			scan = scanExecutableSourceContent
		} else if isSourceCode(relative) {
			scan = scanSourceContent
		}
		found, err := scan(data)
		if err != nil {
			return fmt.Errorf("source safety verification %s: %w", filepath.ToSlash(relative), err)
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
	name := strings.ToLower(filepath.Base(relative))
	if name == "dockerfile" || strings.HasPrefix(name, "dockerfile.") ||
		name == "makefile" || strings.HasPrefix(name, "makefile.") {
		return true
	}
	switch strings.ToLower(filepath.Ext(relative)) {
	case ".go", ".ml", ".mli", ".sh", ".bash", ".zsh", ".py", ".js", ".mjs", ".cjs", ".ts", ".tsx", ".jsx", ".rs", ".java", ".kt", ".kts", ".rb", ".php":
		return true
	default:
		return false
	}
}

func isShellLikeSource(relative string) bool {
	name := strings.ToLower(filepath.Base(relative))
	if name == "dockerfile" || strings.HasPrefix(name, "dockerfile.") ||
		name == "makefile" || strings.HasPrefix(name, "makefile.") {
		return true
	}
	switch strings.ToLower(filepath.Ext(relative)) {
	case ".sh", ".bash", ".zsh":
		return true
	default:
		return false
	}
}

func isDockerSource(relative string) bool {
	name := strings.ToLower(filepath.Base(relative))
	return name == "dockerfile" || strings.HasPrefix(name, "dockerfile.")
}

func ignoredDirectory(name string) bool {
	switch name {
	case ".git", ".cache", ".direnv", ".mypy_cache", ".pytest_cache", ".ruff_cache", ".venv", ".worktrees", "__pycache__",
		"_build", "build", "coverage", "dist", "node_modules", "out", "release-artifacts", "target", "vendor":
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

func readBoundedText(path string, limit int) ([]byte, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	if err != nil {
		return nil, false, err
	}
	if !utf8.Valid(data) || bytes.IndexByte(data, 0) >= 0 {
		return nil, false, nil
	}
	if len(data) > limit {
		return nil, true, fmt.Errorf("file exceeds the verification size limit of %d bytes", limit)
	}
	return data, true, nil
}

func scanContent(data []byte) (*finding, error) {
	return scanWithCredentialFieldPattern(data, credentialFieldPattern, false, false, maxSourceFileBytes)
}

func scanNetrcContent(data []byte) (*finding, error) {
	found, err := scanContent(data)
	if err != nil || found != nil {
		return found, err
	}
	return scanWithCredentialFieldPattern(data, netrcPasswordPattern, false, false, maxSourceFileBytes)
}

func scanSourceContent(data []byte) (*finding, error) {
	return scanWithCredentialFieldPattern(data, quotedCredentialFieldPattern, false, false, maxSourceFileBytes)
}

func scanExecutableSourceContent(data []byte) (*finding, error) {
	return scanWithCredentialFieldPattern(data, executableCredentialPattern, false, true, maxSourceFileBytes)
}

func scanDockerSourceContent(data []byte) (*finding, error) {
	found, err := scanExecutableSourceContent(data)
	if err != nil || found != nil {
		return found, err
	}
	return scanWithCredentialFieldPattern(data, dockerSpaceCredentialPattern, false, true, maxSourceFileBytes)
}

func scanTestOutput(data []byte) (*finding, error) {
	return scanWithCredentialFieldPattern(data, credentialFieldPattern, true, false, maxTestOutputBytes)
}

func scanWithCredentialFieldPattern(data []byte, fieldPattern *regexp.Regexp, detectOutputLeaks, allowVariableWiring bool, limit int) (*finding, error) {
	if len(data) > limit {
		return nil, fmt.Errorf("source safety verification input exceeds the %d byte limit", limit)
	}
	candidates := []candidate{{data: data, encoding: "raw"}}
	seen := make(map[string]struct{}, maxCandidates)
	queued := map[string]struct{}{string(data): {}}
	for len(candidates) > 0 {
		if len(seen)+len(queued) > maxCandidates {
			return nil, fmt.Errorf("source safety verification decoded candidate limit of %d exceeded (seen=%d queued=%d)", maxCandidates, len(seen), len(queued))
		}
		current := candidates[0]
		candidates = candidates[1:]
		key := string(current.data)
		delete(queued, key)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		if found := findCredentialLikeMaterial(current.data, current.encoding, fieldPattern, detectOutputLeaks, allowVariableWiring); found != nil {
			return found, nil
		}
		if current.depth == maxDecodeDepth || len(seen) >= maxCandidates {
			continue
		}
		decoded := decodedCandidates(current.data, current.depth+1)
		uniqueDecoded := make([]candidate, 0, len(decoded))
		batch := make(map[string]struct{}, len(decoded))
		for _, candidate := range decoded {
			if len(candidate.data) > limit {
				continue
			}
			key := string(candidate.data)
			if _, exists := seen[key]; exists {
				continue
			}
			if _, exists := queued[key]; exists {
				continue
			}
			if _, exists := batch[key]; exists {
				continue
			}
			batch[key] = struct{}{}
			if found := findCredentialLikeMaterial(candidate.data, candidate.encoding, fieldPattern, detectOutputLeaks, allowVariableWiring); found != nil {
				return found, nil
			}
			// Go's JSON test stream can contain tens of thousands of unique
			// record, string, and escape candidates. They have all been inspected
			// above; only base64 candidates need recursive queueing to cover a
			// nested encoding. The raw test stream itself is URL/escape decoded
			// before this queue is reached.
			if detectOutputLeaks && candidate.encoding != "Base64-decoded" {
				continue
			}
			uniqueDecoded = append(uniqueDecoded, candidate)
		}
		if len(seen)+len(queued)+len(uniqueDecoded) > maxCandidates {
			return nil, fmt.Errorf("source safety verification decoded candidate limit of %d exceeded (seen=%d queued=%d decoded=%d)", maxCandidates, len(seen), len(queued), len(uniqueDecoded))
		}
		for _, candidate := range uniqueDecoded {
			key := string(candidate.data)
			queued[key] = struct{}{}
			candidates = append(candidates, candidate)
		}
	}
	return nil, nil
}

func findCredentialLikeMaterial(data []byte, encoding string, fieldPattern *regexp.Regexp, detectOutputLeaks, allowVariableWiring bool) *finding {
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
		if containsRedactionMarker(match) || allowVariableWiring && isCredentialVariableWiring(match) {
			continue
		}
		return &finding{category: "credential-like denied field", encoding: encoding}
	}
	return nil
}

func containsRedactionMarker(value []byte) bool {
	assignment, ok := parseCredentialAssignment(value)
	if !ok {
		return false
	}
	for _, marker := range []string{"redacted", "placeholder", "example", "fixture", "local-only", "not-configured"} {
		if assignment.value == marker {
			return true
		}
	}
	for _, prefix := range []string{"redacted-", "placeholder-", "example-", "fixture-", "local-", "mock-", "test-"} {
		if strings.HasPrefix(assignment.value, prefix) {
			return true
		}
	}
	return false
}

type credentialAssignment struct {
	field  string
	value  string
	quoted bool
	bearer bool
}

func parseCredentialAssignment(match []byte) (credentialAssignment, bool) {
	raw := strings.TrimSpace(strings.ToLower(string(match)))
	delimiter := strings.IndexAny(raw, ":=")
	fieldValue := ""
	value := ""
	colonDelimited := false
	if delimiter >= 0 {
		fieldValue = raw[:delimiter]
		value = strings.TrimSpace(raw[delimiter+1:])
		colonDelimited = raw[delimiter] == ':'
	} else {
		parts := strings.Fields(raw)
		if len(parts) < 2 {
			return credentialAssignment{}, false
		}
		if parts[0] == "env" && len(parts) >= 3 {
			fieldValue = parts[1]
			value = strings.Join(parts[2:], " ")
		} else {
			fieldValue = parts[0]
			value = strings.Join(parts[1:], " ")
		}
	}
	field := normalizeCredentialIdentifier(fieldValue)
	if colonDelimited {
		value = strings.TrimSpace(strings.TrimLeft(value, ":="))
		if strings.HasPrefix(value, "-") {
			value = strings.TrimSpace(value[1:])
		}
	}
	quoted := false
	for _, quote := range []string{`\"`, `\'`, `"`, `'`} {
		if strings.HasPrefix(value, quote) {
			quoted = true
			value = strings.TrimSpace(value[len(quote):])
			break
		}
	}
	bearer := strings.HasPrefix(value, "bearer ")
	if bearer {
		value = strings.TrimSpace(strings.TrimPrefix(value, "bearer "))
	}
	value = strings.Trim(value, `\"' `)
	if field == "" || value == "" {
		return credentialAssignment{}, false
	}
	return credentialAssignment{field: field, value: value, quoted: quoted, bearer: bearer}, true
}

func isCredentialVariableWiring(match []byte) bool {
	assignment, ok := parseCredentialAssignment(match)
	if !ok || assignment.quoted || assignment.bearer {
		return false
	}
	return assignment.field == normalizeCredentialIdentifier(assignment.value)
}

func normalizeCredentialIdentifier(value string) string {
	var normalized strings.Builder
	for _, char := range value {
		if char >= 'a' && char <= 'z' || char >= '0' && char <= '9' {
			normalized.WriteRune(char)
		}
	}
	return normalized.String()
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
	appendJSONCandidates(data, depth, &decoded)
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
