package documentationtest

import (
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"unicode"
)

// runbookFields are the labels every alert runbook section must give: the
// signal that fires, the readiness reason code, what is safe to do and what
// must never be done (#1557).
var runbookFields = []string{"**Signal:**", "**Readiness reason:**", "**Safe action:**", "**Forbidden:**"}

var alertRunbookLink = regexp.MustCompile(`^\[Runbook\]\(\.\./runbooks/([a-z0-9-]+\.md)#([a-z0-9_-]+)\)$`)

// TestAlertCatalogRowsLinkToRunbookSections binds the operations guide's
// alerts catalog to its runbooks: every row must link to an existing section
// of a file under docs/runbooks, and that section must name the signal,
// readiness reason, safe action and forbidden action.
func TestAlertCatalogRowsLinkToRunbookSections(t *testing.T) {
	root := repositoryRoot(t)
	operations := readText(t, root, "docs/architecture/deployment-and-operations.md")
	rows := alertCatalogRows(t, operations)
	if len(rows) == 0 {
		t.Fatal("the alerts catalog has no rows")
	}
	seen := make(map[string]bool, len(rows))
	for _, row := range rows {
		alert, link := row[0], row[2]
		if seen[alert] {
			t.Errorf("alert %q is listed twice", alert)
		}
		seen[alert] = true
		match := alertRunbookLink.FindStringSubmatch(link)
		if match == nil {
			t.Errorf("alert %q runbook cell %q is not a [Runbook](../runbooks/<file>.md#<section>) link", alert, link)
			continue
		}
		runbook := readText(t, root, filepath.Join("docs", "runbooks", match[1]))
		section, ok := markdownSection(runbook, match[2])
		if !ok {
			t.Errorf("alert %q links to missing section %s#%s", alert, match[1], match[2])
			continue
		}
		for _, field := range runbookFields {
			if !strings.Contains(section, field) {
				t.Errorf("runbook section %s#%s for alert %q does not give %s", match[1], match[2], alert, field)
			}
		}
	}
}

// TestIncidentRunbooksCoverRequiredFailureModes keeps the runbooks for failure
// modes that have no alert row of their own, and the on-call entry point that
// docs/index.md links to.
func TestIncidentRunbooksCoverRequiredFailureModes(t *testing.T) {
	root := repositoryRoot(t)
	index := readText(t, root, "docs/index.md")
	assertContains(t, index, "(runbooks/on-call.md)", "docs/index.md must link the on-call escalation and severity page")
	onCall := readText(t, root, "docs/runbooks/on-call.md")
	for _, heading := range []string{"severity-definitions", "escalation", "readiness-reason-codes"} {
		if _, ok := markdownSection(onCall, heading); !ok {
			t.Errorf("docs/runbooks/on-call.md has no %q section", heading)
		}
	}
	for file, anchors := range map[string][]string{
		"state-stores.md": {
			"dynamodb-throttling", "dynamodb-table-updating",
			"s3-unavailable", "s3-restored-with-missing-objects",
		},
		"temporal.md":                      {"temporal-namespace-outage"},
		"configuration-and-credentials.md": {"provider-credential-rotation", "configuration-rollback-for-process-lifetime-fields"},
	} {
		runbook := readText(t, root, filepath.Join("docs", "runbooks", file))
		for _, anchor := range anchors {
			section, ok := markdownSection(runbook, anchor)
			if !ok {
				t.Errorf("docs/runbooks/%s has no %q section", file, anchor)
				continue
			}
			for _, field := range runbookFields {
				if !strings.Contains(section, field) {
					t.Errorf("runbook section %s#%s does not give %s", file, anchor, field)
				}
			}
		}
	}
	stuck := readNormalizedText(t, filepath.Join(root, "docs", "runbooks", "stuck-ambiguous-attempt.md"))
	assertContainsEach(t, stuck, "stuck ambiguous attempt page must point at %q",
		"generation-recover", "status reader", "unknown-cost-reconciliation.md#phased-implementation-plan", "What is not yet possible")
}

func TestMarkdownSectionUsesGitHubAnchors(t *testing.T) {
	document := "# Title\n\n## Route held\n\nheld\n\n### Detail\n\nmore\n\n```sh\n# not a heading\n```\n\n## Catalog `effective_until` expiry\n\nlast\n"
	section, ok := markdownSection(document, "route-held")
	if !ok || !strings.Contains(section, "held") || !strings.Contains(section, "more") || strings.Contains(section, "last") {
		t.Fatalf("route-held section = %q, %v", section, ok)
	}
	section, ok = markdownSection(document, "catalog-effective_until-expiry")
	if !ok || !strings.Contains(section, "last") || strings.Contains(section, "# not a heading") {
		t.Fatalf("catalog section = %q, %v", section, ok)
	}
	if _, ok := markdownSection(document, "not-a-heading"); ok {
		t.Fatal("a comment inside a code fence must not count as a heading")
	}
}

// alertCatalogRows returns the cells of each data row of the table in the
// "## Alerts" section, after checking its header.
func alertCatalogRows(t *testing.T, document string) [][]string {
	t.Helper()
	section, ok := markdownSection(document, "alerts")
	if !ok {
		t.Fatal("deployment-and-operations.md has no Alerts section")
	}
	var rows [][]string
	header := false
	for _, line := range strings.Split(section, "\n") {
		if !strings.HasPrefix(line, "|") {
			if header && len(rows) > 0 {
				break
			}
			continue
		}
		cells := markdownTableCells(line)
		switch {
		case !header:
			if strings.Join(cells, "|") != "Alert|Signal|Runbook" {
				t.Fatalf("alerts table header = %q, want Alert | Signal | Runbook", cells)
			}
			header = true
		case strings.Trim(strings.Join(cells, ""), "-: ") == "":
		default:
			if len(cells) != 3 {
				t.Fatalf("alerts table row %q has %d cells, want 3", line, len(cells))
			}
			rows = append(rows, cells)
		}
	}
	return rows
}

// markdownTableCells splits a table row on unescaped pipes.
func markdownTableCells(line string) []string {
	const escapedPipe = "\x00"
	line = strings.ReplaceAll(strings.TrimSpace(line), `\|`, escapedPipe)
	line = strings.TrimSuffix(strings.TrimPrefix(line, "|"), "|")
	parts := strings.Split(line, "|")
	cells := make([]string, len(parts))
	for index, part := range parts {
		cells[index] = strings.TrimSpace(strings.ReplaceAll(part, escapedPipe, `\|`))
	}
	return cells
}

// markdownSection returns the text under the heading whose GitHub anchor is
// anchor, up to the next heading of the same or a higher level. Lines inside
// fenced code blocks are never headings.
func markdownSection(document, anchor string) (string, bool) {
	lines := strings.Split(document, "\n")
	start, level := -1, 0
	fenced := false
	for index, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			fenced = !fenced
			continue
		}
		if fenced {
			continue
		}
		depth, title, ok := markdownHeading(line)
		if !ok {
			continue
		}
		if start >= 0 && depth <= level {
			return strings.Join(lines[start:index], "\n"), true
		}
		if start < 0 && headingAnchor(title) == anchor {
			start, level = index+1, depth
		}
	}
	if start < 0 {
		return "", false
	}
	return strings.Join(lines[start:], "\n"), true
}

func markdownHeading(line string) (int, string, bool) {
	depth := 0
	for depth < len(line) && line[depth] == '#' {
		depth++
	}
	if depth == 0 || depth > 6 || depth >= len(line) || line[depth] != ' ' {
		return 0, "", false
	}
	return depth, strings.TrimSpace(line[depth:]), true
}

// headingAnchor is GitHub's heading slug: lower case, spaces become hyphens,
// and punctuation other than hyphens and underscores is dropped.
func headingAnchor(title string) string {
	var anchor strings.Builder
	for _, r := range strings.ToLower(title) {
		switch {
		case r == ' ':
			anchor.WriteRune('-')
		case r == '-' || r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r):
			anchor.WriteRune(r)
		}
	}
	return anchor.String()
}
