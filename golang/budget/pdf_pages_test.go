package budget

import (
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func pdf(body string) llm.DocumentPart {
	return llm.DocumentPart{Bytes: []byte("%PDF-1.7\n" + body + "\n%%EOF"), MediaType: "application/pdf"}
}

func TestDocumentAllowanceBoundsAnInlinePDFByItsVisiblePages(t *testing.T) {
	twoPages := pdf("1 0 obj <</Type /Catalog /Pages 2 0 R>> endobj\n2 0 obj <</Type/Pages /Kids [3 0 R 4 0 R] /Count 2>> endobj\n" +
		"3 0 obj <</Type /Page /Parent 2 0 R>> endobj\n4 0 obj <</Type\n% comment\n/Page /Parent 2 0 R>> endobj")
	if got, want := documentInputAllowance(twoPages), 2*MediaDocumentTokensPerPage; got != want {
		t.Fatalf("two-page PDF allowance = %d, want %d", got, want)
	}
	for name, part := range map[string]llm.DocumentPart{
		"object stream":   pdf("3 0 obj <</Type /ObjStm /N 600>> stream x endstream endobj\n4 0 obj <</Type /Page>> endobj"),
		"escaped name":    pdf("3 0 obj <</Type /Pag#65>> endobj\n4 0 obj <</Type /Page>> endobj"),
		"escaped key":     pdf("3 0 obj <</Ty#70e /Page>> endobj\n4 0 obj <</Type /Page>> endobj"),
		"escaped stream":  pdf("3 0 obj <</Type /Obj#53tm /N 600>> endobj\n4 0 obj <</Type /Page>> endobj"),
		"no visible page": pdf("1 0 obj <</Type /Catalog>> endobj"),
		"not a PDF":       {Bytes: []byte("<</Type /Page>>"), MediaType: "application/pdf"},
		"URL":             {URL: "https://example.com/a.pdf", MediaType: "application/pdf"},
	} {
		if got := documentInputAllowance(part); got != MediaDocumentInputTokenFloor {
			t.Fatalf("%s allowance = %d, want the unknown-size floor", name, got)
		}
	}
	many := pdf(strings.Repeat("<</Type /Page>>\n", 700))
	if got := documentInputAllowance(many); got != MediaDocumentInputTokenFloor {
		t.Fatalf("700-page allowance = %d, want it capped at the floor", got)
	}
	if got := documentInputAllowance(pdf("<</Type /Pages /Count 1>> <</TypeX /Page>> <</Type /Page>>")); got != MediaDocumentTokensPerPage {
		t.Fatalf("one-page allowance = %d, want one page (ignoring /Pages and /TypeX)", got)
	}
}
