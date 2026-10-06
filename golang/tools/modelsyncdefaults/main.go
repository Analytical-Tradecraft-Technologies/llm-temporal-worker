// Command modelsyncdefaults regenerates the built-in model-sync rules from
// the first-party upstream endpoint prices OpenRouter publishes. Run it from
// the golang directory and review the diff before committing:
//
//	go run ./tools/modelsyncdefaults
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/modelsync"
)

func main() {
	output := flag.String("out", "internal/modelsync/rules/default.yaml", "rules file to write")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	document, err := modelsync.Fetcher{Client: &http.Client{Timeout: 30 * time.Second}}.Fetch(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fetch OpenRouter catalog:", err)
		os.Exit(1)
	}
	rendered, err := modelsync.GenerateDefaultRules(document)
	if err != nil {
		fmt.Fprintln(os.Stderr, "generate rules:", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*output, rendered, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "write rules:", err)
		os.Exit(1)
	}
}
