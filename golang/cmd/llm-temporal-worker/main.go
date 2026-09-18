package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	workerruntime "github.com/mfow/llm-temporal-worker/golang/internal/runtime"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	os.Exit(Execute(ctx, os.Args[1:], CommandOptions{
		Out:                os.Stdout,
		ErrOut:             os.Stderr,
		RunWorkerFile:      workerruntime.RunWorkerFile,
		RunBudgetBootstrap: runBudgetBootstrap,
	}))
}
