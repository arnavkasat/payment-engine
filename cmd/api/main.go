// Command api serves the payment HTTP API.
//
// It captures payments and writes their events to the outbox; publishing
// those events and settling them is the worker's job. Splitting the two
// means a broker outage slows settlement without taking payments down.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/arnavkasat/payment-engine/internal/app"
	"github.com/arnavkasat/payment-engine/internal/config"
	"github.com/arnavkasat/payment-engine/internal/consumer"
	"github.com/arnavkasat/payment-engine/internal/events"
	"github.com/arnavkasat/payment-engine/internal/events/inmem"
	"github.com/arnavkasat/payment-engine/internal/httpapi"
	"github.com/arnavkasat/payment-engine/internal/outbox"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "api: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// The signal context is cancelled on the first SIGINT or SIGTERM;
	// a second signal kills the process outright, which is what an
	// impatient operator expects.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	application, err := app.Open(startCtx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = application.Close() }()
	application.LogBalances(startCtx)

	// In the dependency-free demo mode there is no worker and no broker, so
	// the API runs the publisher and the settlement consumer itself over an
	// in-process bus. A real deployment runs cmd/worker against Kafka.
	if cfg.DatabaseURL == app.MemoryDSN {
		stopPipeline, err := runEmbeddedPipeline(ctx, application)
		if err != nil {
			return err
		}
		defer stopPipeline()
	}

	api, err := httpapi.New(httpapi.Config{
		Service: application.Service,
		Store:   application.Store,
		Metrics: application.Metrics,
		Faults:  application.Faults,
		Logger:  application.Log,
	})
	if err != nil {
		return err
	}

	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: api,
		// Timeouts are set explicitly: the zero value is no timeout at all,
		// which lets a stalled client hold a connection indefinitely.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		application.Log.Info("api listening", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
			return
		}
		errc <- nil
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		application.Log.Info("shutting down", "timeout", cfg.ShutdownTimeout)
	}

	// Shutdown gets a context of its own: the signal already cancelled ctx,
	// and in-flight payments deserve the chance to commit.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
	defer cancelShutdown()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	application.Log.Info("stopped")
	return <-errc
}

// runEmbeddedPipeline starts an in-process outbox publisher and settlement
// consumer, and returns a function that waits for both to stop.
func runEmbeddedPipeline(ctx context.Context, application *app.App) (func(), error) {
	bus := inmem.New(4096, 5*time.Millisecond)

	publisher, err := outbox.New(application.Store, bus, outbox.Config{
		Owner:    "embedded-publisher",
		Batch:    application.Config.OutboxBatch,
		Interval: application.Config.OutboxInterval,
		Lease:    application.Config.OutboxLease,
		Faults:   application.Faults,
		Metrics:  application.Metrics,
		Logger:   application.Log,
	})
	if err != nil {
		return nil, err
	}
	processor, err := consumer.New(application.Store, consumer.Config{
		Group:   application.Config.ConsumerGroup,
		Faults:  application.Faults,
		Metrics: application.Metrics,
		Logger:  application.Log,
	})
	if err != nil {
		return nil, err
	}
	processor.Register(events.TypePaymentCaptured, application.Service.HandleCaptured)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = publisher.Run(ctx) }()
	go func() { defer wg.Done(); _ = bus.Run(ctx, processor.Handle) }()
	application.Log.Info("embedded settlement pipeline started")

	return func() {
		wg.Wait()
		_ = bus.Close()
	}, nil
}
