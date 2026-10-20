// Package app wires the engine's parts together. Both binaries open an App,
// so they cannot drift apart in how they connect, migrate or bootstrap.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/arnavkasat/payment-engine/internal/config"
	"github.com/arnavkasat/payment-engine/internal/faults"
	"github.com/arnavkasat/payment-engine/internal/metrics"
	"github.com/arnavkasat/payment-engine/internal/money"
	"github.com/arnavkasat/payment-engine/internal/payments"
	"github.com/arnavkasat/payment-engine/internal/storage"
	"github.com/arnavkasat/payment-engine/internal/storage/memory"
	"github.com/arnavkasat/payment-engine/internal/storage/postgres"
)

// MemoryDSN selects the in-memory store instead of PostgreSQL. It exists so
// the API can be demonstrated with nothing installed; it keeps no data
// across restarts and is not a deployment option.
const MemoryDSN = "memory"

// ExternalAccount is the counterparty every deposit is drawn from. It is the
// one account allowed to run negative: its balance is, by construction,
// minus the total value held in the system.
const ExternalAccount = "world"

// App holds the long-lived objects a binary needs.
type App struct {
	Config  config.Config
	Log     *slog.Logger
	Metrics *metrics.Registry
	Store   storage.Store
	Service *payments.Service
	// Faults is shared by the service, the publisher and the consumer, so
	// arming a point affects the whole pipeline at once. It is nil unless
	// EnableFaultInjection is set, and a nil injector never fires.
	Faults *faults.Injector

	Clearing []uuid.UUID
	Fees     []uuid.UUID
	External uuid.UUID
}

// Open connects to PostgreSQL, applies migrations, optionally creates the
// standard accounts and builds the payment service.
func Open(ctx context.Context, cfg config.Config) (*App, error) {
	log := cfg.Logger()
	reg := metrics.NewRegistry()

	var store storage.Store
	if cfg.DatabaseURL == MemoryDSN {
		store = memory.New()
		log.Warn("using the in-memory store; nothing is persisted")
	} else {
		pg, err := postgres.Open(ctx, postgres.Config{
			DSN:      cfg.DatabaseURL,
			MaxConns: cfg.DatabaseMaxConns,
		})
		if err != nil {
			return nil, err
		}
		if err := postgres.Migrate(ctx, pg.Pool()); err != nil {
			_ = pg.Close()
			return nil, err
		}
		store = pg
		log.Info("database ready", "max_conns", cfg.DatabaseMaxConns)
	}

	a := &App{Config: cfg, Log: log, Metrics: reg, Store: store}
	if cfg.EnableFaultInjection {
		a.Faults = faults.New(time.Now().UnixNano())
		log.Warn("fault injection is enabled; /v1/faults can make this process fail on demand")
	}
	if cfg.Bootstrap {
		if err := a.bootstrap(ctx); err != nil {
			_ = store.Close()
			return nil, err
		}
	}
	if err := a.resolveAccounts(ctx); err != nil {
		_ = store.Close()
		return nil, err
	}

	svc, err := payments.New(store, payments.Config{
		Clearing: a.Clearing,
		Fees:     a.Fees,
		External: a.External,
		Faults:   a.Faults,
		Metrics:  reg,
	})
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	a.Service = svc
	log.Info("ledger accounts resolved",
		"clearing_shards", len(a.Clearing), "fee_shards", len(a.Fees))
	return a, nil
}

// bootstrap creates the standard accounts if they are missing. It is safe to
// run concurrently from several instances: a lost race surfaces as
// ErrAlreadyExists, which means the account the caller wanted now exists.
func (a *App) bootstrap(ctx context.Context) error {
	wanted := []storage.Account{
		{Name: ExternalAccount, Currency: "USD", Kind: storage.KindExternal, AllowNegative: true},
	}
	for i := 0; i < a.Config.ClearingShards; i++ {
		wanted = append(wanted,
			storage.Account{Name: shardName(a.Config.ClearingAccount, i), Currency: "USD", Kind: storage.KindClearing},
			storage.Account{Name: shardName(a.Config.FeeAccount, i), Currency: "USD", Kind: storage.KindRevenue})
	}
	for _, want := range wanted {
		if want.Name == "" {
			continue
		}
		if _, err := a.Store.GetAccountByName(ctx, want.Name); err == nil {
			continue
		} else if !errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("app: checking account %q: %w", want.Name, err)
		}
		if _, err := a.Store.CreateAccount(ctx, want); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
			return fmt.Errorf("app: creating account %q: %w", want.Name, err)
		}
		a.Log.Info("bootstrapped account", "name", want.Name, "kind", want.Kind)
	}
	return nil
}

// shardName renders the account name for one shard.
func shardName(prefix string, i int) string {
	return fmt.Sprintf("%s-%02d", prefix, i)
}

// resolveAccounts looks up the accounts the service posts against.
func (a *App) resolveAccounts(ctx context.Context) error {
	for i := 0; i < a.Config.ClearingShards; i++ {
		name := shardName(a.Config.ClearingAccount, i)
		clearing, err := a.Store.GetAccountByName(ctx, name)
		if err != nil {
			return fmt.Errorf("app: clearing account %q: %w", name, err)
		}
		a.Clearing = append(a.Clearing, clearing.ID)

		if a.Config.FeeAccount == "" {
			continue
		}
		feeName := shardName(a.Config.FeeAccount, i)
		fees, err := a.Store.GetAccountByName(ctx, feeName)
		if err != nil {
			return fmt.Errorf("app: fee account %q: %w", feeName, err)
		}
		a.Fees = append(a.Fees, fees.ID)
	}

	external, err := a.Store.GetAccountByName(ctx, ExternalAccount)
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("app: external account: %w", err)
		}
		a.Log.Warn("no external account; deposits are disabled", "name", ExternalAccount)
	} else {
		a.External = external.ID
	}
	return nil
}

// Close releases the store.
func (a *App) Close() error {
	if a.Store == nil {
		return nil
	}
	return a.Store.Close()
}

// LogBalances writes the current balance of every account, which is how both
// binaries announce what they are starting from.
func (a *App) LogBalances(ctx context.Context) {
	accounts, err := a.Store.ListAccounts(ctx)
	if err != nil {
		a.Log.Warn("listing accounts", "error", err)
		return
	}
	var total money.Minor
	for _, acct := range accounts {
		total += acct.Balance
	}
	a.Log.Info("ledger", "accounts", len(accounts), "net_balance", int64(total))
}
