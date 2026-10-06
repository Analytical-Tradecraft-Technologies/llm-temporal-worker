package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/catalog"
	"github.com/mfow/llm-temporal-worker/golang/internal/modelsync"
	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

// modelSyncRuntime owns one configuration snapshot's synced catalog. It is the
// engine's SnapshotSource: each installed OpenRouter Document produces a new
// immutable engine.Snapshot that keeps the configuration digest and epoch of
// the base snapshot, so a refresh never fences in-flight requests the way a
// configuration reload does. Configured models and prices always win over
// synced ones with the same identity.
type modelSyncRuntime struct {
	base       engine.Snapshot
	basePrices pricing.Catalog
	rules      modelsync.Rules
	openrouter modelsync.Source
	direct     []modelsync.Source
	reserved   map[string]struct{}
	// enabled holds the sync endpoints whose adapter was built; an optional
	// endpoint without its credential is absent and never routed to.
	enabled map[string]struct{}

	current atomic.Pointer[engine.Snapshot]

	refresher *modelsync.Refresher
	cancel    context.CancelFunc
	done      chan struct{}
	stopOnce  sync.Once
}

var _ engine.SnapshotSource = (*modelSyncRuntime)(nil)

// newModelSyncRuntime resolves the rules and endpoint sources. It does not
// install anything yet: enabled endpoints are known only after adapters are
// built.
func newModelSyncRuntime(value config.Config, loaded loadedCatalogs, options catalog.Options) (*modelSyncRuntime, error) {
	settings := value.ModelSync
	documents := []modelsync.RulesDocument{}
	builtIn, err := modelsync.ParseRules(modelsync.DefaultRulesDocument())
	if err != nil {
		return nil, fmt.Errorf("model_sync built-in rules: %w", err)
	}
	documents = append(documents, builtIn)
	for index, ref := range settings.Rules {
		data, err := catalog.ReadVerified(ref, options)
		if err != nil {
			return nil, fmt.Errorf("model_sync.rules[%d]: %w", index, &catalogLoadError{cause: err})
		}
		document, err := modelsync.ParseRules(data)
		if err != nil {
			return nil, fmt.Errorf("model_sync.rules[%d]: %w", index, &catalogLoadError{cause: err})
		}
		documents = append(documents, document)
	}
	rules, err := modelsync.MergeRules(documents...)
	if err != nil {
		return nil, fmt.Errorf("model_sync rules: %w", err)
	}
	runtime := &modelSyncRuntime{base: loaded.snapshot, basePrices: loaded.prices, rules: rules, reserved: make(map[string]struct{}, len(value.Models))}
	for name := range value.Models {
		runtime.reserved[name] = struct{}{}
	}
	runtime.openrouter, err = modelSyncSource(value, loaded.bundle, settings.OpenRouter.Endpoint, "")
	if err != nil {
		return nil, err
	}
	for _, direct := range settings.Direct {
		if _, ok := rules.Providers[direct.Provider]; !ok {
			return nil, fmt.Errorf("model_sync.direct endpoint %q provider %q has no rules", direct.Endpoint, direct.Provider)
		}
		source, err := modelSyncSource(value, loaded.bundle, direct.Endpoint, direct.Provider)
		if err != nil {
			return nil, err
		}
		runtime.direct = append(runtime.direct, source)
	}
	runtime.current.Store(&runtime.base)
	return runtime, nil
}

func modelSyncSource(value config.Config, bundle catalog.Bundle, endpointID, rulesProvider string) (modelsync.Source, error) {
	endpoint := value.Endpoints[endpointID]
	profile, ok := bundle.Capabilities[endpoint.CapabilityProfile]
	if !ok {
		return modelsync.Source{}, fmt.Errorf("model_sync endpoint %q capability profile %q is unavailable", endpointID, endpoint.CapabilityProfile)
	}
	family := endpointFamily(endpoint.Family)
	if profile.Family != family {
		return modelsync.Source{}, fmt.Errorf("model_sync endpoint %q capability family %q does not match %q", endpointID, profile.Family, family)
	}
	region := endpoint.Region
	if region == "" {
		region = endpoint.AccountRegion
	}
	providerName := rulesProvider
	if providerName == "" {
		providerName = modelsync.ProviderOpenRouter
	}
	tiers := make(map[llm.ServiceClass]string, len(endpoint.ServiceClasses))
	for class, tier := range endpoint.ServiceClasses {
		tiers[class] = tier.ProviderValue
	}
	extensions := make([]string, 0, len(endpoint.Extensions))
	for name := range endpoint.Extensions {
		extensions = append(extensions, name)
	}
	sort.Strings(extensions)
	return modelsync.Source{
		EndpointID:            endpointID,
		RulesProvider:         rulesProvider,
		Family:                string(family),
		Region:                region,
		AccountRegion:         endpoint.AccountRegion,
		EndpointAccountHMAC:   routing.DeriveEndpointAccountHMAC(providerName, endpointID, endpoint.AccountRegion, region, value.Version),
		EndpointAccountDigest: endpointAccountDigest(providerName, endpoint),
		Tiers:                 tiers,
		Capabilities:          routingCapabilities(profile.Set),
		ProviderFeatures:      adapterCapabilities(profile.Set),
		ExtensionNames:        extensions,
	}, nil
}

// capabilities returns the adapter capability set of a sync endpoint that no
// configured route references.
func (syncer *modelSyncRuntime) capabilities(endpointID string) (provider.CapabilitySet, bool) {
	sources := append([]modelsync.Source{syncer.openrouter}, syncer.direct...)
	for _, source := range sources {
		if source.EndpointID == endpointID {
			return completeCapabilities(routeProviderCapabilities(routing.Route{Capabilities: source.Capabilities, ProviderFeatures: source.ProviderFeatures})), true
		}
	}
	return provider.CapabilitySet{}, false
}

// enable records which sync endpoints have adapters and installs the
// rules-only catalog, so extra models route before the first Document.
func (syncer *modelSyncRuntime) enable(adapters map[string]provider.Adapter) error {
	syncer.enabled = make(map[string]struct{}, len(adapters))
	for endpointID := range adapters {
		syncer.enabled[endpointID] = struct{}{}
	}
	return syncer.install(nil)
}

func (syncer *modelSyncRuntime) Current(ctx context.Context) (engine.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return engine.Snapshot{}, err
	}
	return *syncer.current.Load(), nil
}

// install compiles one Document over the base snapshot and publishes it.
func (syncer *modelSyncRuntime) install(document *modelsync.Document) error {
	input := modelsync.Input{Document: document, Rules: syncer.rules, Reserved: syncer.reserved}
	if _, ok := syncer.enabled[syncer.openrouter.EndpointID]; ok {
		source := syncer.openrouter
		input.OpenRouter = &source
	}
	for _, source := range syncer.direct {
		if _, ok := syncer.enabled[source.EndpointID]; ok {
			input.Direct = append(input.Direct, source)
		}
	}
	compiled, err := modelsync.Compile(input)
	if err != nil {
		return err
	}
	models := make(map[string]routing.Model, len(syncer.base.Routes.Models)+len(compiled.Models))
	for name, model := range syncer.base.Routes.Models {
		models[name] = model
	}
	for name, model := range compiled.Models {
		models[name] = model
	}
	routes, err := routing.CompileCatalog(syncer.base.Routes.Version, models)
	if err != nil {
		return fmt.Errorf("compile synced routes: %w", err)
	}
	configured := make(map[string]struct{}, len(syncer.basePrices.Entries))
	entries := append([]pricing.Entry(nil), syncer.basePrices.Entries...)
	for _, entry := range entries {
		configured[priceIdentity(entry)] = struct{}{}
	}
	for _, entry := range compiled.Prices {
		if _, exists := configured[priceIdentity(entry)]; !exists {
			entries = append(entries, entry)
		}
	}
	version := syncer.basePrices.Version
	if version == "" {
		version = "runtime-prices/" + syncer.base.Version
	}
	if document != nil {
		encoded, err := document.Encode()
		if err != nil {
			return err
		}
		version += "/openrouter-" + modelsync.Digest(encoded)[:16]
	}
	prices, err := pricing.CompileUSD(version, entries)
	if err != nil {
		return fmt.Errorf("compile synced prices: %w", err)
	}
	next := syncer.base
	next.Routes = routes
	next.Prices = pricing.NewResolver(prices)
	syncer.current.Store(&next)
	return nil
}

func priceIdentity(entry pricing.Entry) string {
	return strings.Join([]string{entry.Provider, entry.Family, entry.EndpointID, entry.Region, entry.Model, entry.ProviderTier}, "\x00")
}

// start installs the published Document, if any, and then runs the refresher
// until stop. httpClient must enforce the OpenRouter endpoint's egress policy.
func (syncer *modelSyncRuntime) start(ctx context.Context, value config.Config, store modelsync.Store, httpClient *http.Client, apiKey string, observe func(modelsync.Event)) {
	minimum, maximum := value.ModelSync.RefreshIntervalMin, value.ModelSync.RefreshIntervalMax
	syncer.refresher = &modelsync.Refresher{
		Store: store,
		Fetch: modelsync.Fetcher{Client: httpClient, BaseURL: value.Endpoints[value.ModelSync.OpenRouter.Endpoint].BaseURL, APIKey: apiKey}.Fetch,
		Install: func(document modelsync.Document) error {
			return syncer.install(&document)
		},
		MinInterval: time.Duration(minimum),
		MaxInterval: time.Duration(maximum),
		Observe:     observe,
	}
	// A published Document is installed before the snapshot serves requests,
	// so a restarted worker routes synced models immediately.
	_, _ = syncer.refresher.Sync(ctx)
	runContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
	syncer.cancel, syncer.done = cancel, make(chan struct{})
	go func() {
		defer close(syncer.done)
		syncer.refresher.Run(runContext)
	}()
}

// stop ends the refresher and waits for it, bounded by ctx.
func (syncer *modelSyncRuntime) stop(ctx context.Context) {
	syncer.stopOnce.Do(func() {
		if syncer.cancel == nil {
			return
		}
		syncer.cancel()
		select {
		case <-syncer.done:
		case <-ctx.Done():
		}
	})
}

// modelSyncLogger forwards refresher events to the runtime logger once the
// runtime exists; events before then are dropped. Only bounded classes are
// logged, never a fetched value.
type modelSyncLogger struct {
	logger atomic.Pointer[observability.Logger]
}

func (log *modelSyncLogger) observe(event modelsync.Event) {
	logger := log.logger.Load()
	if logger == nil {
		return
	}
	attrs := []slog.Attr{slog.String("source", "model_sync"), slog.String("phase", event.Step), slog.String("outcome", string(event.Outcome))}
	if event.Cause != "" {
		attrs = append(attrs, slog.String("cause", event.Cause))
	}
	if event.Outcome == modelsync.OutcomeFailed {
		logger.Warn(context.Background(), "model catalog sync step failed", attrs...)
		return
	}
	logger.Info(context.Background(), "model catalog sync step completed", attrs...)
}
