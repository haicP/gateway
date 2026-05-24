package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	_ "time/tzdata" // embed IANA timezone data for scratch container images

	"github.com/ongoingai/gateway/internal/api"
	"github.com/ongoingai/gateway/internal/auth"
	"github.com/ongoingai/gateway/internal/backup"
	"github.com/ongoingai/gateway/internal/config"
	"github.com/ongoingai/gateway/internal/configstore"
	"github.com/ongoingai/gateway/internal/correlation"
	"github.com/ongoingai/gateway/internal/limits"
	"github.com/ongoingai/gateway/internal/observability"
	"github.com/ongoingai/gateway/internal/pathutil"
	"github.com/ongoingai/gateway/internal/providers"
	"github.com/ongoingai/gateway/internal/proxy"
	"github.com/ongoingai/gateway/internal/trace"
	"github.com/ongoingai/gateway/internal/version"
)

const defaultConfigPath = "ongoingai.yaml"

const gatewayKeyRefreshInterval = 30 * time.Second
const gatewayKeyCacheMaxStaleness = 60 * time.Second
const traceWriterShutdownTimeout = 5 * time.Second
const otelShutdownTimeout = 5 * time.Second
const requestDetailsBackupSchedulerShutdownTimeout = 5 * time.Second
const serverReadHeaderTimeout = 10 * time.Second
const serverReadTimeout = 30 * time.Second
const serverIdleTimeout = 2 * time.Minute

var newPostgresGatewayKeyStore = func(dsn string) (configstore.GatewayKeyStore, error) {
	return configstore.NewPostgresStore(dsn)
}

type asyncTraceWriter interface {
	Start(ctx context.Context)
	Enqueue(t *trace.Trace) bool
	Stop()
	Shutdown(ctx context.Context) error
}

type traceWriteFailureHandlerSetter interface {
	SetWriteFailureHandler(handler trace.WriteFailureHandler)
}

type traceWriterMetricsSetter interface {
	SetMetrics(m *trace.WriterMetrics)
}

type traceWriterQueueLenProvider interface {
	QueueLen() int
}

type traceWriterQueueCapProvider interface {
	QueueCap() int
}

var newTraceWriter = func(store trace.TraceStore, bufferSize int) asyncTraceWriter {
	return trace.NewWriter(store, bufferSize)
}

type requestDetailsBackupScheduler interface {
	Start(ctx context.Context)
	Shutdown(ctx context.Context) error
}

var newRequestDetailsBackupScheduler = func(cfg config.BackupRequestDetailsConfig, store trace.TraceStore, logger *slog.Logger) (requestDetailsBackupScheduler, error) {
	return newRequestDetailsBackupSchedulerFromConfig(context.Background(), cfg, store, logger)
}

var signalNotifyContext = signal.NotifyContext

type gatewayAuthorizerCache struct {
	mu          sync.RWMutex
	authorizer  *auth.Authorizer
	lastRefresh time.Time
}

func newGatewayAuthorizerCache(authorizer *auth.Authorizer, refreshedAt time.Time) *gatewayAuthorizerCache {
	return &gatewayAuthorizerCache{
		authorizer:  authorizer,
		lastRefresh: refreshedAt.UTC(),
	}
}

func (c *gatewayAuthorizerCache) Current(maxStaleness time.Duration) (*auth.Authorizer, error) {
	if c == nil {
		return nil, errors.New("gateway authorizer cache is not initialized")
	}

	c.mu.RLock()
	authorizer := c.authorizer
	lastRefresh := c.lastRefresh
	c.mu.RUnlock()

	if authorizer == nil {
		return nil, errors.New("gateway authorizer cache has no authorizer")
	}
	// Fail closed when the cache is too old so revoked keys are not accepted
	// indefinitely if background refresh is failing.
	if maxStaleness > 0 && !lastRefresh.IsZero() && time.Since(lastRefresh) > maxStaleness {
		return nil, fmt.Errorf("gateway authorizer cache is stale (last refresh %s)", lastRefresh.Format(time.RFC3339))
	}
	return authorizer, nil
}

func (c *gatewayAuthorizerCache) Set(authorizer *auth.Authorizer, refreshedAt time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.authorizer = authorizer
	c.lastRefresh = refreshedAt.UTC()
	c.mu.Unlock()
}

func startGatewayAuthRefresher(
	ctx context.Context,
	cache *gatewayAuthorizerCache,
	cfg config.Config,
	gatewayKeyStore configstore.GatewayKeyStore,
	logger *slog.Logger,
) {
	startGatewayAuthRefresherWithInterval(ctx, cache, cfg, gatewayKeyStore, logger, gatewayKeyRefreshInterval)
}

func startGatewayAuthRefresherWithInterval(
	ctx context.Context,
	cache *gatewayAuthorizerCache,
	cfg config.Config,
	gatewayKeyStore configstore.GatewayKeyStore,
	logger *slog.Logger,
	refreshInterval time.Duration,
) {
	if refreshInterval <= 0 {
		refreshInterval = gatewayKeyRefreshInterval
	}

	// Refresh the in-memory authorizer out-of-band so proxy requests avoid config
	// store round-trips while still picking up revocations/rotations quickly.
	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			keys, err := loadGatewayAuthKeys(ctx, cfg, logger, gatewayKeyStore)
			if err != nil {
				if logger != nil {
					logger.Error("failed to refresh gateway key cache", "error", err)
				}
				continue
			}
			nextAuthorizer, err := auth.NewAuthorizer(auth.Options{
				Enabled: cfg.Auth.Enabled,
				Header:  cfg.Auth.Header,
				Keys:    keys,
			})
			if err != nil {
				if logger != nil {
					logger.Error("failed to rebuild authorizer from refreshed gateway keys", "error", err)
				}
				continue
			}
			cache.Set(nextAuthorizer, time.Now().UTC())
			if logger != nil {
				logger.Debug("refreshed gateway key cache", "key_count", len(keys))
			}
		}
	}
}

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		return runServe(nil)
	}

	switch args[0] {
	case "version", "--version", "-v":
		fmt.Println(version.String())
		return 0
	case "serve":
		return runServe(args[1:])
	case "config":
		return runConfig(args[1:], os.Stdout, os.Stderr)
	case "shell-init":
		return runShellInit(args[1:], os.Stdout, os.Stderr)
	case "wrap":
		return runWrap(args[1:], os.Stdout, os.Stderr)
	case "report":
		return runReport(args[1:], os.Stdout, os.Stderr)
	case "debug":
		return runDebug(args[1:], os.Stdout, os.Stderr)
	case "doctor":
		return runDoctor(args[1:], os.Stdout, os.Stderr)
	case "diagnostics":
		return runDiagnostics(args[1:], os.Stdout, os.Stderr)
	default:
		printUsage(os.Stderr)
		return 2
	}
}

func runShellInit(args []string, out io.Writer, errOut io.Writer) int {
	flagSet := flag.NewFlagSet("shell-init", flag.ContinueOnError)
	flagSet.SetOutput(errOut)
	configPath := flagSet.String("config", defaultConfigPath, "Path to config file")
	if err := flagSet.Parse(args); err != nil {
		return 2
	}
	if flagSet.NArg() != 0 {
		fmt.Fprintln(errOut, "shell-init does not accept positional arguments")
		return 2
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(errOut, "failed to load config: %v\n", err)
		return 1
	}

	fmt.Fprint(out, shellInitScript(cfg))
	return 0
}

func runConfig(args []string, out io.Writer, errOut io.Writer) int {
	if len(args) == 0 {
		printConfigUsage(errOut)
		return 2
	}

	switch args[0] {
	case "validate":
		return runConfigValidate(args[1:], out, errOut)
	default:
		printConfigUsage(errOut)
		return 2
	}
}

func runConfigValidate(args []string, out io.Writer, errOut io.Writer) int {
	flagSet := flag.NewFlagSet("config validate", flag.ContinueOnError)
	flagSet.SetOutput(errOut)
	configPath := flagSet.String("config", defaultConfigPath, "Path to config file")
	if err := flagSet.Parse(args); err != nil {
		return 2
	}
	if flagSet.NArg() != 0 {
		fmt.Fprintln(errOut, "config validate does not accept positional arguments")
		return 2
	}

	_, _, err := loadAndValidateConfig(*configPath)
	if err != nil {
		fmt.Fprintf(errOut, "config is invalid: %v\n", err)
		return 1
	}

	fmt.Fprintf(out, "config is valid: %s\n", *configPath)
	return 0
}

func runWrap(args []string, out io.Writer, errOut io.Writer) int {
	flagSet := flag.NewFlagSet("wrap", flag.ContinueOnError)
	flagSet.SetOutput(errOut)
	configPath := flagSet.String("config", defaultConfigPath, "Path to config file")
	if err := flagSet.Parse(args); err != nil {
		return 2
	}

	cmdArgs := flagSet.Args()
	if len(cmdArgs) > 0 && cmdArgs[0] == "--" {
		cmdArgs = cmdArgs[1:]
	}
	if len(cmdArgs) == 0 {
		fmt.Fprintln(errOut, "usage: ongoingai wrap [--config path/to/ongoingai.yaml] -- <command> [args...]")
		return 2
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(errOut, "failed to load config: %v\n", err)
		return 1
	}

	return runWrappedCommand(cfg, cmdArgs, out, errOut)
}

func runWrappedCommand(cfg config.Config, cmdArgs []string, out io.Writer, errOut io.Writer) int {
	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = out
	cmd.Stderr = errOut
	cmd.Env = gatewayCommandEnv(cfg, os.Environ())

	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return exitErr.ExitCode()
		}
		fmt.Fprintf(errOut, "failed to start command: %v\n", err)
		return 1
	}

	return 0
}

func runServe(args []string) int {
	flagSet := flag.NewFlagSet("serve", flag.ContinueOnError)
	flagSet.SetOutput(os.Stderr)
	configPath := flagSet.String("config", defaultConfigPath, "Path to config file")
	if err := flagSet.Parse(args); err != nil {
		return 2
	}

	cfg, stage, err := loadAndValidateConfig(*configPath)
	if err != nil {
		if stage == configStageLoad {
			fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "config is invalid: %v\n", err)
		}
		return 1
	}

	logger := slog.New(observability.NewTraceLogHandler(
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
	))
	otelRuntime, otelErr := observability.Setup(context.Background(), cfg.Observability.OTel, version.String(), logger)
	if otelErr != nil {
		logger.Error("failed to initialize opentelemetry; continuing with instrumentation disabled", "error", otelErr)
	}
	if otelRuntime != nil {
		defer shutdownOpenTelemetry(logger, otelRuntime, otelShutdownTimeout)
	}

	var traceStore trace.TraceStore
	var traceWriter asyncTraceWriter
	switch cfg.Storage.Driver {
	case "sqlite":
		sqliteStore, err := trace.NewSQLiteStore(cfg.Storage.Path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to initialize sqlite storage: %v\n", err)
			return 1
		}
		defer func() {
			if err := sqliteStore.Close(); err != nil {
				logger.Error("failed to close sqlite storage", "error", err)
			}
		}()

		traceStore = sqliteStore
		// Keep enough headroom for short proxy bursts while preserving explicit
		// backpressure (drop on full queue) if storage falls behind.
		traceWriter = newTraceWriter(traceStore, 1024)
		traceWriter.Start(context.Background())
	case "postgres":
		postgresStore, err := trace.NewPostgresStore(cfg.Storage.DSN)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to initialize postgres storage: %v\n", err)
			return 1
		}
		defer func() {
			if err := postgresStore.Close(); err != nil {
				logger.Error("failed to close postgres storage", "error", err)
			}
		}()

		traceStore = postgresStore
		// Keep enough headroom for short proxy bursts while preserving explicit
		// backpressure (drop on full queue) if storage falls behind.
		traceWriter = newTraceWriter(traceStore, 1024)
		traceWriter.Start(context.Background())
	default:
		fmt.Fprintf(os.Stderr, "unsupported storage.driver %q\n", cfg.Storage.Driver)
		return 1
	}
	llmResponseContentEnricher := newLLMResponseContentEnricher(
		traceStore,
		logger,
		llmResponseContentEnrichBufferSize,
		llmResponseContentEnrichTimeout,
	)
	if llmResponseContentEnricher != nil {
		llmResponseContentEnricher.Start(context.Background())
		defer shutdownLLMResponseContentEnricher(logger, llmResponseContentEnricher, llmResponseContentShutdownTimeout)
	}
	attachTraceWriterMetrics(traceWriter, otelRuntime, llmResponseContentEnricher)
	attachTraceWriterFailureLogging(logger, traceWriter, func(failure trace.WriteFailure) {
		if otelRuntime != nil {
			otelRuntime.RecordTraceWriteFailure(failure.Operation, failure.FailedCount, failure.ErrorClass, cfg.Storage.Driver)
		}
	})
	defer shutdownTraceWriter(logger, traceWriter, traceWriterShutdownTimeout)
	var tracePipelineReader trace.TracePipelineDiagnosticsReader
	if reader, ok := traceWriter.(trace.TracePipelineDiagnosticsReader); ok {
		tracePipelineReader = reader
	}
	var backupScheduler requestDetailsBackupScheduler
	if cfg.Backup.RequestDetails.Enabled {
		backupScheduler, err = newRequestDetailsBackupScheduler(cfg.Backup.RequestDetails, traceStore, logger)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to initialize request details backup scheduler: %v\n", err)
			return 1
		}
	}

	gatewayKeyStore, err := newGatewayKeyStore(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize gateway key store: %v\n", err)
		return 1
	}
	defer func() {
		if err := gatewayKeyStore.Close(); err != nil {
			logger.Error("failed to close gateway key store", "error", err)
		}
	}()

	authKeys, err := loadGatewayAuthKeys(context.Background(), cfg, logger, gatewayKeyStore)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize auth config: %v\n", err)
		return 1
	}
	authorizer, err := auth.NewAuthorizer(auth.Options{
		Enabled: cfg.Auth.Enabled,
		Header:  cfg.Auth.Header,
		Keys:    authKeys,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize auth config: %v\n", err)
		return 1
	}

	providerRegistry := providers.DefaultRegistry()
	proxyAuditRecorder := newProxyAuthAuditRecorder(logger)
	gatewayKeyAuditRecorder := newGatewayKeyAuditRecorder(logger)
	apiHandler := api.NewRouter(api.RouterOptions{
		AppVersion:              version.String(),
		Store:                   traceStore,
		StorageDriver:           cfg.Storage.Driver,
		StoragePath:             cfg.Storage.Path,
		TracePipelineReader:     tracePipelineReader,
		GatewayAuthHeader:       cfg.Auth.Header,
		GatewayKeyStore:         gatewayKeyStore,
		GatewayKeyAuditRecorder: gatewayKeyAuditRecorder,
	})
	proxyOptions := proxy.HandlerOptions{}
	if otelRuntime != nil {
		proxyOptions.Transport = otelRuntime.WrapHTTPTransport(http.DefaultTransport)
	}
	proxyHandler, err := proxy.NewHandlerWithOptions(proxyRoutesFromConfig(cfg), logger, apiHandler, proxyOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to configure proxy routes: %v\n", err)
		return 1
	}
	guardedProxyHandler := piiGuardrailMiddleware(cfg, logger, proxyHandler)
	if otelRuntime != nil {
		guardedProxyHandler = otelRuntime.WrapRouteSpan(guardedProxyHandler)
	}

	captureSink := func(exchange *proxy.CapturedExchange) {
		if !shouldCaptureTrace(exchange.Path) {
			exchange.CleanupBodyFiles()
			return
		}

		traceRecord := buildTraceRecord(cfg, providerRegistry, exchange)
		enqueueCtx := exchange.Context
		if enqueueCtx == nil {
			enqueueCtx = context.Background()
		}
		if otelRuntime != nil {
			otelRuntime.RecordProviderRequest(
				enqueueCtx,
				traceRecord.Provider,
				traceRecord.Model,
				exchange.GatewayOrgID,
				exchange.GatewayWorkspaceID,
				exchange.Path,
				exchange.StatusCode,
				exchange.DurationMS,
			)
			otelRuntime.RecordProxyRequest(
				enqueueCtx,
				traceRecord.Provider,
				traceRecord.Model,
				exchange.GatewayOrgID,
				exchange.GatewayWorkspaceID,
				exchange.Path,
				exchange.StatusCode,
				exchange.DurationMS,
			)
		}
		_, endEnqueueSpan := otelRuntime.StartTraceEnqueueSpan(
			enqueueCtx,
			traceRecord.Provider,
			traceRecord.Model,
			exchange.GatewayOrgID,
			exchange.GatewayWorkspaceID,
			exchange.Path,
		)
		queued := traceWriter.Enqueue(traceRecord)
		endEnqueueSpan(queued)

		if !queued {
			traceRecord.CleanupBodyFiles()
			logger.WarnContext(enqueueCtx,
				"trace queue is full; dropping trace",
				"correlation_id", strings.TrimSpace(exchange.CorrelationID),
				"path", exchange.Path,
				"status", exchange.StatusCode,
			)
			if otelRuntime != nil {
				otelRuntime.RecordTraceQueueDrop(
					traceRecord.Provider,
					traceRecord.Model,
					exchange.GatewayOrgID,
					exchange.GatewayWorkspaceID,
					exchange.Path,
					exchange.StatusCode,
				)
			}
		}

		logger.DebugContext(enqueueCtx,
			"captured exchange",
			"correlation_id", strings.TrimSpace(exchange.CorrelationID),
			"method", exchange.Method,
			"path", exchange.Path,
			"status", exchange.StatusCode,
			"streaming", exchange.Streaming,
			"stream_chunks", exchange.StreamChunks,
			"ttft_ms", exchange.TimeToFirstTokenMS,
			"ttft_us", exchange.TimeToFirstTokenUS,
			"request_body_bytes", len(exchange.RequestBody),
			"response_body_bytes", len(exchange.ResponseBody),
			"duration_ms", exchange.DurationMS,
		)
	}
	captureHandler := proxy.BodyCaptureMiddleware(proxy.BodyCaptureOptions{
		Enabled:      cfg.Tracing.CaptureBodies,
		ParseBodies:  true,
		MaxBodySize:  cfg.Tracing.BodyMaxSize,
		ParseMaxSize: metadataParseMaxSize,
	}, captureSink, guardedProxyHandler)
	if otelRuntime != nil {
		captureHandler = otelRuntime.SpanEnrichmentMiddleware(captureHandler)
	}
	gatewayLimiter := limits.NewGatewayLimiter(traceStore, limits.Config{
		PerKey: limits.Policy{
			RequestsPerMinute: cfg.Limits.PerKey.RequestsPerMinute,
			MaxTokensPerDay:   cfg.Limits.PerKey.MaxTokensPerDay,
			MaxCostUSDPerDay:  cfg.Limits.PerKey.MaxCostUSDPerDay,
		},
		PerWorkspace: limits.Policy{
			RequestsPerMinute: cfg.Limits.PerWorkspace.RequestsPerMinute,
			MaxTokensPerDay:   cfg.Limits.PerWorkspace.MaxTokensPerDay,
			MaxCostUSDPerDay:  cfg.Limits.PerWorkspace.MaxCostUSDPerDay,
		},
	})
	authOptions := auth.MiddlewareOptions{
		APIPrefix:     "/api",
		Providers:     authProviderRoutesFromConfig(cfg),
		ProxyLimiter:  gatewayLimiter.CheckRequest,
		AuditRecorder: proxyAuditRecorder,
	}
	if usageTracker, ok := gatewayKeyStore.(configstore.GatewayKeyUsageTracker); ok {
		authOptions.ProxyUsageRecorder = newGatewayKeyProxyUsageRecorder(logger, usageTracker)
	}
	protectedHandler := auth.Middleware(authorizer, authOptions, captureHandler)
	var authorizerCache *gatewayAuthorizerCache
	if cfg.Auth.Enabled && strings.TrimSpace(cfg.Storage.Driver) == "postgres" {
		authorizerCache = newGatewayAuthorizerCache(authorizer, time.Now().UTC())
		protectedHandler = auth.DynamicMiddleware(func(_ *http.Request) (*auth.Authorizer, error) {
			return authorizerCache.Current(gatewayKeyCacheMaxStaleness)
		}, authOptions, captureHandler)
	}
	if otelRuntime != nil {
		protectedHandler = otelRuntime.WrapAuthMiddleware(protectedHandler)
	}

	serverHandler := protectedHandler
	if otelRuntime != nil {
		serverHandler = otelRuntime.WrapHTTPHandler(serverHandler)
	}
	if otelRuntime != nil && otelRuntime.PrometheusHandler() != nil {
		promPath := cfg.Observability.OTel.PrometheusPath
		promHandler := otelRuntime.PrometheusHandler()
		inner := serverHandler
		serverHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == promPath {
				promHandler.ServeHTTP(w, r)
				return
			}
			inner.ServeHTTP(w, r)
		})
	}
	server := newGatewayServer(cfg, logger, serverHandler)

	logger.Info(
		"startup banner",
		"version", version.String(),
		"addr", server.Addr,
		"port", cfg.Server.Port,
		"storage_driver", cfg.Storage.Driver,
		"providers", configuredProviderSummaries(cfg),
		"config_path", *configPath,
		"auth_enabled", cfg.Auth.Enabled,
		"prometheus_enabled", cfg.Observability.OTel.PrometheusEnabled,
		"prometheus_path", cfg.Observability.OTel.PrometheusPath,
		"request_details_backup_enabled", cfg.Backup.RequestDetails.Enabled,
	)

	ctx, stop := signalNotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if backupScheduler != nil {
		backupScheduler.Start(ctx)
		defer shutdownRequestDetailsBackupScheduler(logger, backupScheduler, requestDetailsBackupSchedulerShutdownTimeout)
	}
	if authorizerCache != nil {
		go startGatewayAuthRefresher(ctx, authorizerCache, cfg, gatewayKeyStore, logger)
	}

	errCh := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("failed to shutdown", "error", err)
			return 1
		}
		logger.Info("gateway stopped")
		return 0
	case err := <-errCh:
		if err != nil {
			logger.Error("gateway failed", "error", err)
			return 1
		}
		return 0
	}
}

func gatewayCommandEnv(cfg config.Config, baseEnv []string) []string {
	openAIURL, anthropicURL := gatewayProviderURLs(cfg)
	envMap := make(map[string]string, len(baseEnv)+2)
	for _, kv := range baseEnv {
		eq := strings.IndexByte(kv, '=')
		if eq <= 0 {
			continue
		}
		envMap[kv[:eq]] = kv[eq+1:]
	}
	envMap["OPENAI_BASE_URL"] = openAIURL
	envMap["ANTHROPIC_BASE_URL"] = anthropicURL

	keys := make([]string, 0, len(envMap))
	for key := range envMap {
		keys = append(keys, key)
	}
	// Keep environment ordering stable for tests and reproducible subprocess env.
	sort.Strings(keys)

	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key+"="+envMap[key])
	}
	return out
}

func newGatewayServer(cfg config.Config, logger *slog.Logger, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              cfg.Server.Address(),
		Handler:           proxy.LoggingMiddleware(logger, handler),
		ReadHeaderTimeout: serverReadHeaderTimeout,
		ReadTimeout:       serverReadTimeout,
		IdleTimeout:       serverIdleTimeout,
	}
}

type requestDetailsTraceExporter struct {
	store    trace.TraceExporter
	pageSize int
}

func (e requestDetailsTraceExporter) ExportTraces(ctx context.Context, window backup.Window, yield func(*trace.Trace) error) error {
	if e.store == nil {
		return errors.New("trace exporter is not configured")
	}
	limit := e.pageSize
	if limit <= 0 {
		limit = 500
	}

	cursor := ""
	for {
		result, err := e.store.ExportTraces(ctx, trace.TraceExportFilter{
			From:   window.Start.UTC(),
			To:     window.End.UTC(),
			Limit:  limit,
			Cursor: cursor,
		})
		if err != nil {
			return err
		}
		for _, item := range result.Items {
			if err := yield(item); err != nil {
				return err
			}
		}
		if result.NextCursor == "" {
			return nil
		}
		cursor = result.NextCursor
	}
}

type requestDetailsBackupSchedulerRunner struct {
	scheduler *backup.Scheduler
	logger    *slog.Logger

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan error
	started bool
}

func (r *requestDetailsBackupSchedulerRunner) Start(ctx context.Context) {
	if r == nil || r.scheduler == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return
	}
	workerCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.done = make(chan error, 1)
	r.started = true
	r.mu.Unlock()

	go func() {
		err := r.scheduler.Start(workerCtx)
		r.done <- err
		close(r.done)
		if err != nil && !errors.Is(err, context.Canceled) && r.logger != nil {
			r.logger.Error("request details backup scheduler stopped", "error", err)
		}
	}()
}

func (r *requestDetailsBackupSchedulerRunner) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	r.mu.Lock()
	cancel := r.cancel
	done := r.done
	started := r.started
	r.mu.Unlock()
	if !started {
		return nil
	}
	if cancel != nil {
		cancel()
	}

	select {
	case err := <-done:
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newRequestDetailsBackupSchedulerFromConfig(
	ctx context.Context,
	cfg config.BackupRequestDetailsConfig,
	store trace.TraceStore,
	logger *slog.Logger,
) (requestDetailsBackupScheduler, error) {
	exportStore, ok := store.(trace.TraceExporter)
	if !ok {
		return nil, errors.New("trace store does not support request details export")
	}
	loc, err := time.LoadLocation(strings.TrimSpace(cfg.Timezone))
	if err != nil {
		return nil, fmt.Errorf("load backup timezone: %w", err)
	}
	runAt, err := parseRequestDetailsBackupDailyAt(cfg.DailyAt)
	if err != nil {
		return nil, err
	}
	uploader, err := backup.NewS3Uploader(ctx, backup.S3UploaderConfig{
		Bucket:    cfg.S3.Bucket,
		Region:    cfg.S3.Region,
		Endpoint:  cfg.S3.Endpoint,
		PathStyle: cfg.S3.ForcePathStyle,
	})
	if err != nil {
		return nil, err
	}
	runner, err := backup.NewRunner(requestDetailsTraceExporter{
		store:    exportStore,
		pageSize: cfg.PageSize,
	}, uploader, backup.RunnerOptions{
		LocalDir:          cfg.TempDir,
		KeyPrefix:         cfg.S3.Prefix,
		MaxPartBytes:      cfg.ShardMaxBytes,
		MaxUploadAttempts: cfg.Retry.MaxAttempts,
		RetryBackoff:      time.Duration(cfg.Retry.InitialBackoffMS) * time.Millisecond,
		RetryMaxBackoff:   time.Duration(cfg.Retry.MaxBackoffMS) * time.Millisecond,
		Location:          loc,
	})
	if err != nil {
		return nil, err
	}
	scheduler, err := backup.NewScheduler(runner, backup.SchedulerOptions{
		Location: loc,
		RunAt:    runAt,
		ErrorHandler: func(err error) {
			if logger != nil {
				logger.Error("request details backup run failed", "error", err)
			}
		},
	})
	if err != nil {
		return nil, err
	}
	return &requestDetailsBackupSchedulerRunner{scheduler: scheduler, logger: logger}, nil
}

func parseRequestDetailsBackupDailyAt(raw string) (time.Duration, error) {
	parsed, err := time.Parse("15:04", strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("parse backup daily_at: %w", err)
	}
	return time.Duration(parsed.Hour())*time.Hour + time.Duration(parsed.Minute())*time.Minute, nil
}

func shellInitScript(cfg config.Config) string {
	openAIURL, anthropicURL := gatewayProviderURLs(cfg)
	return fmt.Sprintf("export OPENAI_BASE_URL=%s\nexport ANTHROPIC_BASE_URL=%s\n", openAIURL, anthropicURL)
}

func gatewayProviderURLs(cfg config.Config) (string, string) {
	base := gatewayBaseURL(cfg)
	prefix := primaryProviderPrefix(cfg)
	return base + openAIBasePath(prefix), base + prefix
}

func gatewayBaseURL(cfg config.Config) string {
	host := strings.TrimSpace(cfg.Server.Host)
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		host = "localhost"
	}
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") && !strings.HasSuffix(host, "]") {
		host = "[" + host + "]"
	}
	return "http://" + host + ":" + strconv.Itoa(cfg.Server.Port)
}

func openAIBasePath(prefix string) string {
	p := pathutil.NormalizePrefix(prefix)
	if strings.HasSuffix(p, "/v1") {
		return p
	}
	return strings.TrimRight(p, "/") + "/v1"
}

func primaryProviderPrefix(cfg config.Config) string {
	if provider, ok := cfg.Providers["llm"]; ok {
		return pathutil.NormalizePrefix(provider.Prefix)
	}
	names := sortedProviderNames(cfg.Providers)
	if len(names) == 0 {
		return "/llm"
	}
	return pathutil.NormalizePrefix(cfg.Providers[names[0]].Prefix)
}

func printUsage(out *os.File) {
	fmt.Fprintln(out, "Usage:")
	fmt.Fprintln(out, "  ongoingai serve [--config path/to/ongoingai.yaml]")
	fmt.Fprintln(out, "  ongoingai version")
	fmt.Fprintln(out, "  ongoingai config validate [--config path/to/ongoingai.yaml]")
	fmt.Fprintln(out, "  ongoingai shell-init [--config path/to/ongoingai.yaml]")
	fmt.Fprintln(out, "  ongoingai wrap [--config path/to/ongoingai.yaml] -- <command> [args...]")
	fmt.Fprintln(out, "  ongoingai report [--config path/to/ongoingai.yaml] [--format text|json] [--from RFC3339|YYYY-MM-DD] [--to RFC3339|YYYY-MM-DD] [--provider NAME] [--model NAME] [--limit N]")
	fmt.Fprintln(out, "  ongoingai debug [last] [--config path/to/ongoingai.yaml] [--trace-id ID] [--trace-group-id ID] [--thread-id ID] [--run-id ID] [--format text|json] [--limit N] [--diff] [--bundle-out PATH] [--include-headers] [--include-bodies]")
	fmt.Fprintln(out, "  ongoingai doctor [--config path/to/ongoingai.yaml] [--format text|json]")
	fmt.Fprintln(out, "  ongoingai diagnostics [trace-pipeline] [--config path/to/ongoingai.yaml] [--base-url URL] [--gateway-key TOKEN] [--auth-header HEADER] [--format text|json] [--timeout DURATION]")
}

func printConfigUsage(out io.Writer) {
	fmt.Fprintln(out, "Usage:")
	fmt.Fprintln(out, "  ongoingai config validate [--config path/to/ongoingai.yaml]")
}

func newGatewayKeyStore(cfg config.Config) (configstore.GatewayKeyStore, error) {
	if strings.TrimSpace(cfg.Storage.Driver) == "postgres" {
		store, err := newPostgresGatewayKeyStore(cfg.Storage.DSN)
		if err != nil {
			return nil, err
		}
		return store, nil
	}
	return configstore.NewStaticStore(configStoreKeysFromConfig(cfg.Auth.Keys)), nil
}

func loadGatewayAuthKeys(ctx context.Context, cfg config.Config, logger *slog.Logger, gatewayKeyStore configstore.GatewayKeyStore) ([]auth.KeyConfig, error) {
	configKeys := configStoreKeysFromConfig(cfg.Auth.Keys)
	if !cfg.Auth.Enabled {
		return authKeysFromStore(configKeys), nil
	}
	if strings.TrimSpace(cfg.Storage.Driver) != "postgres" {
		return authKeysFromStore(configKeys), nil
	}
	if gatewayKeyStore == nil {
		return nil, fmt.Errorf("gateway key store is not configured")
	}

	keys, err := gatewayKeyStore.ListGatewayKeys(ctx, configstore.GatewayKeyFilter{})
	if err != nil {
		return nil, fmt.Errorf("load gateway keys from postgres config store: %w", err)
	}
	if len(keys) == 0 {
		if logger != nil {
			logger.Info("postgres config store has no active gateway keys; using yaml auth.keys fallback", "fallback_key_count", len(configKeys))
		}
		return authKeysFromStore(configKeys), nil
	}
	if logger != nil {
		logger.Info("loaded gateway keys from postgres config store", "key_count", len(keys))
	}
	return authKeysFromStore(keys), nil
}

func configStoreKeysFromConfig(keys []config.GatewayKeyConfig) []configstore.GatewayKey {
	if len(keys) == 0 {
		return nil
	}

	out := make([]configstore.GatewayKey, 0, len(keys))
	for _, key := range keys {
		out = append(out, configstore.GatewayKey{
			ID:          key.ID,
			Token:       key.Token,
			OrgID:       key.OrgID,
			WorkspaceID: key.WorkspaceID,
			Team:        key.Team,
			Name:        key.Name,
			Description: key.Description,
			CreatedBy:   key.CreatedBy,
			Role:        key.Role,
			Permissions: append([]string(nil), key.Permissions...),
		})
	}
	return out
}

func authKeysFromStore(keys []configstore.GatewayKey) []auth.KeyConfig {
	if len(keys) == 0 {
		return nil
	}

	out := make([]auth.KeyConfig, 0, len(keys))
	for _, key := range keys {
		out = append(out, auth.KeyConfig{
			ID:          key.ID,
			Token:       key.Token,
			TokenHash:   key.TokenHash,
			OrgID:       key.OrgID,
			WorkspaceID: key.WorkspaceID,
			Team:        key.Team,
			Role:        key.Role,
			Permissions: append([]string(nil), key.Permissions...),
		})
	}
	return out
}

func newProxyAuthAuditRecorder(logger *slog.Logger) auth.AuditRecorder {
	if logger == nil {
		return nil
	}
	return func(req *http.Request, event auth.AuditEvent) {
		logger.WarnContext(req.Context(),
			"audit gateway auth deny",
			"correlation_id", requestCorrelationID(req),
			"audit_action", strings.TrimSpace(event.Action),
			"audit_outcome", strings.TrimSpace(event.Outcome),
			"audit_reason", strings.TrimSpace(event.Reason),
			"status_code", event.StatusCode,
			"path", strings.TrimSpace(event.Path),
			"audit_resource", strings.TrimSpace(event.Resource),
			"audit_resource_action", strings.TrimSpace(event.ResourceAction),
			"audit_scope", strings.TrimSpace(event.Scope),
			"provider", strings.TrimSpace(event.Provider),
			"required_permission", string(event.RequiredPermission),
			"key_id", strings.TrimSpace(event.KeyID),
			"org_id", strings.TrimSpace(event.OrgID),
			"workspace_id", strings.TrimSpace(event.WorkspaceID),
			"limit_code", strings.TrimSpace(event.LimitCode),
		)
	}
}

func newGatewayKeyAuditRecorder(logger *slog.Logger) api.GatewayKeyAuditRecorder {
	if logger == nil {
		return nil
	}
	return func(req *http.Request, event api.GatewayKeyAuditEvent) {
		logger.Info(
			"audit gateway key lifecycle",
			"correlation_id", requestCorrelationID(req),
			"audit_action", strings.TrimSpace(event.Action),
			"audit_outcome", strings.TrimSpace(event.Outcome),
			"audit_reason", strings.TrimSpace(event.Reason),
			"status_code", event.StatusCode,
			"actor_key_id", strings.TrimSpace(event.ActorKeyID),
			"org_id", strings.TrimSpace(event.OrgID),
			"workspace_id", strings.TrimSpace(event.WorkspaceID),
			"target_key_id", strings.TrimSpace(event.TargetKeyID),
		)
	}
}

func newGatewayKeyProxyUsageRecorder(logger *slog.Logger, usageTracker configstore.GatewayKeyUsageTracker) auth.ProxyUsageRecorder {
	if usageTracker == nil {
		return nil
	}
	return func(r *http.Request, identity *auth.Identity) {
		if identity == nil {
			return
		}
		keyID := strings.TrimSpace(identity.KeyID)
		if keyID == "" {
			return
		}
		filter := configstore.GatewayKeyFilter{
			OrgID:       strings.TrimSpace(identity.OrgID),
			WorkspaceID: strings.TrimSpace(identity.WorkspaceID),
		}
		ctx := context.Background()
		if r != nil {
			ctx = r.Context()
		}
		if err := usageTracker.TouchGatewayKeyLastUsed(ctx, keyID, filter); err != nil && logger != nil {
			logger.Warn(
				"failed to update gateway key last_used_at",
				"correlation_id", requestCorrelationID(r),
				"key_id", keyID,
				"org_id", filter.OrgID,
				"workspace_id", filter.WorkspaceID,
				"error", err,
			)
		}
	}
}

func shutdownTraceWriter(logger *slog.Logger, writer asyncTraceWriter, timeout time.Duration) {
	if writer == nil {
		return
	}

	start := time.Now()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := writer.Shutdown(shutdownCtx); err != nil {
		if logger != nil {
			logger.Error(
				"failed to flush pending traces before shutdown",
				"error", err,
				"timeout", timeout.String(),
			)
		}
		return
	}

	if logger != nil {
		logger.Info("flushed pending traces before shutdown", "duration_ms", time.Since(start).Milliseconds())
	}
}

func shutdownRequestDetailsBackupScheduler(logger *slog.Logger, scheduler requestDetailsBackupScheduler, timeout time.Duration) {
	if scheduler == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := scheduler.Shutdown(ctx); err != nil && logger != nil {
		logger.Error("failed to shutdown request details backup scheduler", "error", err, "timeout", timeout.String())
	}
}

func attachTraceWriterMetrics(writer asyncTraceWriter, otelRuntime *observability.Runtime, enricher *llmResponseContentEnricher) {
	if writer == nil {
		return
	}

	otelEnabled := otelRuntime != nil && otelRuntime.Enabled()
	if otelEnabled {
		if qlp, ok := writer.(traceWriterQueueLenProvider); ok {
			otelRuntime.RegisterTraceQueueDepthGauge(qlp.QueueLen)
		}
		if qcp, ok := writer.(traceWriterQueueCapProvider); ok {
			otelRuntime.RegisterTraceQueueCapacityGauge(qcp.QueueCap)
		}
	}

	ms, ok := writer.(traceWriterMetricsSetter)
	if !ok {
		return
	}
	metrics := &trace.WriterMetrics{}
	if otelEnabled {
		metrics.OnEnqueue = otelRuntime.RecordTraceEnqueued
		metrics.OnFlush = otelRuntime.RecordTraceFlush
		metrics.OnWriteStart = otelRuntime.MakeWriteSpanHook()
		metrics.OnWriteSuccess = otelRuntime.RecordTraceWritten
		// OnDrop left nil: the captureSink already calls RecordTraceQueueDrop
		// with richer route/status attributes.
	}
	if enricher != nil {
		metrics.OnWriteSuccessIDs = enricher.EnqueueTraceIDs
	}
	ms.SetMetrics(metrics)
}

func attachTraceWriterFailureLogging(logger *slog.Logger, writer asyncTraceWriter, onFailure func(trace.WriteFailure)) {
	if logger == nil || writer == nil {
		return
	}

	handlerSetter, ok := writer.(traceWriteFailureHandlerSetter)
	if !ok {
		return
	}

	handlerSetter.SetWriteFailureHandler(func(failure trace.WriteFailure) {
		if failure.FailedCount <= 0 {
			return
		}
		if onFailure != nil {
			onFailure(failure)
		}
		logger.Error(
			"trace persistence failed; dropped trace records",
			"operation", strings.TrimSpace(failure.Operation),
			"batch_size", failure.BatchSize,
			"failed_count", failure.FailedCount,
			"error_class", failure.ErrorClass,
			"error_kind", fmt.Sprintf("%T", failure.Err),
		)
	})
}

func shutdownOpenTelemetry(logger *slog.Logger, runtime *observability.Runtime, timeout time.Duration) {
	if runtime == nil || !runtime.Enabled() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := runtime.Shutdown(ctx); err != nil {
		if logger != nil {
			logger.Error("failed to shutdown opentelemetry providers", "error", err, "timeout", timeout.String())
		}
	}
}

func configuredProviderSummaries(cfg config.Config) []string {
	names := sortedProviderNames(cfg.Providers)
	out := make([]string, 0, len(names))
	for _, name := range names {
		provider := cfg.Providers[name]
		prefix := strings.TrimSpace(provider.Prefix)
		upstream := strings.TrimSpace(provider.Upstream)
		if prefix == "" || upstream == "" {
			continue
		}
		out = append(out, fmt.Sprintf("%s:%s->%s", name, pathutil.NormalizePrefix(prefix), upstream))
	}
	return out
}

func proxyRoutesFromConfig(cfg config.Config) []proxy.Route {
	names := sortedProviderNames(cfg.Providers)
	routes := make([]proxy.Route, 0, len(names))
	for _, name := range names {
		provider := cfg.Providers[name]
		routes = append(routes, proxy.Route{
			Prefix:   provider.Prefix,
			Upstream: provider.Upstream,
		})
	}
	return routes
}

func authProviderRoutesFromConfig(cfg config.Config) []auth.ProviderRoute {
	names := sortedProviderNames(cfg.Providers)
	routes := make([]auth.ProviderRoute, 0, len(names))
	for _, name := range names {
		routes = append(routes, auth.ProviderRoute{
			Name:   name,
			Prefix: cfg.Providers[name].Prefix,
		})
	}
	return routes
}

func sortedProviderNames(providers config.ProvidersConfig) []string {
	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func requestCorrelationID(req *http.Request) string {
	if req == nil {
		return ""
	}
	if id, ok := correlation.FromContext(req.Context()); ok {
		return id
	}
	return correlation.FromHeaders(req.Header)
}

func shouldCaptureTrace(path string) bool {
	return !pathutil.HasPathPrefix(path, "/api")
}
