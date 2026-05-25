package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ongoingai/gateway/internal/pathutil"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Storage   StorageConfig   `yaml:"storage"`
	Providers ProvidersConfig `yaml:"providers"`
	Tracing   TracingConfig   `yaml:"tracing"`
	Backup    BackupConfig    `yaml:"backup"`
}

type ServerConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

func (c ServerConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

type StorageConfig struct {
	Driver string `yaml:"driver"`
	Path   string `yaml:"path"`
	DSN    string `yaml:"dsn"`
}

type ProvidersConfig map[string]ProviderConfig

type ProviderConfig struct {
	Upstream string `yaml:"upstream"`
	Prefix   string `yaml:"prefix"`
}

func (providers *ProvidersConfig) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.MappingNode {
		return fmt.Errorf("providers must be a mapping")
	}

	next := ProvidersConfig{}
	for i := 0; i < len(value.Content); i += 2 {
		key := strings.TrimSpace(value.Content[i].Value)
		providerNode := value.Content[i+1]
		if providerNode.Kind != yaml.MappingNode {
			return fmt.Errorf("providers.%s must be a mapping", key)
		}

		var provider ProviderConfig
		for j := 0; j < len(providerNode.Content); j += 2 {
			field := strings.TrimSpace(providerNode.Content[j].Value)
			fieldNode := providerNode.Content[j+1]
			switch field {
			case "upstream":
				provider.Upstream = fieldNode.Value
			case "prefix":
				provider.Prefix = fieldNode.Value
			default:
				return fmt.Errorf("field %s not found in type config.ProviderConfig", field)
			}
		}
		next[key] = provider
	}

	*providers = next
	return nil
}

type TracingConfig struct {
	CaptureBodies bool                 `yaml:"capture_bodies"`
	BodyMaxSize   int                  `yaml:"body_max_size"`
	Retention     TraceRetentionConfig `yaml:"retention"`
}

type TraceRetentionConfig struct {
	Days            int    `yaml:"days"`
	CleanupEnabled  bool   `yaml:"cleanup_enabled"`
	CleanupDailyAt  string `yaml:"cleanup_daily_at"`
	CleanupTimezone string `yaml:"cleanup_timezone"`
}

type BackupConfig struct {
	RequestDetails BackupRequestDetailsConfig `yaml:"request_details"`
}

type BackupRequestDetailsConfig struct {
	Enabled       bool                      `yaml:"enabled"`
	Timezone      string                    `yaml:"timezone"`
	DailyAt       string                    `yaml:"daily_at"`
	TempDir       string                    `yaml:"temp_dir"`
	PageSize      int                       `yaml:"page_size"`
	ShardMaxBytes int64                     `yaml:"shard_max_bytes"`
	Retry         BackupRequestDetailsRetry `yaml:"retry"`
	S3            BackupRequestDetailsS3    `yaml:"s3"`
}

type BackupRequestDetailsRetry struct {
	MaxAttempts      int `yaml:"max_attempts"`
	InitialBackoffMS int `yaml:"initial_backoff_ms"`
	MaxBackoffMS     int `yaml:"max_backoff_ms"`
}

type BackupRequestDetailsS3 struct {
	Bucket         string `yaml:"bucket"`
	Prefix         string `yaml:"prefix"`
	Region         string `yaml:"region"`
	Endpoint       string `yaml:"endpoint"`
	ForcePathStyle bool   `yaml:"force_path_style"`
}

func Default() Config {
	return Config{
		Server: ServerConfig{
			Host: "0.0.0.0",
			Port: 8080,
		},
		Storage: StorageConfig{
			Driver: "sqlite",
			Path:   "./data/ongoingai.db",
		},
		Providers: ProvidersConfig{
			"llm": ProviderConfig{
				Upstream: "https://api.openai.com",
				Prefix:   "/llm",
			},
		},
		Tracing: TracingConfig{
			CaptureBodies: false,
			BodyMaxSize:   1 << 20,
			Retention: TraceRetentionConfig{
				Days:            14,
				CleanupEnabled:  false,
				CleanupDailyAt:  "02:00",
				CleanupTimezone: "Local",
			},
		},
		Backup: BackupConfig{
			RequestDetails: BackupRequestDetailsConfig{
				Enabled:       false,
				Timezone:      "Local",
				DailyAt:       "02:00",
				TempDir:       "./data/backup-tmp",
				PageSize:      500,
				ShardMaxBytes: 200 * 1024 * 1024,
				Retry: BackupRequestDetailsRetry{
					MaxAttempts:      5,
					InitialBackoffMS: 1000,
					MaxBackoffMS:     30000,
				},
				S3: BackupRequestDetailsS3{
					Prefix: "request-details",
				},
			},
		},
	}
}

func Load(path string) (Config, error) {
	cfg := Default()

	if path != "" {
		data, err := os.ReadFile(path)
		if err == nil {
			decoder := yaml.NewDecoder(bytes.NewReader(data))
			decoder.KnownFields(true)
			decodeErr := decoder.Decode(&cfg)
			if errors.Is(decodeErr, io.EOF) {
				decodeErr = nil
			}
			if decodeErr != nil {
				return Config{}, fmt.Errorf("parse yaml %q: %w", path, decodeErr)
			}
			var trailing any
			trailingErr := decoder.Decode(&trailing)
			if trailingErr != nil && !errors.Is(trailingErr, io.EOF) {
				return Config{}, fmt.Errorf("parse yaml %q: %w", path, trailingErr)
			}
			if trailing != nil {
				return Config{}, fmt.Errorf("parse yaml %q: multiple yaml documents are not supported", path)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return Config{}, fmt.Errorf("read config %q: %w", path, err)
		}
	}

	if err := applyEnv(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func Validate(cfg Config) error {
	if cfg.Server.Port <= 0 || cfg.Server.Port > 65535 {
		return fmt.Errorf("server.port must be between 1 and 65535 (got %d)", cfg.Server.Port)
	}

	switch driver := strings.TrimSpace(cfg.Storage.Driver); driver {
	case "sqlite":
		if strings.TrimSpace(cfg.Storage.Path) == "" {
			return errors.New("storage.path is required when storage.driver=sqlite")
		}
	case "postgres":
		if strings.TrimSpace(cfg.Storage.DSN) == "" {
			return errors.New("storage.dsn is required when storage.driver=postgres")
		}
	default:
		return fmt.Errorf("storage.driver must be one of sqlite, postgres (got %q)", cfg.Storage.Driver)
	}

	if _, err := validateProviders(cfg.Providers); err != nil {
		return err
	}
	if err := validateTraceRetentionConfig(cfg.Tracing.Retention); err != nil {
		return err
	}
	return validateBackupRequestDetailsConfig(cfg.Backup.RequestDetails)
}

func validateTraceRetentionConfig(cfg TraceRetentionConfig) error {
	if cfg.Days <= 0 {
		return fmt.Errorf("tracing.retention.days must be > 0 (got %d)", cfg.Days)
	}
	if !cfg.CleanupEnabled {
		return nil
	}
	if strings.TrimSpace(cfg.CleanupTimezone) == "" {
		return errors.New("tracing.retention.cleanup_timezone is required when tracing.retention.cleanup_enabled=true")
	}
	if _, err := time.LoadLocation(strings.TrimSpace(cfg.CleanupTimezone)); err != nil {
		return fmt.Errorf("tracing.retention.cleanup_timezone must be a valid IANA timezone (got %q): %w", cfg.CleanupTimezone, err)
	}
	if strings.TrimSpace(cfg.CleanupDailyAt) == "" {
		return errors.New("tracing.retention.cleanup_daily_at is required when tracing.retention.cleanup_enabled=true")
	}
	if _, err := time.Parse("15:04", strings.TrimSpace(cfg.CleanupDailyAt)); err != nil {
		return fmt.Errorf("tracing.retention.cleanup_daily_at must use HH:MM 24-hour format (got %q): %w", cfg.CleanupDailyAt, err)
	}
	return nil
}

func validateBackupRequestDetailsConfig(cfg BackupRequestDetailsConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if strings.TrimSpace(cfg.S3.Bucket) == "" {
		return errors.New("backup.request_details.s3.bucket is required when backup.request_details.enabled=true")
	}
	if strings.TrimSpace(cfg.Timezone) == "" {
		return errors.New("backup.request_details.timezone is required when backup.request_details.enabled=true")
	}
	if _, err := time.LoadLocation(strings.TrimSpace(cfg.Timezone)); err != nil {
		return fmt.Errorf("backup.request_details.timezone must be a valid IANA timezone (got %q): %w", cfg.Timezone, err)
	}
	if strings.TrimSpace(cfg.DailyAt) == "" {
		return errors.New("backup.request_details.daily_at is required when backup.request_details.enabled=true")
	}
	if _, err := time.Parse("15:04", strings.TrimSpace(cfg.DailyAt)); err != nil {
		return fmt.Errorf("backup.request_details.daily_at must use HH:MM 24-hour format (got %q): %w", cfg.DailyAt, err)
	}
	if strings.TrimSpace(cfg.TempDir) == "" {
		return errors.New("backup.request_details.temp_dir is required when backup.request_details.enabled=true")
	}
	if cfg.Retry.MaxAttempts <= 0 {
		return fmt.Errorf("backup.request_details.retry.max_attempts must be > 0 (got %d)", cfg.Retry.MaxAttempts)
	}
	if cfg.Retry.InitialBackoffMS <= 0 {
		return fmt.Errorf("backup.request_details.retry.initial_backoff_ms must be > 0 (got %d)", cfg.Retry.InitialBackoffMS)
	}
	if cfg.Retry.MaxBackoffMS <= 0 {
		return fmt.Errorf("backup.request_details.retry.max_backoff_ms must be > 0 (got %d)", cfg.Retry.MaxBackoffMS)
	}
	if cfg.Retry.MaxBackoffMS < cfg.Retry.InitialBackoffMS {
		return fmt.Errorf("backup.request_details.retry.max_backoff_ms must be >= initial_backoff_ms (got %d < %d)", cfg.Retry.MaxBackoffMS, cfg.Retry.InitialBackoffMS)
	}
	if cfg.PageSize <= 0 {
		return fmt.Errorf("backup.request_details.page_size must be > 0 (got %d)", cfg.PageSize)
	}
	if cfg.ShardMaxBytes <= 0 {
		return fmt.Errorf("backup.request_details.shard_max_bytes must be > 0 (got %d)", cfg.ShardMaxBytes)
	}
	if endpoint := strings.TrimSpace(cfg.S3.Endpoint); endpoint != "" {
		parsed, err := url.Parse(endpoint)
		if err != nil {
			return fmt.Errorf("parse backup.request_details.s3.endpoint: %w", err)
		}
		if strings.TrimSpace(parsed.Scheme) == "" || strings.TrimSpace(parsed.Host) == "" {
			return fmt.Errorf("backup.request_details.s3.endpoint must include scheme and host (got %q)", cfg.S3.Endpoint)
		}
	}
	return nil
}

func validateProviders(providers ProvidersConfig) ([]string, error) {
	if len(providers) == 0 {
		return nil, errors.New("providers must configure at least one provider")
	}

	prefixes := make([]string, 0, len(providers))
	for name, provider := range providers {
		normalizedName := strings.TrimSpace(name)
		if err := validateProviderName(normalizedName); err != nil {
			return nil, err
		}
		configName := "providers." + normalizedName
		if err := validateProvider(configName, provider); err != nil {
			return nil, err
		}
		prefix := pathutil.NormalizePrefix(provider.Prefix)
		if prefix == "/" {
			return nil, fmt.Errorf("%s.prefix must not be root ('/')", configName)
		}
		if prefixesOverlap(prefix, "/api") {
			return nil, fmt.Errorf("%s.prefix must not overlap with /api routes (got %q)", configName, provider.Prefix)
		}
		for _, existing := range prefixes {
			if prefixesOverlap(prefix, existing) {
				return nil, fmt.Errorf("provider prefixes must not overlap (got %q and %q)", existing, prefix)
			}
		}
		prefixes = append(prefixes, prefix)
	}
	return prefixes, nil
}

func validateProviderName(name string) error {
	if name == "" {
		return errors.New("provider name must not be empty")
	}
	for idx, r := range name {
		valid := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-'
		if !valid {
			return fmt.Errorf("provider name %q must contain only lowercase letters, numbers, hyphen, or underscore", name)
		}
		if idx == 0 && !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
			return fmt.Errorf("provider name %q must start with a lowercase letter or number", name)
		}
	}
	return nil
}

func prefixesOverlap(left, right string) bool {
	left = pathutil.NormalizePrefix(left)
	right = pathutil.NormalizePrefix(right)
	return pathutil.HasPathPrefix(left, right) || pathutil.HasPathPrefix(right, left)
}

func validateProvider(name string, provider ProviderConfig) error {
	prefix := strings.TrimSpace(provider.Prefix)
	if prefix == "" {
		return fmt.Errorf("%s.prefix is required", name)
	}
	if !strings.HasPrefix(prefix, "/") {
		return fmt.Errorf("%s.prefix must start with '/' (got %q)", name, provider.Prefix)
	}

	upstream := strings.TrimSpace(provider.Upstream)
	if upstream == "" {
		return fmt.Errorf("%s.upstream is required", name)
	}
	parsed, err := url.Parse(upstream)
	if err != nil {
		return fmt.Errorf("parse %s.upstream: %w", name, err)
	}
	if strings.TrimSpace(parsed.Scheme) == "" || strings.TrimSpace(parsed.Host) == "" {
		return fmt.Errorf("%s.upstream must include scheme and host (got %q)", name, provider.Upstream)
	}
	return nil
}

func applyEnv(cfg *Config) error {
	if host := os.Getenv("ONGOINGAI_HOST"); host != "" {
		cfg.Server.Host = host
	}
	if port := os.Getenv("ONGOINGAI_PORT"); port != "" {
		v, err := strconv.Atoi(port)
		if err != nil {
			return fmt.Errorf("invalid ONGOINGAI_PORT: %w", err)
		}
		cfg.Server.Port = v
	}
	if storageDriver := os.Getenv("ONGOINGAI_STORAGE_DRIVER"); storageDriver != "" {
		cfg.Storage.Driver = storageDriver
	}
	if storagePath := os.Getenv("ONGOINGAI_STORAGE_PATH"); storagePath != "" {
		cfg.Storage.Path = storagePath
	}
	if storageDSN := os.Getenv("ONGOINGAI_STORAGE_DSN"); storageDSN != "" {
		cfg.Storage.DSN = storageDSN
	}
	if llmUpstream := os.Getenv("ONGOINGAI_LLM_UPSTREAM"); llmUpstream != "" {
		provider := cfg.Providers["llm"]
		provider.Upstream = llmUpstream
		cfg.Providers["llm"] = provider
	}
	if captureBodies := os.Getenv("ONGOINGAI_CAPTURE_BODIES"); captureBodies != "" {
		v, err := strconv.ParseBool(captureBodies)
		if err != nil {
			return fmt.Errorf("invalid ONGOINGAI_CAPTURE_BODIES: %w", err)
		}
		cfg.Tracing.CaptureBodies = v
	}
	if bodyMaxSize := os.Getenv("ONGOINGAI_BODY_MAX_SIZE"); bodyMaxSize != "" {
		v, err := strconv.Atoi(bodyMaxSize)
		if err != nil {
			return fmt.Errorf("invalid ONGOINGAI_BODY_MAX_SIZE: %w", err)
		}
		cfg.Tracing.BodyMaxSize = v
	}
	if retentionDays := os.Getenv("ONGOINGAI_TRACE_RETENTION_DAYS"); retentionDays != "" {
		v, err := strconv.Atoi(retentionDays)
		if err != nil {
			return fmt.Errorf("invalid ONGOINGAI_TRACE_RETENTION_DAYS: %w", err)
		}
		cfg.Tracing.Retention.Days = v
	}
	if cleanupEnabled := os.Getenv("ONGOINGAI_TRACE_CLEANUP_ENABLED"); cleanupEnabled != "" {
		v, err := strconv.ParseBool(cleanupEnabled)
		if err != nil {
			return fmt.Errorf("invalid ONGOINGAI_TRACE_CLEANUP_ENABLED: %w", err)
		}
		cfg.Tracing.Retention.CleanupEnabled = v
	}
	if cleanupDailyAt := os.Getenv("ONGOINGAI_TRACE_CLEANUP_DAILY_AT"); cleanupDailyAt != "" {
		cfg.Tracing.Retention.CleanupDailyAt = cleanupDailyAt
	}
	if cleanupTimezone := os.Getenv("ONGOINGAI_TRACE_CLEANUP_TIMEZONE"); cleanupTimezone != "" {
		cfg.Tracing.Retention.CleanupTimezone = cleanupTimezone
	}
	return applyBackupEnv(cfg)
}

func applyBackupEnv(cfg *Config) error {
	if enabled := os.Getenv("ONGOINGAI_BACKUP_REQUEST_DETAILS_ENABLED"); enabled != "" {
		v, err := strconv.ParseBool(enabled)
		if err != nil {
			return fmt.Errorf("invalid ONGOINGAI_BACKUP_REQUEST_DETAILS_ENABLED: %w", err)
		}
		cfg.Backup.RequestDetails.Enabled = v
	}
	if timezone := os.Getenv("ONGOINGAI_BACKUP_REQUEST_DETAILS_TIMEZONE"); timezone != "" {
		cfg.Backup.RequestDetails.Timezone = timezone
	}
	if dailyAt := os.Getenv("ONGOINGAI_BACKUP_REQUEST_DETAILS_DAILY_AT"); dailyAt != "" {
		cfg.Backup.RequestDetails.DailyAt = dailyAt
	}
	if tempDir := os.Getenv("ONGOINGAI_BACKUP_REQUEST_DETAILS_TEMP_DIR"); tempDir != "" {
		cfg.Backup.RequestDetails.TempDir = tempDir
	}
	if pageSize := os.Getenv("ONGOINGAI_BACKUP_REQUEST_DETAILS_PAGE_SIZE"); pageSize != "" {
		v, err := strconv.Atoi(pageSize)
		if err != nil {
			return fmt.Errorf("invalid ONGOINGAI_BACKUP_REQUEST_DETAILS_PAGE_SIZE: %w", err)
		}
		cfg.Backup.RequestDetails.PageSize = v
	}
	if shardMaxBytes := os.Getenv("ONGOINGAI_BACKUP_REQUEST_DETAILS_SHARD_MAX_BYTES"); shardMaxBytes != "" {
		v, err := strconv.ParseInt(shardMaxBytes, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid ONGOINGAI_BACKUP_REQUEST_DETAILS_SHARD_MAX_BYTES: %w", err)
		}
		cfg.Backup.RequestDetails.ShardMaxBytes = v
	}
	if retryMaxAttempts := os.Getenv("ONGOINGAI_BACKUP_REQUEST_DETAILS_RETRY_MAX_ATTEMPTS"); retryMaxAttempts != "" {
		v, err := strconv.Atoi(retryMaxAttempts)
		if err != nil {
			return fmt.Errorf("invalid ONGOINGAI_BACKUP_REQUEST_DETAILS_RETRY_MAX_ATTEMPTS: %w", err)
		}
		cfg.Backup.RequestDetails.Retry.MaxAttempts = v
	}
	if retryInitialBackoffMS := os.Getenv("ONGOINGAI_BACKUP_REQUEST_DETAILS_RETRY_INITIAL_BACKOFF_MS"); retryInitialBackoffMS != "" {
		v, err := strconv.Atoi(retryInitialBackoffMS)
		if err != nil {
			return fmt.Errorf("invalid ONGOINGAI_BACKUP_REQUEST_DETAILS_RETRY_INITIAL_BACKOFF_MS: %w", err)
		}
		cfg.Backup.RequestDetails.Retry.InitialBackoffMS = v
	}
	if retryMaxBackoffMS := os.Getenv("ONGOINGAI_BACKUP_REQUEST_DETAILS_RETRY_MAX_BACKOFF_MS"); retryMaxBackoffMS != "" {
		v, err := strconv.Atoi(retryMaxBackoffMS)
		if err != nil {
			return fmt.Errorf("invalid ONGOINGAI_BACKUP_REQUEST_DETAILS_RETRY_MAX_BACKOFF_MS: %w", err)
		}
		cfg.Backup.RequestDetails.Retry.MaxBackoffMS = v
	}
	if bucket := os.Getenv("ONGOINGAI_BACKUP_REQUEST_DETAILS_S3_BUCKET"); bucket != "" {
		cfg.Backup.RequestDetails.S3.Bucket = bucket
	}
	if prefix := os.Getenv("ONGOINGAI_BACKUP_REQUEST_DETAILS_S3_PREFIX"); prefix != "" {
		cfg.Backup.RequestDetails.S3.Prefix = prefix
	}
	if region := os.Getenv("ONGOINGAI_BACKUP_REQUEST_DETAILS_S3_REGION"); region != "" {
		cfg.Backup.RequestDetails.S3.Region = region
	}
	if endpoint := os.Getenv("ONGOINGAI_BACKUP_REQUEST_DETAILS_S3_ENDPOINT"); endpoint != "" {
		cfg.Backup.RequestDetails.S3.Endpoint = endpoint
	}
	if forcePathStyle := os.Getenv("ONGOINGAI_BACKUP_REQUEST_DETAILS_S3_FORCE_PATH_STYLE"); forcePathStyle != "" {
		v, err := strconv.ParseBool(forcePathStyle)
		if err != nil {
			return fmt.Errorf("invalid ONGOINGAI_BACKUP_REQUEST_DETAILS_S3_FORCE_PATH_STYLE: %w", err)
		}
		cfg.Backup.RequestDetails.S3.ForcePathStyle = v
	}
	return nil
}
