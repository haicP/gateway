package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadMissingFileUsesLightweightDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Server.Address() != "0.0.0.0:8080" {
		t.Fatalf("server address=%q, want 0.0.0.0:8080", cfg.Server.Address())
	}
	if cfg.Storage.Driver != "sqlite" || cfg.Storage.Path == "" {
		t.Fatalf("storage=%+v, want sqlite path default", cfg.Storage)
	}
	if len(cfg.Providers) != 1 || cfg.Providers["llm"].Prefix != "/llm" {
		t.Fatalf("providers=%+v, want default llm provider", cfg.Providers)
	}
	if cfg.Tracing.CaptureBodies {
		t.Fatalf("tracing.capture_bodies=%v, want false", cfg.Tracing.CaptureBodies)
	}
	if cfg.Tracing.Retention.Days != 14 {
		t.Fatalf("retention days=%d, want 14", cfg.Tracing.Retention.Days)
	}
	if cfg.Tracing.Retention.CleanupEnabled {
		t.Fatalf("retention cleanup=%v, want false", cfg.Tracing.Retention.CleanupEnabled)
	}
	if cfg.Backup.RequestDetails.Enabled {
		t.Fatalf("request detail backup enabled=%v, want false", cfg.Backup.RequestDetails.Enabled)
	}
}

func TestLoadYAMLAndEnvForCoreGatewayConfig(t *testing.T) {
	t.Setenv("ONGOINGAI_PORT", "9090")
	t.Setenv("ONGOINGAI_CAPTURE_BODIES", "true")
	t.Setenv("ONGOINGAI_TRACE_RETENTION_DAYS", "30")
	t.Setenv("ONGOINGAI_BACKUP_REQUEST_DETAILS_S3_BUCKET", "env-bucket")

	path := filepath.Join(t.TempDir(), "ongoingai.yaml")
	data := []byte(`
server:
  host: 127.0.0.1
  port: 8081
storage:
  driver: postgres
  dsn: postgres://gateway:gateway@localhost:5432/gateway
providers:
  openai:
    upstream: https://api.openai.com
    prefix: /openai
tracing:
  capture_bodies: false
  body_max_size: 2048
  retention:
    days: 7
    cleanup_enabled: true
    cleanup_daily_at: "03:30"
    cleanup_timezone: UTC
backup:
  request_details:
    enabled: true
    timezone: UTC
    daily_at: "04:00"
    temp_dir: ./tmp
    page_size: 100
    shard_max_bytes: 1048576
    retry:
      max_attempts: 2
      initial_backoff_ms: 100
      max_backoff_ms: 1000
    s3:
      bucket: yaml-bucket
      prefix: traces
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if err := Validate(cfg); err != nil {
		t.Fatalf("Validate() error: %v", err)
	}

	if cfg.Server.Address() != "127.0.0.1:9090" {
		t.Fatalf("server address=%q, want env port override", cfg.Server.Address())
	}
	if !cfg.Tracing.CaptureBodies {
		t.Fatalf("tracing.capture_bodies=%v, want env override true", cfg.Tracing.CaptureBodies)
	}
	if cfg.Tracing.Retention.Days != 30 {
		t.Fatalf("retention days=%d, want env override 30", cfg.Tracing.Retention.Days)
	}
	if cfg.Backup.RequestDetails.S3.Bucket != "env-bucket" {
		t.Fatalf("backup bucket=%q, want env override", cfg.Backup.RequestDetails.S3.Bucket)
	}
}

func TestValidateRejectsRemovedAndInvalidConfig(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "ongoingai.yaml")
	data := []byte(`
server:
  host: 0.0.0.0
  port: 8080
storage:
  driver: sqlite
  path: ./data/ongoingai.db
providers:
  llm:
    upstream: https://api.openai.com
    prefix: /llm
auth:
  enabled: true
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() error=nil, want unknown auth field error")
	}
	if !strings.Contains(err.Error(), "field auth not found") {
		t.Fatalf("Load() error=%v, want unknown auth field", err)
	}
}

func TestValidateRequiresBackupBucketWhenEnabled(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Backup.RequestDetails.Enabled = true
	err := Validate(cfg)
	if err == nil {
		t.Fatal("Validate() error=nil, want missing bucket error")
	}
	if !strings.Contains(err.Error(), "backup.request_details.s3.bucket") {
		t.Fatalf("Validate() error=%v, want backup bucket error", err)
	}
}

func TestValidateRejectsOverlappingProviderPrefixes(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Providers = ProvidersConfig{
		"one": {Upstream: "https://one.example", Prefix: "/llm"},
		"two": {Upstream: "https://two.example", Prefix: "/llm/chat"},
	}
	err := Validate(cfg)
	if err == nil {
		t.Fatal("Validate() error=nil, want overlapping prefix error")
	}
	if !strings.Contains(err.Error(), "provider prefixes must not overlap") {
		t.Fatalf("Validate() error=%v, want overlapping prefix error", err)
	}
}
