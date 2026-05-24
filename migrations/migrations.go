package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"
	"time"
)

const (
	// DriverSQLite applies migrations from migrations/sqlite.
	DriverSQLite = "sqlite"
	// DriverPostgres applies migrations from migrations/postgres.
	DriverPostgres = "postgres"
)

//go:embed sqlite/*.sql postgres/*.sql
var embedded embed.FS

const postgresSchemaAdvisoryLockKey int64 = 8067396491

type migrationDB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// Apply runs all embedded migrations for the selected driver in lexicographic order.
// Each migration is applied exactly once and tracked in schema_migrations.
func Apply(ctx context.Context, db *sql.DB, driver string) error {
	if db == nil {
		return fmt.Errorf("database is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	driver = strings.ToLower(strings.TrimSpace(driver))
	if driver != DriverSQLite && driver != DriverPostgres {
		return fmt.Errorf("unsupported migration driver %q", driver)
	}

	if driver == DriverPostgres {
		return WithPostgresSchemaLock(ctx, db, func(conn *sql.Conn) error {
			return apply(ctx, conn, driver)
		})
	}

	return apply(ctx, db, driver)
}

// WithPostgresSchemaLock serializes Postgres schema changes across packages and
// processes using one session-bound advisory lock.
func WithPostgresSchemaLock(ctx context.Context, db *sql.DB, fn func(conn *sql.Conn) error) error {
	if db == nil {
		return fmt.Errorf("database is required")
	}
	if fn == nil {
		return fmt.Errorf("postgres schema lock callback is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open postgres schema lock connection: %w", err)
	}
	defer conn.Close() //nolint:errcheck

	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1); -- ongoingai_schema", postgresSchemaAdvisoryLockKey); err != nil {
		return fmt.Errorf("acquire postgres schema advisory lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.ExecContext(unlockCtx, "SELECT pg_advisory_unlock($1)", postgresSchemaAdvisoryLockKey)
	}()

	return fn(conn)
}

func apply(ctx context.Context, db migrationDB, driver string) error {
	if err := ensureMigrationsTable(ctx, db, driver); err != nil {
		return err
	}

	entries, err := fs.ReadDir(embedded, driver)
	if err != nil {
		return fmt.Errorf("read embedded %s migrations: %w", driver, err)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".sql") {
			continue
		}
		name := path.Join(driver, entry.Name())
		bodyBytes, err := embedded.ReadFile(name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if err := applyMigration(ctx, db, driver, name, string(bodyBytes)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
	}

	return nil
}

func ensureMigrationsTable(ctx context.Context, db migrationDB, driver string) error {
	var ddl string
	switch driver {
	case DriverSQLite:
		ddl = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    name TEXT PRIMARY KEY,
    applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
`
	case DriverPostgres:
		ddl = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    name TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`
	default:
		return fmt.Errorf("unsupported migration driver %q", driver)
	}
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}
	return nil
}

func applyMigration(ctx context.Context, db migrationDB, driver, name, statement string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	claimed, err := claimMigration(ctx, tx, driver, name)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if !claimed {
		if err := tx.Rollback(); err != nil {
			return fmt.Errorf("rollback transaction: %w", err)
		}
		return nil
	}

	if _, err := tx.ExecContext(ctx, statement); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("execute migration sql: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func claimMigration(ctx context.Context, tx *sql.Tx, driver, name string) (bool, error) {
	var (
		sqlText string
		args    []any
	)
	switch driver {
	case DriverSQLite:
		sqlText = `INSERT OR IGNORE INTO schema_migrations (name) VALUES (?)`
		args = append(args, name)
	case DriverPostgres:
		sqlText = `INSERT INTO schema_migrations (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`
		args = append(args, name)
	default:
		return false, fmt.Errorf("unsupported migration driver %q", driver)
	}

	res, err := tx.ExecContext(ctx, sqlText, args...)
	if err != nil {
		return false, fmt.Errorf("insert schema_migrations row: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read insert row count: %w", err)
	}
	return affected > 0, nil
}
