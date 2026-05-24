CREATE TABLE IF NOT EXISTS trace_bodies (
    trace_id TEXT PRIMARY KEY REFERENCES traces(id) ON DELETE CASCADE,
    request_body_gzip BYTEA,
    response_body_gzip BYTEA,
    request_body_size_bytes BIGINT NOT NULL DEFAULT 0,
    response_body_size_bytes BIGINT NOT NULL DEFAULT 0,
    request_body_compressed_size_bytes BIGINT NOT NULL DEFAULT 0,
    response_body_compressed_size_bytes BIGINT NOT NULL DEFAULT 0,
    request_body_sha256 TEXT,
    response_body_sha256 TEXT,
    request_body_truncated BOOLEAN NOT NULL DEFAULT FALSE,
    response_body_truncated BOOLEAN NOT NULL DEFAULT FALSE,
    pii_status TEXT NOT NULL DEFAULT 'unknown',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE trace_bodies ENABLE ROW LEVEL SECURITY;
ALTER TABLE trace_bodies FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trace_bodies_tenant_scope ON trace_bodies;
CREATE POLICY trace_bodies_tenant_scope ON trace_bodies
USING (
    EXISTS (
        SELECT 1
        FROM traces
        WHERE traces.id = trace_bodies.trace_id
    )
)
WITH CHECK (
    EXISTS (
        SELECT 1
        FROM traces
        WHERE traces.id = trace_bodies.trace_id
    )
);
