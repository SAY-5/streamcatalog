CREATE TABLE IF NOT EXISTS streams (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL UNIQUE,
    topic          TEXT NOT NULL,
    domain         TEXT NOT NULL,
    owner          TEXT NOT NULL,
    tags           TEXT[] NOT NULL DEFAULT '{}',
    retention_ms   BIGINT NOT NULL DEFAULT 0,
    access         TEXT NOT NULL,
    allow_list     TEXT[] NOT NULL DEFAULT '{}',
    schema_type    TEXT NOT NULL,
    schema_version INT NOT NULL DEFAULT 1,
    schema_def     TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_streams_domain ON streams (domain);
CREATE INDEX IF NOT EXISTS idx_streams_owner ON streams (owner);
CREATE INDEX IF NOT EXISTS idx_streams_tags ON streams USING GIN (tags);

CREATE TABLE IF NOT EXISTS subscriptions (
    id         TEXT PRIMARY KEY,
    stream_id  TEXT NOT NULL REFERENCES streams (id) ON DELETE CASCADE,
    consumer   TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (stream_id, consumer)
);

CREATE TABLE IF NOT EXISTS lineage_edges (
    id        TEXT PRIMARY KEY,
    from_node TEXT NOT NULL,
    to_node   TEXT NOT NULL,
    stream_id TEXT NOT NULL REFERENCES streams (id) ON DELETE CASCADE,
    kind      TEXT NOT NULL,
    UNIQUE (from_node, to_node, stream_id, kind)
);

CREATE INDEX IF NOT EXISTS idx_lineage_from ON lineage_edges (from_node);
CREATE INDEX IF NOT EXISTS idx_lineage_to ON lineage_edges (to_node);
CREATE INDEX IF NOT EXISTS idx_lineage_stream ON lineage_edges (stream_id);
