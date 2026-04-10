CREATE TABLE IF NOT EXISTS introspect_sink_metadata (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    initialized_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
