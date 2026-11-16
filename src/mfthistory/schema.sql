-- Transfer history schema.
-- Written in plain SQL that runs on SQLite as is. For SQL Server, Db2 or PostgreSQL,
-- change TEXT to VARCHAR and INTEGER PRIMARY KEY to an identity column.
-- All timestamps are UTC ISO 8601 strings (YYYY-MM-DDTHH:MM:SS.fffZ) so they sort as text.

CREATE TABLE IF NOT EXISTS transfers (
    transfer_id        TEXT PRIMARY KEY,
    source_agent       TEXT,
    source_qmgr        TEXT,
    destination_agent  TEXT,
    destination_qmgr   TEXT,
    originator_host    TEXT,
    originator_user    TEXT,
    job_name           TEXT,
    started_at         TEXT,
    completed_at       TEXT,
    last_action        TEXT,
    last_event_at      TEXT,
    result_code        INTEGER,
    status             TEXT,
    supplement         TEXT,
    file_count         INTEGER DEFAULT 0,
    total_bytes        INTEGER,
    updated_at         TEXT
);

CREATE INDEX IF NOT EXISTS ix_transfers_started   ON transfers (started_at);
CREATE INDEX IF NOT EXISTS ix_transfers_src_agent ON transfers (source_agent, started_at);
CREATE INDEX IF NOT EXISTS ix_transfers_dst_agent ON transfers (destination_agent, started_at);
CREATE INDEX IF NOT EXISTS ix_transfers_status    ON transfers (status, started_at);
CREATE INDEX IF NOT EXISTS ix_transfers_job       ON transfers (job_name);

CREATE TABLE IF NOT EXISTS transfer_items (
    id                 INTEGER PRIMARY KEY,
    transfer_id        TEXT NOT NULL REFERENCES transfers (transfer_id) ON DELETE CASCADE,
    seq                INTEGER NOT NULL,
    source_file        TEXT,
    source_name        TEXT,
    destination_file   TEXT,
    source_size        INTEGER,
    destination_size   INTEGER,
    mode               TEXT,
    checksum_method    TEXT,
    checksum           TEXT,
    result_code        INTEGER,
    supplement         TEXT,
    UNIQUE (transfer_id, seq)
);

CREATE INDEX IF NOT EXISTS ix_items_source_name ON transfer_items (source_name);
CREATE INDEX IF NOT EXISTS ix_items_transfer    ON transfer_items (transfer_id);

-- Every message received, kept for audit and for rebuilding transfers.
CREATE TABLE IF NOT EXISTS transfer_events (
    id                 INTEGER PRIMARY KEY,
    transfer_id        TEXT NOT NULL,
    action             TEXT NOT NULL,
    event_time         TEXT,
    received_at        TEXT NOT NULL,
    fingerprint        TEXT NOT NULL UNIQUE,
    raw_xml            TEXT
);

CREATE INDEX IF NOT EXISTS ix_events_transfer ON transfer_events (transfer_id);
