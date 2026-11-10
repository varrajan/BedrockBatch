"""SQLite reference storage layer.

This is a reference implementation of the persistence a batch pipeline
needs: one row per job, one row per event, one row per on-demand
recovery. Swap it for your own observability stack in production
(DynamoDB, Postgres, your telemetry pipeline, etc.).

The schema is intentionally flat so it's easy to query from a
dashboard, a notebook, or another pipeline stage.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    region TEXT NOT NULL,
    batch_size INTEGER NOT NULL,
    model_id TEXT NOT NULL,
    workload TEXT NOT NULL,
    submitted_at REAL NOT NULL,
    started_at REAL,
    completed_at REAL,
    status TEXT NOT NULL,
    records_succeeded INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    input_tokens INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    s3_input_uri TEXT,
    s3_output_uri TEXT,
    failure_reason TEXT
);

CREATE TABLE IF NOT EXISTS fallbacks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    triggered_at REAL NOT NULL,
    records_recovered INTEGER NOT NULL,
    ondemand_duration_seconds REAL NOT NULL,
    ondemand_input_tokens INTEGER DEFAULT 0,
    ondemand_output_tokens INTEGER DEFAULT 0,
    reason TEXT,
    FOREIGN KEY (job_id) REFERENCES jobs(job_id)
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    region TEXT NOT NULL,
    old_status TEXT,
    new_status TEXT NOT NULL,
    event_time REAL NOT NULL,
    received_at REAL NOT NULL,
    source TEXT NOT NULL,
    raw_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_region_size ON jobs(region, batch_size);
CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id);
"""


@contextmanager
def connect(db_path: Path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)


_VALID_COLS = {
    "jobs": {"job_id", "region", "batch_size", "model_id", "workload", "submitted_at",
             "started_at", "completed_at", "status", "records_succeeded", "records_failed",
             "input_tokens", "output_tokens", "s3_input_uri", "s3_output_uri", "failure_reason"},
    "fallbacks": {"job_id", "triggered_at", "records_recovered", "ondemand_duration_seconds",
                  "ondemand_input_tokens", "ondemand_output_tokens", "reason"},
    "events": {"job_id", "region", "old_status", "new_status", "event_time", "received_at",
               "source", "raw_json"},
}


def _validate_cols(table: str, keys):
    allowed = _VALID_COLS.get(table)
    if allowed:
        bad = set(keys) - allowed
        if bad:
            raise ValueError(f"Invalid column names for {table}: {bad}")


def record_job(db_path: Path, row: dict) -> None:
    _validate_cols("jobs", row.keys())
    cols = ",".join(row.keys())
    placeholders = ",".join("?" for _ in row)
    with connect(db_path) as conn:
        conn.execute(
            f"INSERT OR REPLACE INTO jobs ({cols}) VALUES ({placeholders})",
            tuple(row.values()),
        )


def update_job(db_path: Path, job_id: str, updates: dict) -> None:
    _validate_cols("jobs", updates.keys())
    sets = ",".join(f"{k} = ?" for k in updates)
    with connect(db_path) as conn:
        conn.execute(
            f"UPDATE jobs SET {sets} WHERE job_id = ?",
            (*updates.values(), job_id),
        )


def record_fallback(db_path: Path, row: dict) -> None:
    _validate_cols("fallbacks", row.keys())
    cols = ",".join(row.keys())
    placeholders = ",".join("?" for _ in row)
    with connect(db_path) as conn:
        conn.execute(
            f"INSERT INTO fallbacks ({cols}) VALUES ({placeholders})",
            tuple(row.values()),
        )


def record_event(db_path: Path, row: dict) -> None:
    _validate_cols("events", row.keys())
    cols = ",".join(row.keys())
    placeholders = ",".join("?" for _ in row)
    with connect(db_path) as conn:
        conn.execute(
            f"INSERT INTO events ({cols}) VALUES ({placeholders})",
            tuple(row.values()),
        )


def all_jobs(db_path: Path) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute("SELECT * FROM jobs ORDER BY region, batch_size").fetchall()
        return [dict(r) for r in rows]


def all_fallbacks(db_path: Path) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT f.*, j.region, j.batch_size FROM fallbacks f "
            "JOIN jobs j ON j.job_id = f.job_id ORDER BY f.triggered_at"
        ).fetchall()
        return [dict(r) for r in rows]


def all_events(db_path: Path) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT e.*, j.batch_size FROM events e "
            "LEFT JOIN jobs j ON j.job_id = e.job_id "
            "ORDER BY e.event_time"
        ).fetchall()
        return [dict(r) for r in rows]


def get_job(db_path: Path, job_id: str) -> dict | None:
    with connect(db_path) as conn:
        row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return dict(row) if row else None


def fallback_exists(db_path: Path, job_id: str) -> bool:
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM fallbacks WHERE job_id = ? LIMIT 1", (job_id,)
        ).fetchone()
        return row is not None
