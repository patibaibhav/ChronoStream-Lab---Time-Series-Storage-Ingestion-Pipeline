"""Versioned SQLite metadata repository for local simulation runs."""

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from app.engine.chunk_builder import Chunk
from app.engine.local_writer import PersistedChunk


SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class RunMetadata:
    run_id: str
    started_at: datetime
    settings: dict[str, object]


class SQLiteMetadataRepository:
    """Persist run and pipeline metadata in a durable local SQLite database."""

    def __init__(self, database_path: Path) -> None:
        self._database_path = database_path
        self._database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def create_run(self, run_id: str, started_at: datetime, settings: dict[str, object]) -> None:
        self._execute(
            """
            INSERT INTO runs (run_id, started_at, settings_json)
            VALUES (?, ?, ?)
            """,
            (run_id, started_at.isoformat(), json.dumps(settings, sort_keys=True)),
        )

    def finish_run(self, run_id: str, finished_at: datetime, status: str) -> None:
        self._execute(
            "UPDATE runs SET finished_at = ?, status = ? WHERE run_id = ?",
            (finished_at.isoformat(), status, run_id),
        )

    def record_event(
        self,
        run_id: str,
        event_id: str | None,
        status: str,
        reason: str | None,
        payload: dict[str, object] | None,
    ) -> None:
        self._execute(
            """
            INSERT INTO events (run_id, event_id, status, reason, payload_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run_id, event_id, status, reason, json.dumps(payload, sort_keys=True) if payload else None),
        )

    def record_metric_sample(
        self,
        run_id: str,
        event_id: str,
        metric: str,
        value: float,
        event_timestamp: datetime,
    ) -> None:
        self._execute(
            """
            INSERT INTO metric_samples
                (run_id, event_id, metric, value, event_timestamp)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run_id, event_id, metric, value, event_timestamp.isoformat()),
        )

    def record_chunk(
        self,
        run_id: str,
        chunk: Chunk,
        status: str,
        persisted: PersistedChunk | None = None,
        error: str | None = None,
    ) -> None:
        self._execute(
            """
            INSERT OR REPLACE INTO chunks
                (chunk_id, run_id, status, record_count, path, byte_size, checksum, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                chunk.chunk_id,
                run_id,
                status,
                chunk.record_count,
                str(persisted.local_path) if persisted else None,
                persisted.byte_size if persisted else None,
                persisted.checksum if persisted else None,
                error,
            ),
        )

    def count(self, table: str, run_id: str) -> int:
        if table not in {"events", "metric_samples", "chunks"}:
            raise ValueError("unsupported metadata table")
        with self._connect() as connection:
            row = connection.execute(
                f"SELECT COUNT(*) FROM {table} WHERE run_id = ?", (run_id,)
            ).fetchone()
        return int(row[0])

    def list_runs(self) -> list[dict[str, object]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT run_id, started_at, finished_at, status, settings_json "
                "FROM runs ORDER BY started_at DESC"
            ).fetchall()
        return [
            {
                "run_id": row[0],
                "started_at": row[1],
                "finished_at": row[2],
                "status": row[3],
                "settings": json.loads(row[4]),
            }
            for row in rows
        ]

    def list_chunks(self, run_id: str | None = None) -> list[dict[str, object]]:
        statement = (
            "SELECT chunk_id, run_id, status, record_count, path, byte_size, checksum, error "
            "FROM chunks"
        )
        parameters: tuple[object, ...] = ()
        if run_id is not None:
            statement += " WHERE run_id = ?"
            parameters = (run_id,)
        statement += " ORDER BY rowid DESC"
        with self._connect() as connection:
            rows = connection.execute(statement, parameters).fetchall()
        return [
            {
                "chunk_id": row[0],
                "run_id": row[1],
                "status": row[2],
                "record_count": row[3],
                "path": row[4],
                "byte_size": row[5],
                "checksum": row[6],
                "error": row[7],
            }
            for row in rows
        ]

    def list_events(self, run_id: str | None = None) -> list[dict[str, object]]:
        statement = (
            "SELECT run_id, event_id, status, reason, payload_json "
            "FROM events"
        )
        parameters: tuple[object, ...] = ()
        if run_id is not None:
            statement += " WHERE run_id = ?"
            parameters = (run_id,)
        statement += " ORDER BY id DESC LIMIT 100"
        with self._connect() as connection:
            rows = connection.execute(statement, parameters).fetchall()
        return [
            {
                "run_id": row[0],
                "event_id": row[1],
                "status": row[2],
                "reason": row[3],
                "payload": json.loads(row[4]) if row[4] else None,
            }
            for row in rows
        ]

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY
                );
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    status TEXT NOT NULL DEFAULT 'running',
                    settings_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL REFERENCES runs(run_id),
                    event_id TEXT,
                    status TEXT NOT NULL,
                    reason TEXT,
                    payload_json TEXT
                );
                CREATE TABLE IF NOT EXISTS metric_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL REFERENCES runs(run_id),
                    event_id TEXT NOT NULL,
                    metric TEXT NOT NULL,
                    value REAL NOT NULL,
                    event_timestamp TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS chunks (
                    chunk_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES runs(run_id),
                    status TEXT NOT NULL,
                    record_count INTEGER NOT NULL,
                    path TEXT,
                    byte_size INTEGER,
                    checksum TEXT,
                    error TEXT
                );
                """
            )
            connection.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (?)",
                (SCHEMA_VERSION,),
            )

    def _execute(self, statement: str, parameters: tuple[object, ...]) -> None:
        with self._connect() as connection:
            connection.execute(statement, parameters)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._database_path)
        connection.execute("PRAGMA foreign_keys = ON")
        return connection
