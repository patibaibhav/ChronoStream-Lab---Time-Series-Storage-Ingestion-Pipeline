"""Versioned SQLite metadata repository for local simulation runs."""

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
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
        *,
        flush_reason: str | None = None,
        retry_attempts: int = 0,
    ) -> None:
        now = datetime.now(UTC).isoformat()
        self._execute(
            """
            INSERT OR REPLACE INTO chunks
                (chunk_id, run_id, status, record_count, path, byte_size, checksum, error,
                 flush_reason, partition_id, sealed_reason, created_at, sealed_at,
                 write_started_at, write_completed_at, retry_attempts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                flush_reason,
                ":".join(chunk.partition_id),
                chunk.sealed_reason,
                now,
                now,
                now if status in ("writing", "persisted", "failed") else None,
                now if status in ("persisted", "failed") else None,
                retry_attempts,
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
            result = []
            for row in rows:
                run_id = row[0]
                event_counts = connection.execute(
                    "SELECT status, COUNT(*) FROM events WHERE run_id = ? GROUP BY status",
                    (run_id,),
                ).fetchall()
                chunk_summary = connection.execute(
                    """
                    SELECT COUNT(*), COALESCE(SUM(record_count), 0), COALESCE(SUM(byte_size), 0)
                    FROM chunks WHERE run_id = ? AND status = 'persisted'
                    """,
                    (run_id,),
                ).fetchone()
                flush_reasons = connection.execute(
                    """
                    SELECT flush_reason, COUNT(*)
                    FROM chunks WHERE run_id = ? AND status = 'persisted'
                    GROUP BY flush_reason
                    """,
                    (run_id,),
                ).fetchall()
                failed_chunks = connection.execute(
                    "SELECT COUNT(*) FROM chunks WHERE run_id = ? AND status = 'failed'",
                    (run_id,),
                ).fetchone()[0]
                counts = {str(status): int(count) for status, count in event_counts}
                settings = json.loads(row[4])
                result.append({
                    "run_id": run_id,
                    "started_at": row[1],
                    "finished_at": row[2],
                    "status": row[3],
                    "settings": settings,
                    "input_records": sum(counts.values()),
                    "accepted_records": counts.get("accepted", 0),
                    "rejected_records": counts.get("rejected", 0),
                    "persisted_chunks": int(chunk_summary[0]),
                    "persisted_records": int(chunk_summary[1]),
                    "persisted_bytes": int(chunk_summary[2]),
                    "failed_chunks": int(failed_chunks),
                    "flush_reasons": {str(reason or "unknown"): int(cnt) for reason, cnt in flush_reasons},
                })
            return result

    def list_chunks(self, run_id: str | None = None) -> list[dict[str, object]]:
        statement = (
            "SELECT chunk_id, run_id, status, record_count, path, byte_size, checksum, error, "
            "flush_reason, partition_id, sealed_reason, created_at, sealed_at, "
            "write_started_at, write_completed_at, retry_attempts "
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
                "flush_reason": row[8],
                "partition_id": row[9],
                "sealed_reason": row[10],
                "created_at": row[11],
                "sealed_at": row[12],
                "write_started_at": row[13],
                "write_completed_at": row[14],
                "retry_attempts": row[15],
            }
            for row in rows
        ]

    def list_files(self, run_id: str) -> list[dict[str, object]]:
        """List Parquet files for a specific run."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT path, byte_size, checksum, write_completed_at
                FROM chunks
                WHERE run_id = ? AND status = 'persisted' AND path IS NOT NULL
                ORDER BY write_completed_at DESC
                """,
                (run_id,),
            ).fetchall()
        return [
            {
                "path": row[0],
                "byte_size": row[1],
                "checksum": row[2],
                "write_completed_at": row[3],
            }
            for row in rows
        ]

    def aggregate_metrics(self) -> dict[str, object]:
        """Return aggregate totals across all runs."""
        with self._connect() as connection:
            run_count = connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
            event_counts = connection.execute(
                "SELECT status, COUNT(*) FROM events GROUP BY status"
            ).fetchall()
            chunk_summary = connection.execute(
                """
                SELECT COUNT(*), COALESCE(SUM(record_count), 0), COALESCE(SUM(byte_size), 0)
                FROM chunks WHERE status = 'persisted'
                """
            ).fetchone()
            failed_chunks = connection.execute(
                "SELECT COUNT(*) FROM chunks WHERE status = 'failed'"
            ).fetchone()[0]
        counts = {str(status): int(count) for status, count in event_counts}
        return {
            "total_runs": int(run_count),
            "total_generated": sum(counts.values()),
            "total_accepted": counts.get("accepted", 0),
            "total_rejected": counts.get("rejected", 0),
            "total_chunks_created": int(chunk_summary[0]),
            "total_chunks_persisted": int(chunk_summary[0]),
            "total_chunks_failed": int(failed_chunks),
            "total_persisted_records": int(chunk_summary[1]),
            "total_files": int(chunk_summary[0]),
            "total_bytes": int(chunk_summary[2]),
        }

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

    def get_event(self, event_id: str) -> dict[str, object] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT run_id, event_id, status, reason, payload_json
                FROM events
                WHERE event_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (event_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "run_id": row[0],
            "event_id": row[1],
            "status": row[2],
            "reason": row[3],
            "payload": json.loads(row[4]) if row[4] else None,
        }

    def run_summary(self, run_id: str) -> dict[str, object]:
        with self._connect() as connection:
            event_counts = connection.execute(
                "SELECT status, COUNT(*) FROM events WHERE run_id = ? GROUP BY status",
                (run_id,),
            ).fetchall()
            chunk_summary = connection.execute(
                """
                SELECT COUNT(*), COALESCE(SUM(record_count), 0), COALESCE(SUM(byte_size), 0)
                FROM chunks
                WHERE run_id = ? AND status = 'persisted'
                """,
                (run_id,),
            ).fetchone()
            flush_reasons = connection.execute(
                """
                SELECT flush_reason, COUNT(*)
                FROM chunks
                WHERE run_id = ? AND status = 'persisted'
                GROUP BY flush_reason
                """,
                (run_id,),
            ).fetchall()
            failed_chunks = connection.execute(
                "SELECT COUNT(*) FROM chunks WHERE run_id = ? AND status = 'failed'",
                (run_id,),
            ).fetchone()[0]
        counts = {str(status): int(count) for status, count in event_counts}
        return {
            "run_id": run_id,
            "input_records": sum(counts.values()),
            "accepted_records": counts.get("accepted", 0),
            "rejected_records": counts.get("rejected", 0),
            "persisted_chunks": int(chunk_summary[0]),
            "persisted_records": int(chunk_summary[1]),
            "persisted_bytes": int(chunk_summary[2]),
            "failed_chunks": int(failed_chunks),
            "flush_reasons": {str(reason or "unknown"): int(count) for reason, count in flush_reasons},
        }

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
                    error TEXT,
                    flush_reason TEXT,
                    partition_id TEXT,
                    sealed_reason TEXT,
                    created_at TEXT,
                    sealed_at TEXT,
                    write_started_at TEXT,
                    write_completed_at TEXT,
                    retry_attempts INTEGER DEFAULT 0
                );
                """
            )
            connection.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (?)",
                (SCHEMA_VERSION,),
            )
            self._migrate(connection)

    def _migrate(self, connection: sqlite3.Connection) -> None:
        """Add missing columns to existing tables for backward compatibility."""
        existing_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(chunks)").fetchall()
        }
        required_columns: dict[str, str] = {
            "flush_reason": "TEXT",
            "partition_id": "TEXT",
            "sealed_reason": "TEXT",
            "created_at": "TEXT",
            "sealed_at": "TEXT",
            "write_started_at": "TEXT",
            "write_completed_at": "TEXT",
            "retry_attempts": "INTEGER DEFAULT 0",
        }
        for column, column_type in required_columns.items():
            if column not in existing_columns:
                connection.execute(
                    f"ALTER TABLE chunks ADD COLUMN {column} {column_type}"
                )

    def _execute(self, statement: str, parameters: tuple[object, ...]) -> None:
        with self._connect() as connection:
            connection.execute(statement, parameters)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._database_path)
        connection.execute("PRAGMA foreign_keys = ON")
        return connection
