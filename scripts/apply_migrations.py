"""Versioned database migration runner. Execute as a one-shot Compose service.

- Holds a PostgreSQL transaction-level advisory lock per database.
- Records each committed migration and its SHA-256; rejects changed migrations.
- Runs each file in the same transaction as its migration-record insert.
- Safe to rerun with existing named volumes; never drops or resets seed data.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "sql" / "migrations"
TARGETS = (
    ("metadata", "METADATA_DATABASE_URL", MIGRATIONS / "metadata"),
    ("shard-a", "SHARD_A_DATABASE_URL", MIGRATIONS / "shard"),
    ("shard-b", "SHARD_B_DATABASE_URL", MIGRATIONS / "shard"),
)


def apply_target(name: str, dsn: str, folder: Path) -> None:
    if not folder.is_dir():
        raise RuntimeError(f"Missing migrations folder: {folder}")
    files = sorted(folder.glob("[0-9][0-9][0-9]_*.sql"))
    if not files:
        raise RuntimeError(f"No migrations found in {folder}")

    with psycopg.connect(dsn, connect_timeout=10) as conn:
        for path in files:
            migration = path.name
            body = path.read_text(encoding="utf-8")
            checksum = hashlib.sha256(body.encode("utf-8")).hexdigest()
            # Each migration is atomic and concurrent migrations serialize.
            with conn.transaction():
                conn.execute("SELECT pg_advisory_xact_lock(hashtext('distributed-banking-schema-v1'))")
                conn.execute("""CREATE TABLE IF NOT EXISTS schema_migrations (
                    version TEXT PRIMARY KEY,
                    checksum CHAR(64) NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                )""")
                row = conn.execute(
                    "SELECT checksum FROM schema_migrations WHERE version = %s", (migration,)
                ).fetchone()
                if row is not None:
                    if row[0] != checksum:
                        raise RuntimeError(f"Refusing edited previously applied migration: {name}/{migration}")
                    print(f"SKIP {name}/{migration} (already applied)", flush=True)
                    continue
                # psycopg's non-parameterized simple-query protocol accepts an
                # SQL script containing multiple statements in this transaction.
                conn.execute(body)
                conn.execute(
                    "INSERT INTO schema_migrations(version, checksum) VALUES (%s, %s)",
                    (migration, checksum),
                )
                print(f"APPLIED {name}/{migration}", flush=True)


def main() -> None:
    for name, variable, folder in TARGETS:
        apply_target(name, os.environ[variable], folder)
    print("STAGE 02 MIGRATIONS: ALL APPLIED OR ALREADY PRESENT", flush=True)


if __name__ == "__main__":
    main()
