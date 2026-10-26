"""Read-only checks of Stage 02 schema; run in the schema-migrator container."""
import os

import psycopg


def check_metadata() -> None:
    with psycopg.connect(os.environ["METADATA_DATABASE_URL"]) as c:
        assert c.execute("SELECT epoch FROM routing_epoch WHERE singleton = 1").fetchone() == (1,)
        assert c.execute("SELECT count(*) FROM shard_map WHERE active").fetchone()[0] == 2
        assert c.execute("SELECT count(*) FROM desired_replica_count WHERE desired_count = 2").fetchone()[0] == 2
        assert c.execute("SELECT count(*) FROM replica_registry WHERE slot IS NULL").fetchone()[0] == 0
        assert set(c.execute("SELECT replica_id, slot FROM replica_registry").fetchall()) == {
            ("A1", 1), ("A2", 2), ("B1", 1), ("B2", 2)
        }
        assert c.execute("SELECT count(*) FROM transactions").fetchone()[0] >= 0
        assert c.execute("SELECT count(*) FROM transaction_participants").fetchone()[0] >= 0
        assert c.execute("SELECT count(*) FROM migration_jobs").fetchone()[0] >= 0
        assert c.execute("SELECT count(*) FROM migration_account_checkpoints").fetchone()[0] >= 0
        assert c.execute("SELECT count(*) FROM schema_migrations WHERE version='002_foundation.sql'").fetchone() == (1,)
        assert c.execute("""SELECT count(*) FROM pg_constraint
                            WHERE conname = 'shard_map_active_no_overlap' AND contype = 'x'""").fetchone() == (1,)
        assert c.execute("""SELECT count(*) FROM pg_trigger
                            WHERE tgname = 'transactions_decision_immutable' AND NOT tgisinternal""").fetchone() == (1,)
        assert c.execute("""SELECT count(*) FROM pg_trigger
                            WHERE tgname = 'routing_epoch_increases' AND NOT tgisinternal""").fetchone() == (1,)
    print("PASS metadata: map epoch, registries, durable 2PC and migration tables", flush=True)


def check_shard(env_var: str, db: str, ranges: tuple, sample: str, amount: str) -> None:
    with psycopg.connect(os.environ[env_var]) as c:
        assert c.execute("SELECT balance::text FROM accounts WHERE account_id=%s", (sample,)).fetchone() == (amount,)
        assert c.execute("SELECT range_start, range_end, epoch, writable FROM ownership_fences").fetchall() == [
            (ranges[0], ranges[1], 1, True)
        ]
        for table in ("account_change_groups", "account_changes", "local_request_dedup",
                      "migration_applied_groups", "participant_commit_ledger"):
            assert c.execute("SELECT to_regclass(%s)", ("public." + table,)).fetchone()[0] is not None
        assert c.execute("SELECT count(*) FROM schema_migrations WHERE version='002_foundation.sql'").fetchone() == (1,)
        assert int(c.execute("SHOW max_prepared_transactions").fetchone()[0]) > 0
    print(f"PASS {db}: data preserved, fences, journal and 2PC-ready setting", flush=True)


if __name__ == "__main__":
    check_metadata()
    check_shard("SHARD_A_DATABASE_URL", "Shard A", (1, 1000), "A0500", "1000.00")
    check_shard("SHARD_B_DATABASE_URL", "Shard B", (1001, 2000), "A1500", "500.00")
    print("STAGE 02 SCHEMA CHECK: ALL CHECKS PASSED", flush=True)
