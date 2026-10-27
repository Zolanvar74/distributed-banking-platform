Stage 14 verification hotfix v2 (2026-10-27)
==========================================
This hotfix updates ONLY scripts/verify_stage14.py.
It does not modify PostgreSQL data, Docker Compose services or migrations.

Changes:
1. Keeps the previous fix: tuple order (start, end, shard) in active_ranges loop.
2. Parameterizes gid LIKE pattern as 'gid LIKE %s' with ('bank2pc:%',).
3. Executes non-parameterized queries without passing an empty args tuple.

Windows PowerShell steps (from repository root):
  - Extract zip and copy its scripts/verify_stage14.py over the repository's scripts/verify_stage14.py.
  - docker compose build schema-migrator
  - docker compose run --rm schema-migrator python -m scripts.verify_stage14 --scenario=postcutover --json

Do not rerun cutover, delete volumes, or restart the database to resolve this Python SQL query formatting error.
