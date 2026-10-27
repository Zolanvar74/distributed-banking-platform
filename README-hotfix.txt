HOTFIX: missing /app/compose.yaml for stage 14 contract test

Replace only the root Dockerfile with Dockerfile from this ZIP.
Do not replace .env or compose.yaml. Do not delete any Docker volumes.

Windows PowerShell commands from repository root:
  docker compose build schema-migrator
  docker compose run --rm schema-migrator python -m pytest -q tests

Expected: 94 passed (assuming no other environment-dependent issues).

Commit the corrected Dockerfile only after tests have passed:
  git add Dockerfile
  git commit -m "fix: include compose config in test image"

Reason: test_stage14_contract.py reads /app/compose.yaml.
The shared app image previously copied only Python modules, scripts, tests, and migrations.
