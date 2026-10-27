"""Regression check for fixed Stage 14 query argument handling (no live DB needed)."""
import ast
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'verify_stage14.py'


def test_stage14_audit_query_regressions():
    source = SCRIPT.read_text(encoding='utf-8')
    ast.parse(source)
    assert 'for start,end,shard in intervals:' in source
    assert 'gid LIKE %s' in source
    assert '("bank2pc:%",)' in source
    assert 'db.execute(query, args) if args else db.execute(query)' in source
    assert "gid LIKE 'bank2pc:%'" not in source
