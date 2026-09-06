"""Unit tests for serving/athena_ddl.py's SQL-file parsing (no AWS calls)."""
import os

from tradelens.serving.athena_ddl import _expand_env, _split_statements


def test_expand_env_substitutes_placeholder(monkeypatch):
    monkeypatch.setenv("TRADELENS_CURATED_BUCKET", "my-bucket")
    sql = "LOCATION 's3://${TRADELENS_CURATED_BUCKET}/gold/fills/'"
    assert _expand_env(sql) == "LOCATION 's3://my-bucket/gold/fills/'"


def test_expand_env_missing_var_raises(monkeypatch):
    monkeypatch.delenv("TRADELENS_CURATED_BUCKET", raising=False)
    try:
        _expand_env("${TRADELENS_CURATED_BUCKET}")
        assert False, "expected KeyError for an unset placeholder"
    except KeyError:
        pass


def test_split_statements_strips_comments_and_splits_on_semicolon():
    sql = """
    -- a comment line
    CREATE DATABASE IF NOT EXISTS tradelens_db;

    CREATE EXTERNAL TABLE IF NOT EXISTS tradelens_db.fills
    LOCATION 's3://bucket/gold/fills/'
    TBLPROPERTIES ('table_type' = 'DELTA');
    """
    statements = _split_statements(sql)
    assert len(statements) == 2
    assert statements[0].startswith("CREATE DATABASE")
    assert "tradelens_db.fills" in statements[1]
