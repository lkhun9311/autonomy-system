from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from dp.config import load


def test_load_reads_env_and_defaults(monkeypatch, tmp_path):
    monkeypatch.setenv("DP_DATA", str(tmp_path))
    monkeypatch.delenv("DP_PG_DSN", raising=False)
    s = load()
    assert s.data_dir == tmp_path
    assert s.pg_dsn == "postgresql://dp:dp@localhost:5432/dp"
    assert s.catalog_uri == "postgresql+psycopg://dp:dp@localhost:5432/iceberg_catalog"
    assert s.warehouse == "s3://warehouse/"


def test_settings_are_frozen(tmp_path, monkeypatch):
    monkeypatch.setenv("DP_DATA", str(tmp_path))
    s = load()
    with pytest.raises(FrozenInstanceError):
        s.data_dir = Path("/x")  # type: ignore[misc]
