import os
import shutil
import uuid
from pathlib import Path

import pytest

from dp.config import load

FIXTURES = Path(__file__).parents[2] / "fixtures" / "val-3logs.txt"


@pytest.fixture
def fresh_log(monkeypatch):
    """A hard-linked copy of the first fixture log under a new log id, so ingest always runs the current
    transform instead of hitting an existing commit. Yields (log_id, directory)."""
    real_id = FIXTURES.read_text().split()[0]
    real = load().data_dir / "sensor" / "val" / real_id
    if not real.exists():
        pytest.skip("fixture log not fetched")
    root = load().data_dir.parent / f"dp-test-fresh-{uuid.uuid4().hex[:8]}"
    log_id = f"{real_id}-fresh-{uuid.uuid4().hex[:8]}"
    d = root / "sensor" / "val" / log_id
    shutil.copytree(real, d, copy_function=os.link)
    monkeypatch.setenv("DP_DATA", str(root))
    try:
        yield log_id, d
    finally:
        shutil.rmtree(root, ignore_errors=True)
