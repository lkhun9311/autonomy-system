import os
import shutil
import uuid
from collections import Counter
from pathlib import Path

import pytest

from dp.catalog import spark_session
from dp.cli import main as dp_main
from dp.config import load
from dp.normalise.spark_job import TABLES, ensure_tables, normalise_log
from dp.source.av2 import source_keys

pytestmark = pytest.mark.integration
LOG = "0bae3b5e-417d-3b03-abaa-806b433233b8"


@pytest.fixture(scope="module")
def spark():
    return spark_session(load(), "test-ingest")


def _log_dir():
    d = load().data_dir / "sensor" / "val" / LOG
    if not d.exists():
        pytest.skip("log not fetched")
    return d


def test_sensor_rows_match_source_keys(spark):
    d = _log_dir()
    ensure_tables(spark)
    cid = str(uuid.uuid4())
    written = normalise_log(spark, d, cid)
    rows = spark.sql(
        f"select log_id, sensor, timestamp_ns from dp.av2.sensor_data where ingest_commit_id = '{cid}'"
    ).collect()
    assert Counter((r.log_id, r.sensor, r.timestamp_ns) for r in rows) == source_keys(d)
    assert written["sensor_data"] == sum(source_keys(d).values())
    assert set(TABLES) == {"log", "sensor_data", "pose", "calibration", "track"}


def _fixture_ids() -> list[str]:
    ids = (Path(__file__).parents[2] / "fixtures" / "val-3logs.txt").read_text().split()
    for i in ids:
        if not (load().data_dir / "sensor" / "val" / i).exists():
            pytest.skip("logs not fetched")
    return ids


def test_three_logs_reconcile_to_100_percent_and_rerun_adds_nothing(spark):
    ids = _fixture_ids()
    assert dp_main(["ingest", *ids]) == 0
    before = spark.sql("select count(*) c from dp.av2.sensor_data").collect()[0].c
    assert dp_main(["reconcile", *ids]) == 0
    assert dp_main(["ingest", *ids]) == 0
    after = spark.sql("select count(*) c from dp.av2.sensor_data").collect()[0].c
    assert after == before


def test_reconcile_reports_a_frame_the_latest_commit_lacks(spark, monkeypatch, capsys):
    # A hard-linked copy under its own log id, so the real log's rows are never touched.
    real = _fixture_ids()[0]
    root = load().data_dir.parent / "dp-test-broken"
    shutil.rmtree(root, ignore_errors=True)
    log_id = f"{real}-broken-{uuid.uuid4().hex[:8]}"
    d = root / "sensor" / "val" / log_id
    shutil.copytree(load().data_dir / "sensor" / "val" / real, d, copy_function=os.link)
    try:
        frame = next((d / "sensors" / "cameras" / "ring_side_left").glob("*.jpg"))
        held = frame.with_suffix(".held")
        frame.rename(held)
        monkeypatch.setenv("DP_DATA", str(root))
        assert dp_main(["ingest", log_id]) == 0
        held.rename(frame)
        capsys.readouterr()
        assert dp_main(["reconcile", log_id]) == 1
        assert "missing=1 " in capsys.readouterr().out
    finally:
        shutil.rmtree(root, ignore_errors=True)
