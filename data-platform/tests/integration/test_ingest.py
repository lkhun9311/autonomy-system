import uuid
from collections import Counter

import pytest

from dp.catalog import spark_session
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
