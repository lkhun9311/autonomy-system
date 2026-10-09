import psycopg
import pytest

from dp.catalog import spark_session
from dp.cli import main as dp_main
from dp.config import load
from dp.explore import camera_lidar_skew_ms
from dp.ingest import latest_commit
from dp.source.av2 import RING, STEREO, source_keys

pytestmark = pytest.mark.integration
LOG = "0bae3b5e-417d-3b03-abaa-806b433233b8"
LIDAR_TS = 315969538559783000


@pytest.fixture(scope="module")
def spark():
    return spark_session(load(), "test-samples")


def _commit(log_id):
    with psycopg.connect(load().pg_dsn) as c:
        return latest_commit(c, log_id)


def _rows(spark, log_id):
    return spark.sql(f"select * from dp.av2.sample where ingest_commit_id = '{_commit(log_id)}'").collect()


def test_one_sample_per_sweep_with_all_nine_cameras(spark):
    assert dp_main(["ingest", LOG]) == 0
    rows = _rows(spark, LOG)
    keys = source_keys(load().data_dir / "sensor" / "val" / LOG)
    sweeps = sorted(ts for (_, s, ts) in keys if s == "lidar")
    assert sorted(r.lidar_ts_ns for r in rows) == sweeps
    assert all(set(r.cam_frame_ts) == set(RING + STEREO) for r in rows)


def test_skew_matches_the_published_exploration_value(spark):
    assert dp_main(["ingest", LOG]) == 0
    (row,) = [r for r in _rows(spark, LOG) if r.lidar_ts_ns == LIDAR_TS]
    published = camera_lidar_skew_ms(load().data_dir / "sensor" / "val" / LOG, LIDAR_TS)
    assert {c: v / 1e6 for c, v in row.cam_skew_ns.items()} == pytest.approx(published)
