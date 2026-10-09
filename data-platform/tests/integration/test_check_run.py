import os
import subprocess

import psycopg
import pyarrow.compute as pc
import pytest
from pyarrow import feather

from dp.catalog import spark_session
from dp.checks import CHECK_VERSIONS
from dp.cli import main as dp_main
from dp.config import load
from dp.ingest import latest_commit

pytestmark = pytest.mark.integration
LOG = "0bae3b5e-417d-3b03-abaa-806b433233b8"


@pytest.fixture(scope="module")
def spark():
    return spark_session(load(), "test-check-run")


def _check_commit(log_id):
    with psycopg.connect(load().pg_dsn) as c:
        cid = latest_commit(c, log_id)
        row = c.execute("select id from check_commit where ingest_commit_id = %s", (cid,)).fetchone()
    return cid, (str(row[0]) if row else None)


def _count(spark, where=""):
    return spark.sql(f"select count(*) n from dp.av2.sweep_stat {where}").collect()[0].n


def test_six_checks_per_sample_and_rerun_adds_nothing(spark):
    assert dp_main(["ingest", LOG]) == 0
    assert dp_main(["check", LOG]) == 0
    cid, ccid = _check_commit(LOG)
    samples = (
        spark.sql(f"select count(*) n from dp.av2.sample where ingest_commit_id = '{cid}'").collect()[0].n
    )
    rows = spark.sql(f"select check from dp.av2.sweep_stat where check_commit_id = '{ccid}'").collect()
    assert samples > 0 and len(rows) == samples * len(CHECK_VERSIONS)
    assert {r.check for r in rows} == set(CHECK_VERSIONS)
    before = _count(spark)
    assert dp_main(["check", LOG]) == 0
    assert _count(spark) == before


def test_a_removed_laser_shows_as_zero_returns_in_that_sample(spark, fresh_log):
    log_id, d = fresh_log
    sweep = sorted((d / "sensors" / "lidar").glob("*.feather"))[10]
    t = feather.read_table(sweep)
    sweep.unlink()  # break the hard link before writing
    feather.write_feather(t.filter(pc.not_equal(t["laser_number"], 5)), sweep)
    assert dp_main(["ingest", log_id]) == 0 and dp_main(["check", log_id]) == 0
    _, ccid = _check_commit(log_id)
    (r,) = spark.sql(
        f"select metrics from dp.av2.sweep_stat where check_commit_id = '{ccid}' "
        f"and check = 'laser_returns' and lidar_ts_ns = {int(sweep.stem)}"
    ).collect()
    assert r.metrics["laser_05"] == 0.0


def test_a_crash_between_checks_leaves_no_visible_rows_and_rerun_completes(spark):
    assert dp_main(["ingest", LOG]) == 0
    with psycopg.connect(load().pg_dsn, autocommit=True) as c:  # force a fresh check commit for this run
        c.execute("delete from check_commit where ingest_commit_id = %s", (latest_commit(c, LOG),))
    r = subprocess.run(
        ["uv", "run", "python", "-c", f"from dp.checks.run import run_checks; run_checks('{LOG}')"],
        env=dict(os.environ, DP_CRASH_AFTER_CHECK="sweep_timing"),
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode == 99, r.stderr[-800:]
    assert _check_commit(LOG)[1] is None
    assert dp_main(["check", LOG]) == 0
    assert _check_commit(LOG)[1] is not None
