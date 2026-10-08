import hashlib
import os
import shutil
import subprocess
import uuid
from collections import Counter
from contextlib import contextmanager
from pathlib import Path

import psycopg
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField

from dp.blob import BlobRef, BlobStore, key_for, s3_client
from dp.catalog import pyiceberg_catalog, spark_session
from dp.cli import main as dp_main
from dp.config import load
from dp.ingest import committed_ids, ingest, latest_commit, snapshot_ids
from dp.normalise.spark_job import TABLES, ensure_tables, normalise_log
from dp.reconcile import compare
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
    written = normalise_log(spark, d, cid, BlobStore(s3_client(load())))
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


@contextmanager
def _linked_copy(monkeypatch):
    """A hard-linked copy of a fixture log under its own log id, so the real log's rows are never touched."""
    real = _fixture_ids()[0]
    root = load().data_dir.parent / "dp-test-broken"
    shutil.rmtree(root, ignore_errors=True)
    log_id = f"{real}-copy-{uuid.uuid4().hex[:8]}"
    d = root / "sensor" / "val" / log_id
    shutil.copytree(load().data_dir / "sensor" / "val" / real, d, copy_function=os.link)
    monkeypatch.setenv("DP_DATA", str(root))
    try:
        yield log_id, d
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_restoring_an_earlier_source_selects_its_commit_again(spark, monkeypatch, capsys):
    with _linked_copy(monkeypatch) as (log_id, d):
        assert dp_main(["ingest", log_id]) == 0
        frame = next((d / "sensors" / "cameras" / "ring_side_left").glob("*.jpg"))
        held = frame.with_suffix(".held")
        frame.rename(held)
        assert dp_main(["ingest", log_id]) == 0  # state B: one frame gone
        held.rename(frame)
        assert dp_main(["ingest", log_id]) == 0  # back to state A: reuses A's commit
        capsys.readouterr()
        assert dp_main(["reconcile", log_id]) == 0, capsys.readouterr().out


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


def _ids_in(spark, table: str, log: str) -> set[str]:
    rows = spark.sql(f"select distinct ingest_commit_id from dp.av2.{table} where log_id = '{log}'").collect()
    return {r.ingest_commit_id for r in rows}


def test_crash_between_tables_leaves_no_committed_rows_and_rerun_matches(spark):
    log = _fixture_ids()[2]
    src = load().data_dir / "sensor" / "val" / log
    version = f"crash-test-{uuid.uuid4().hex[:8]}"  # a fresh key every run, so the crash path always runs
    before = {t: _ids_in(spark, t, log) for t in ("sensor_data", "pose", "track")}
    r = subprocess.run(
        [
            "uv",
            "run",
            "python",
            "-c",
            f"from dp.ingest import ingest; ingest(__import__('pathlib').Path('{src}'), '{version}')",
        ],
        env=dict(os.environ, DP_CRASH_AFTER_TABLE="pose"),
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode == 99, r.stderr[-800:]
    with psycopg.connect(load().pg_dsn) as c:
        assert (
            c.execute(
                "select count(*) from ingest_commit where transform_version = %s", (version,)
            ).fetchone()[0]
            == 0
        )
        committed = committed_ids(c)
    new = {t: _ids_in(spark, t, log) - before[t] for t in before}
    assert len(new["sensor_data"]) == 1 and new["pose"] == new["sensor_data"]  # written before the crash
    assert not new["track"]  # never reached
    assert not new["sensor_data"] & committed  # present, but not visible to committed readers

    cid = ingest(src, version)
    with psycopg.connect(load().pg_dsn) as c:
        assert cid in committed_ids(c)
    rows = spark.sql(
        f"select log_id, sensor, timestamp_ns from dp.av2.sensor_data where ingest_commit_id = '{cid}'"
    ).collect()
    assert (
        compare(source_keys(src), Counter((x.log_id, x.sensor, x.timestamp_ns) for x in rows)).completeness
        == 1.0
    )


def test_every_calibration_reference_resolves_and_lidar_is_in_the_ego_frame(spark, monkeypatch):
    with _linked_copy(monkeypatch) as (log_id, _):
        assert dp_main(["ingest", log_id]) == 0
        with psycopg.connect(load().pg_dsn) as c:
            cid = latest_commit(c, log_id)
        dangling = spark.sql(
            f"""select s.sensor, s.calibration_id from dp.av2.sensor_data s
                left join dp.av2.calibration k
                  on k.calibration_id = s.calibration_id and k.ingest_commit_id = s.ingest_commit_id
                where s.ingest_commit_id = '{cid}' and s.calibration_id is not null and k.calibration_id is null"""
        ).collect()
        assert dangling == []
        lidar = spark.sql(
            f"select count(*) n, count(calibration_id) k from dp.av2.sensor_data "
            f"where ingest_commit_id = '{cid}' and sensor = 'lidar'"
        ).collect()[0]
        assert lidar.n > 0 and lidar.k == 0


def test_a_table_without_snapshots_is_recorded_as_none():
    cat = pyiceberg_catalog(load())
    ns = f"probe_{uuid.uuid4().hex[:8]}"
    cat.create_namespace(ns)
    try:
        cat.create_table(f"{ns}.empty", Schema(NestedField(1, "x", LongType(), required=False)))
        assert snapshot_ids(cat, ns, ["empty"]) == {"empty": None}
    finally:
        cat.drop_table(f"{ns}.empty")
        cat.drop_namespace(ns)


def _latest_rows(spark, log_id):
    with psycopg.connect(load().pg_dsn) as c:
        cid = latest_commit(c, log_id)
    return spark.sql(
        "select sensor, timestamp_ns, checksum, blob_uri, blob_version_id from dp.av2.sensor_data "
        f"where ingest_commit_id = '{cid}'"
    ).collect()


def test_every_sensor_row_pins_a_content_named_blob_whose_bytes_match(spark, monkeypatch):
    # A fresh log id every run: re-ingesting a fixture log would hit its existing commit and test nothing.
    with _linked_copy(monkeypatch) as (log_id, _):
        assert dp_main(["ingest", log_id]) == 0
        rows = _latest_rows(spark, log_id)
    assert rows and all(r.blob_uri == f"s3://blobs/{key_for(r.checksum)}" and r.blob_version_id for r in rows)
    store = BlobStore(s3_client(load()))
    for r in sorted(rows, key=lambda r: (r.sensor, r.timestamp_ns))[::300]:
        assert hashlib.sha256(store.get(BlobRef(r.blob_uri, r.blob_version_id))).hexdigest() == r.checksum


def test_the_same_files_under_another_log_pin_the_same_versions(spark, monkeypatch):
    real = _fixture_ids()[0]
    assert dp_main(["ingest", real]) == 0
    pins = {r.checksum: r.blob_version_id for r in _latest_rows(spark, real)}
    with _linked_copy(monkeypatch) as (log_id, _):
        assert dp_main(["ingest", log_id]) == 0
        copy = {r.checksum: r.blob_version_id for r in _latest_rows(spark, log_id)}
    assert copy == pins
