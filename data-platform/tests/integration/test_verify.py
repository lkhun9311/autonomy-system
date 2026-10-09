import hashlib
import os
import shutil
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from dp.blob import BUCKET, BlobRef, BlobStore, key_for, s3_client
from dp.catalog import spark_session
from dp.cli import main as dp_main
from dp.config import load
from dp.verify import pinned_rows, verify_rows

pytestmark = pytest.mark.integration


def _tomorrow():
    return datetime.now(UTC) + timedelta(days=1)


def _store():
    return BlobStore(s3_client(load()))


def _row(data: bytes, ref=None):
    return {
        "blob_uri": ref.uri if ref else None,
        "blob_version_id": ref.version_id if ref else None,
        "checksum": hashlib.sha256(data).hexdigest(),
    }


def _fixture_ids():
    return (Path(__file__).parents[2] / "fixtures" / "val-3logs.txt").read_text().split()


@contextmanager
def _copy_with_one_unique_frame(tag, monkeypatch):
    """A hard-linked copy of the first fixture log whose one camera frame has bytes no other file has."""
    real = load().data_dir / "sensor" / "val" / _fixture_ids()[0]
    root = load().data_dir.parent / f"dp-test-{tag}"
    shutil.rmtree(root, ignore_errors=True)
    log_id = f"{real.name}-{tag}-{uuid.uuid4().hex[:8]}"
    d = root / "sensor" / "val" / log_id
    shutil.copytree(real, d, copy_function=os.link)
    frame = next((d / "sensors" / "cameras" / "ring_front_center").glob("*.jpg"))
    unique = frame.read_bytes() + uuid.uuid4().bytes
    frame.unlink()  # break the hard link before writing
    frame.write_bytes(unique)
    monkeypatch.setenv("DP_DATA", str(root))
    try:
        yield log_id, hashlib.sha256(unique).hexdigest()
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_rows_without_a_blob_are_failures_not_skips():
    r = verify_rows(_store(), [_row(b"x")])
    assert r.checked == 1 and [f.actual for f in r.failures] == ["no blob pinned"]


def test_a_missing_version_is_named():
    data = f"verify {uuid.uuid4()}".encode()
    ref = _store().put(data, hashlib.sha256(data).hexdigest(), retain_until=_tomorrow())
    bogus = BlobRef(ref.uri, "00000000-0000-0000-0000-000000000000")
    (f,) = verify_rows(_store(), [_row(data, bogus)]).failures
    assert f.object == ref.uri and f.actual.startswith("version missing")


def test_the_fixture_logs_verify_clean(capsys):
    ids = _fixture_ids()
    assert dp_main(["ingest", *ids]) == 0
    capsys.readouterr()
    assert dp_main(["verify-blobs", *ids]) == 0, capsys.readouterr().out


def test_overwriting_a_key_after_ingest_does_not_change_what_verifies(monkeypatch, capsys):
    with _copy_with_one_unique_frame("overwrite", monkeypatch) as (log_id, sha):
        assert dp_main(["ingest", log_id]) == 0
        s3 = _store().s3
        s3.put_object(
            Bucket=BUCKET,
            Key=key_for(sha),
            Body=b"later",
            Metadata={"sha256": sha},
            ObjectLockMode="COMPLIANCE",
            ObjectLockRetainUntilDate=_tomorrow(),
        )
        s3.delete_object(Bucket=BUCKET, Key=key_for(sha))  # and a delete marker on top
        capsys.readouterr()
        assert dp_main(["verify-blobs", log_id]) == 0, capsys.readouterr().out


def test_a_forged_current_version_is_pinned_by_ingest_and_caught_by_verify(monkeypatch, capsys):
    with _copy_with_one_unique_frame("forged", monkeypatch) as (log_id, sha):
        _store().s3.put_object(
            Bucket=BUCKET,
            Key=key_for(sha),
            Body=b"forged",
            Metadata={"sha256": sha},
            ObjectLockMode="COMPLIANCE",
            ObjectLockRetainUntilDate=_tomorrow(),
        )
        assert dp_main(["ingest", log_id]) == 0
        capsys.readouterr()
        assert dp_main(["verify-blobs", log_id]) == 1
        out = capsys.readouterr().out
        assert "failures=1" in out and key_for(sha) in out and hashlib.sha256(b"forged").hexdigest() in out


def test_a_table_from_before_s2_reports_no_blob_pinned_instead_of_crashing():
    spark = spark_session(load(), "verify-s1-schema")
    ns = f"dp.verify_probe_{uuid.uuid4().hex[:8]}"
    spark.sql(f"create namespace {ns}")
    try:
        spark.sql(
            f"create table {ns}.sensor_data (log_id string, sensor string, timestamp_ns bigint, "
            "checksum string, ingest_commit_id string) using iceberg"
        )
        spark.sql(f"insert into {ns}.sensor_data values ('L', 'lidar', 1, 'ab', 'c1')")
        rows = pinned_rows(spark, f"{ns}.sensor_data", "L", "c1")
        assert rows == [{"blob_uri": None, "blob_version_id": None, "checksum": "ab"}]
        assert [f.actual for f in verify_rows(_store(), rows).failures] == ["no blob pinned"]
    finally:
        spark.sql(f"drop table if exists {ns}.sensor_data purge")
        spark.sql(f"drop namespace if exists {ns}")


def test_a_forged_ground_raster_is_caught_by_verify(fresh_log, capsys):
    import numpy as np

    log_id, d = fresh_log
    npy = next((d / "map").glob("*_ground_height_surface____*.npy"))
    arr = np.load(npy)
    arr.flat[0] = arr.flat[0] + 1e-3 if np.isfinite(arr.flat[0]) else 0.123  # bytes no other log has
    npy.unlink()  # break the hard link before writing
    np.save(npy, arr)
    sha = hashlib.sha256(npy.read_bytes()).hexdigest()
    _store().s3.put_object(
        Bucket=BUCKET,
        Key=key_for(sha),
        Body=b"forged raster",
        Metadata={"sha256": sha},
        ObjectLockMode="COMPLIANCE",
        ObjectLockRetainUntilDate=_tomorrow(),
    )
    assert dp_main(["ingest", log_id]) == 0
    capsys.readouterr()
    assert dp_main(["verify-blobs", log_id]) == 1
    out = capsys.readouterr().out
    assert key_for(sha) in out and hashlib.sha256(b"forged raster").hexdigest() in out
