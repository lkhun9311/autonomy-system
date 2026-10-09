"""Normalise one Argoverse 2 Sensor log into the canonical Iceberg tables.

Sensor payloads are referenced, never stored (README: the blob row is the load-bearing decision).
The checksum here is of the source file; S2 moves the bytes into the content-addressed blob store.
"""

import hashlib
import os
from collections.abc import Iterator
from pathlib import Path

from pyarrow import feather
from pyspark.sql import SparkSession

from dp.normalise.schema import DDL, NS

TABLES = tuple(DDL)
_POSE = ("qw", "qx", "qy", "qz", "tx_m", "ty_m", "tz_m")
_INTRINSICS = ("fx_px", "fy_px", "cx_px", "cy_px")
_BOX = ("length_m", "width_m", "height_m")


def ensure_tables(spark: SparkSession) -> None:
    spark.sql(f"create namespace if not exists {NS}")
    for ddl in DDL.values():
        spark.sql(ddl)


def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _sensor_file(log: str, sensor: str, p: Path, codec: str, cid: str, calibrated: bool = True) -> dict:
    return {
        "log_id": log,
        "sensor": sensor,
        "timestamp_ns": int(p.stem),
        "source_path": str(p),
        "byte_size": p.stat().st_size,
        "checksum": _sha256(p),
        "codec": codec,
        "calibration_id": f"{log}/{sensor}" if calibrated else None,
        "ingest_commit_id": cid,
    }


def _sensor_rows(log_dir: Path, cid: str) -> Iterator[dict]:
    log = log_dir.name
    lidar = log_dir / "sensors" / "lidar"
    for p in sorted(lidar.glob("*.feather")) if lidar.is_dir() else []:
        if p.stem.isdigit():
            # AV2 merges up_lidar and down_lidar into one sweep already in the ego-vehicle frame.
            yield _sensor_file(log, "lidar", p, "feather", cid, calibrated=False)
    cams = log_dir / "sensors" / "cameras"
    for cam in sorted(cams.iterdir()) if cams.is_dir() else []:
        for p in sorted(cam.glob("*.jpg")):
            if p.stem.isdigit():
                yield _sensor_file(log, cam.name, p, "jpeg", cid)


def _read(path: Path) -> list[dict]:
    return feather.read_table(path).to_pylist()


def _pose_rows(log_dir: Path, cid: str) -> Iterator[dict]:
    for r in _read(log_dir / "city_SE3_egovehicle.feather"):
        yield (
            {"log_id": log_dir.name, "timestamp_ns": int(r["timestamp_ns"])}
            | {k: r[k] for k in _POSE}
            | {"ingest_commit_id": cid}
        )


def _calibration_rows(log_dir: Path, cid: str) -> Iterator[dict]:
    ext = {r["sensor_name"]: r for r in _read(log_dir / "calibration" / "egovehicle_SE3_sensor.feather")}
    intr = {r["sensor_name"]: r for r in _read(log_dir / "calibration" / "intrinsics.feather")}
    for name, e in sorted(ext.items()):
        i = intr.get(name, {})
        yield (
            {"log_id": log_dir.name, "sensor": name, "calibration_id": f"{log_dir.name}/{name}"}
            | {k: e[k] for k in _POSE}
            | {k: i.get(k) for k in _INTRINSICS}
            | {"ingest_commit_id": cid}
        )


def _track_rows(log_dir: Path, cid: str) -> Iterator[dict]:
    for r in _read(log_dir / "annotations.feather"):
        yield (
            {"log_id": log_dir.name, "timestamp_ns": int(r["timestamp_ns"])}
            | {"track_uuid": r["track_uuid"], "category": r["category"]}
            | {k: r[k] for k in _BOX + _POSE}
            | {"num_interior_pts": int(r["num_interior_pts"]), "ingest_commit_id": cid}
        )


def _append(spark: SparkSession, table: str, rows: list[dict]) -> None:
    # Build against the table's own schema: inference fails on all-null columns and may widen types.
    schema = spark.table(f"{NS}.{table}").schema
    data = [tuple(r[f] for f in schema.fieldNames()) for r in rows]
    spark.createDataFrame(data, schema).writeTo(f"{NS}.{table}").append()


def normalise_log(spark: SparkSession, log_dir: Path, ingest_commit_id: str) -> dict[str, int]:
    """Append one log to every table under ingest_commit_id; returns rows written per table.

    Test hook: DP_CRASH_AFTER_TABLE=<table> exits with 99 after that table's append.
    """
    ensure_tables(spark)
    sensors = list(_sensor_rows(log_dir, ingest_commit_id))
    ts = [r["timestamp_ns"] for r in sensors] or [0]
    log_row = {
        "log_id": log_dir.name,
        "city": None,
        "start_ns": min(ts),
        "end_ns": max(ts),
        "source_path": str(log_dir),
        "source_digest": None,
        "ingest_commit_id": ingest_commit_id,
    }
    parts = {
        "sensor_data": sensors,
        "pose": list(_pose_rows(log_dir, ingest_commit_id)),
        "calibration": list(_calibration_rows(log_dir, ingest_commit_id)),
        "track": list(_track_rows(log_dir, ingest_commit_id)),
        "log": [log_row],
    }
    written = {}
    for table in ("sensor_data", "pose", "calibration", "track", "log"):
        if parts[table]:
            _append(spark, table, parts[table])
        written[table] = len(parts[table])
        if os.environ.get("DP_CRASH_AFTER_TABLE") == table:
            raise SystemExit(99)  # test hook: simulate a crash between table appends
    return written
