"""Normalise one Argoverse 2 Sensor log into the canonical Iceberg tables.

Sensor payloads are referenced, never stored (README: the blob row is the load-bearing decision).
The checksum here is of the source file; S2 moves the bytes into the content-addressed blob store.
"""

import hashlib
import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
from pyarrow import feather
from pyspark.sql import SparkSession

from dp.blob import BlobStore
from dp.normalise.schema import ADDED_COLUMNS, DDL, NS
from dp.sample import nearest_frames

TABLES = tuple(DDL)
_POSE = ("qw", "qx", "qy", "qz", "tx_m", "ty_m", "tz_m")
_INTRINSICS = ("fx_px", "fy_px", "cx_px", "cy_px")
_BOX = ("length_m", "width_m", "height_m")


def ensure_tables(spark: SparkSession) -> None:
    spark.sql(f"create namespace if not exists {NS}")
    for ddl in DDL.values():
        spark.sql(ddl)
    for table, columns in ADDED_COLUMNS.items():
        have = set(spark.table(f"{NS}.{table}").columns)
        missing = [f"{name} {kind}" for name, kind in columns if name not in have]
        if missing:
            spark.sql(f"alter table {NS}.{table} add columns ({', '.join(missing)})")


def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _sensor_file(
    store: BlobStore, log: str, sensor: str, p: Path, codec: str, cid: str, calibrated: bool = True
) -> dict:
    checksum = _sha256(p)
    ref = store.put(p, checksum)
    return {
        "log_id": log,
        "sensor": sensor,
        "timestamp_ns": int(p.stem),
        "source_path": str(p),
        "byte_size": p.stat().st_size,
        "checksum": checksum,
        "codec": codec,
        "calibration_id": f"{log}/{sensor}" if calibrated else None,
        "ingest_commit_id": cid,
        "blob_uri": ref.uri,
        "blob_version_id": ref.version_id,
    }


def _sensor_files(log_dir: Path) -> Iterator[tuple[str, Path, str, bool]]:
    lidar = log_dir / "sensors" / "lidar"
    for p in sorted(lidar.glob("*.feather")) if lidar.is_dir() else []:
        if p.stem.isdigit():
            # AV2 merges up_lidar and down_lidar into one sweep already in the ego-vehicle frame.
            yield "lidar", p, "feather", False
    cams = log_dir / "sensors" / "cameras"
    for cam in sorted(cams.iterdir()) if cams.is_dir() else []:
        for p in sorted(cam.glob("*.jpg")):
            if p.stem.isdigit():
                yield cam.name, p, "jpeg", True


def _sensor_rows(log_dir: Path, cid: str, store: BlobStore) -> list[dict]:
    log = log_dir.name
    files = list(_sensor_files(log_dir))
    with ThreadPoolExecutor(max_workers=16) as pool:  # one boto3 client, shared: clients are thread-safe
        return list(
            pool.map(lambda f: _sensor_file(store, log, f[0], f[1], f[2], cid, calibrated=f[3]), files)
        )


def _sample_rows(log: str, sensors: list[dict], cid: str) -> list[dict]:
    """One sample per lidar sweep, from the sensor rows just written, so samples and rows agree."""
    frames: dict[str, list[int]] = {}
    sweeps: list[int] = []
    for r in sensors:
        if r["sensor"] == "lidar":
            sweeps.append(r["timestamp_ns"])
        else:
            frames.setdefault(r["sensor"], []).append(r["timestamp_ns"])
    for ts in frames.values():
        ts.sort()
    rows = []
    for lidar_ts in sorted(sweeps):
        near = nearest_frames(lidar_ts, frames)
        rows.append(
            {
                "log_id": log,
                "lidar_ts_ns": lidar_ts,
                "cam_frame_ts": near,
                "cam_skew_ns": {cam: ts - lidar_ts for cam, ts in near.items()},
                "ingest_commit_id": cid,
            }
        )
    return rows


def _one(log_dir: Path, pattern: str) -> Path:
    found = sorted((log_dir / "map").glob(pattern))
    if len(found) != 1:
        raise ValueError(f"{log_dir.name}: expected one map/{pattern}, found {len(found)}")
    return found[0]


def _map_raster_rows(log_dir: Path, cid: str, store: BlobStore) -> list[dict]:
    npy = _one(log_dir, "*_ground_height_surface____*.npy")
    sim2 = _one(log_dir, "*___img_Sim2_city.json")
    checksum = _sha256(npy)
    ref = store.put(npy, checksum)
    height, width = np.load(npy, mmap_mode="r").shape
    return [
        {
            "log_id": log_dir.name,
            "raster_blob_uri": ref.uri,
            "raster_blob_version_id": ref.version_id,
            "raster_checksum": checksum,
            "sim2_json": sim2.read_text(),
            "height_px": int(height),
            "width_px": int(width),
            "ingest_commit_id": cid,
        }
    ]


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


def normalise_log(
    spark: SparkSession, log_dir: Path, ingest_commit_id: str, store: BlobStore
) -> dict[str, int]:
    """Append one log to every table under ingest_commit_id; returns rows written per table.

    Test hook: DP_CRASH_AFTER_TABLE=<table> exits with 99 after that table's append.
    """
    ensure_tables(spark)
    sensors = _sensor_rows(log_dir, ingest_commit_id, store)
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
        "sample": _sample_rows(log_dir.name, sensors, ingest_commit_id),
        "map_raster": _map_raster_rows(log_dir, ingest_commit_id, store),
        "pose": list(_pose_rows(log_dir, ingest_commit_id)),
        "calibration": list(_calibration_rows(log_dir, ingest_commit_id)),
        "track": list(_track_rows(log_dir, ingest_commit_id)),
        "log": [log_row],
    }
    written = {}
    for table in ("sensor_data", "sample", "map_raster", "pose", "calibration", "track", "log"):
        if parts[table]:
            _append(spark, table, parts[table])
        written[table] = len(parts[table])
        if os.environ.get("DP_CRASH_AFTER_TABLE") == table:
            raise SystemExit(99)  # test hook: simulate a crash between table appends
    return written
