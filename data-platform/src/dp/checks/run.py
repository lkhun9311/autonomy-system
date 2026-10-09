"""dp check: six sensor-check values per sample, behind a check commit (spec §3.1, §9).

Values only; judging is S3b. Sensor bytes are read through their blob pins, never through source paths.
Rows carry a check_commit_id and the Postgres check_commit row is written last, so a crash leaves no
visible rows (the S1 ingest_commit pattern).

Test hook: DP_CRASH_AFTER_CHECK=<check> exits with 99 after that check's append.
"""

import io
import os
import uuid
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import psycopg
from pyarrow import feather

from dp.blob import BlobRef, BlobStore, s3_client
from dp.catalog import spark_session
from dp.checks import CHECK_SET_VERSION, CHECK_VERSIONS
from dp.checks.ground import Sim2, ground_residual
from dp.checks.lidar import laser_returns, points_range, sweep_timing
from dp.checks.pose import pose_continuity, quat_to_rot
from dp.checks.sync import camera_skew
from dp.config import load
from dp.ingest import ensure_schema, latest_commit
from dp.normalise.schema import NS
from dp.normalise.spark_job import append_rows, ensure_tables

_FIND = "select id from check_commit where ingest_commit_id = %s and check_set_version = %s"


def _load_inputs(spark, store: BlobStore, cid: str):
    where = f"where ingest_commit_id = '{cid}'"
    sweeps = sorted(
        r.lidar_ts_ns for r in spark.sql(f"select lidar_ts_ns from {NS}.sample {where}").collect()
    )
    pins: dict[int, BlobRef] = {}
    frames: dict[str, list[int]] = {}
    for r in spark.sql(
        f"select sensor, timestamp_ns, blob_uri, blob_version_id from {NS}.sensor_data {where}"
    ).collect():
        if r.sensor == "lidar":
            pins[r.timestamp_ns] = BlobRef(r.blob_uri, r.blob_version_id)
        else:
            frames.setdefault(r.sensor, []).append(r.timestamp_ns)
    for ts in frames.values():
        ts.sort()
    poses = sorted(spark.sql(f"select * from {NS}.pose {where}").collect(), key=lambda r: r.timestamp_ns)
    pose_ts = np.array([p.timestamp_ns for p in poses], dtype=np.int64)
    pose_xyz = np.array([[p.tx_m, p.ty_m, p.tz_m] for p in poses], dtype=np.float64).reshape(-1, 3)
    pose_rot = [quat_to_rot(p.qw, p.qx, p.qy, p.qz) for p in poses]
    (m,) = spark.sql(f"select * from {NS}.map_raster {where}").collect()
    raster = np.load(io.BytesIO(store.get(BlobRef(m.raster_blob_uri, m.raster_blob_version_id))))
    return sweeps, pins, frames, pose_ts, pose_xyz, pose_rot, raster, Sim2.from_json(m.sim2_json)


def _sample_values(store, pin, lidar_ts, frames, pose_ts, pose_xyz, pose_rot, raster, sim2) -> dict:
    t = feather.read_table(io.BytesIO(store.get(pin)))
    xyz = np.column_stack([t[c].to_numpy().astype(np.float64) for c in ("x", "y", "z")])
    laser = t["laser_number"].to_numpy()
    offset = t["offset_ns"].to_numpy()
    i = int(np.searchsorted(pose_ts, lidar_ts))
    at = i < len(pose_ts) and int(pose_ts[i]) == lidar_ts
    R, tv = (pose_rot[i], pose_xyz[i]) if at else (None, None)
    return {
        "laser_returns": laser_returns(laser),
        "points_range": points_range(xyz),
        "sweep_timing": sweep_timing(laser, offset),
        "camera_skew": camera_skew(lidar_ts, frames),
        "pose_continuity": pose_continuity(pose_ts, pose_xyz, lidar_ts),
        "ground_residual": ground_residual(xyz, R, tv, raster, sim2),
    }


def run_checks(log_id: str) -> tuple[str, int]:
    """Returns (check commit id, samples checked). Idempotent on (ingest commit, check set version)."""
    s = load()
    with psycopg.connect(s.pg_dsn, autocommit=True) as conn:
        ensure_schema(conn)
        cid = latest_commit(conn, log_id)
        if cid is None:
            raise ValueError(f"{log_id} not ingested")
        spark = spark_session(s, "check")
        hit = conn.execute(_FIND, (cid, CHECK_SET_VERSION)).fetchone()
        if hit:
            n = (
                spark.sql(f"select count(*) n from {NS}.sample where ingest_commit_id = '{cid}'")
                .collect()[0]
                .n
            )
            return str(hit[0]), n
        ensure_tables(spark)
        store = BlobStore(s3_client(s))
        sweeps, pins, frames, pose_ts, pose_xyz, pose_rot, raster, sim2 = _load_inputs(spark, store, cid)
        missing = [ts for ts in sweeps if ts not in pins]
        if missing:
            raise ValueError(f"{log_id}: {len(missing)} samples have no lidar blob pin, first {missing[0]}")
        with ThreadPoolExecutor(max_workers=16) as pool:
            values = list(
                pool.map(
                    lambda ts: _sample_values(
                        store, pins[ts], ts, frames, pose_ts, pose_xyz, pose_rot, raster, sim2
                    ),
                    sweeps,
                )
            )
        ccid = str(uuid.uuid4())
        for check, version in CHECK_VERSIONS.items():
            rows = [
                {
                    "log_id": log_id,
                    "lidar_ts_ns": ts,
                    "check": check,
                    "check_version": version,
                    "metrics": v[check],
                    "ingest_commit_id": cid,
                    "check_commit_id": ccid,
                }
                for ts, v in zip(sweeps, values, strict=True)
            ]
            append_rows(spark, "sweep_stat", rows)
            if os.environ.get("DP_CRASH_AFTER_CHECK") == check:
                raise SystemExit(99)  # test hook: simulate a crash between check appends
        conn.execute(
            "insert into check_commit(id, ingest_commit_id, check_set_version) values (%s, %s, %s) "
            "on conflict (ingest_commit_id, check_set_version) do nothing",
            (ccid, cid, CHECK_SET_VERSION),
        )
        return str(conn.execute(_FIND, (cid, CHECK_SET_VERSION)).fetchone()[0]), len(sweeps)
