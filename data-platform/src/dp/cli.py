"""dp — the M1 command line. Screen = API = CLI: these names are the ones later screens use."""

import argparse
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

import psycopg

from dp.blob import BlobStore, s3_client
from dp.catalog import spark_session
from dp.config import load
from dp.explore import camera_lidar_skew_ms
from dp.ingest import ensure_schema, ingest, latest_commit
from dp.reconcile import compare
from dp.source.av2 import source_keys
from dp.verify import verify_rows

TRANSFORM_VERSION = "s2"
# The fetch helper lives in the source checkout, not in the wheel: `dp fetch` is a development command.
FETCH_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "fetch_av2.sh"
_LOG_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*")


def _log_id(value: str) -> str:
    # Ids are interpolated into Spark SQL and joined onto paths, so only plain names pass.
    if not _LOG_ID.fullmatch(value):
        raise argparse.ArgumentTypeError(f"not a log id: {value!r}")
    return value


def _log_dir(log_id: str) -> Path:
    return load().data_dir / "sensor" / "val" / log_id


def cmd_verify_blobs(ids: list[str]) -> int:
    s = load()
    with psycopg.connect(s.pg_dsn, autocommit=True) as c:
        ensure_schema(c)
        latest = {log_id: latest_commit(c, log_id) for log_id in ids}
    spark = spark_session(s, "verify-blobs")
    store = BlobStore(s3_client(s))
    rc = 0
    for log_id in ids:
        cid = latest[log_id]
        if cid is None:
            print(f"{log_id} not ingested")
            rc = 1
            continue
        rows = spark.sql(
            "select blob_uri, blob_version_id, checksum from dp.av2.sensor_data "
            f"where log_id = '{log_id}' and ingest_commit_id = '{cid}'"
        ).collect()
        r = verify_rows(store, (row.asDict() for row in rows))
        print(f"{log_id} commit={cid} checked={r.checked} failures={len(r.failures)}")
        for f in r.failures:
            print(f"  {f.object} version={f.version_id} expected={f.expected} actual={f.actual}")
        if r.failures or r.checked == 0:
            rc = 1
    return rc


def cmd_reconcile(ids: list[str]) -> int:
    s = load()
    with psycopg.connect(s.pg_dsn, autocommit=True) as c:
        ensure_schema(c)
        latest = {log_id: latest_commit(c, log_id) for log_id in ids}
    spark = spark_session(s, "reconcile")
    rc = 0
    for log_id in ids:
        cid = latest[log_id]
        if cid is None:
            print(f"{log_id} not ingested")
            rc = 1
            continue
        rows = spark.sql(
            "select log_id, sensor, timestamp_ns from dp.av2.sensor_data "
            f"where log_id = '{log_id}' and ingest_commit_id = '{cid}'"
        ).collect()
        table = Counter((r.log_id, r.sensor, r.timestamp_ns) for r in rows)
        r = compare(source_keys(_log_dir(log_id)), table)
        print(
            f"{log_id} commit={cid} completeness={r.completeness:.6%} missing={sum(r.missing.values())} "
            f"extra={sum(r.extra.values())} duplicate={sum(r.duplicate.values())}"
        )
        if r.completeness != 1.0 or r.extra or r.duplicate:
            rc = 1
    return rc


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="dp")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("fetch").add_argument("list")
    sub.add_parser("ingest").add_argument("log_id", nargs="+", type=_log_id)
    sub.add_parser("reconcile").add_argument("log_id", nargs="+", type=_log_id)
    sub.add_parser("verify-blobs").add_argument("log_id", nargs="+", type=_log_id)
    ex = sub.add_parser("explore")
    ex.add_argument("what", choices=["skew"])
    ex.add_argument("log_id", type=_log_id)
    ex.add_argument("lidar_ts", type=int)
    a = p.parse_args(argv)
    if a.cmd == "fetch":
        if not FETCH_SCRIPT.is_file():
            p.error(f"fetch needs the source checkout; {FETCH_SCRIPT} is missing")
        return subprocess.call([str(FETCH_SCRIPT), a.list])
    if a.cmd == "ingest":
        for log_id in a.log_id:
            print(log_id, ingest(_log_dir(log_id), TRANSFORM_VERSION))
        return 0
    if a.cmd == "verify-blobs":
        return cmd_verify_blobs(a.log_id)
    if a.cmd == "reconcile":
        return cmd_reconcile(a.log_id)
    if a.cmd == "explore":
        for cam, ms in camera_lidar_skew_ms(_log_dir(a.log_id), a.lidar_ts).items():
            print(f"{cam}\t{ms:+.2f} ms")
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
