"""A log is ingested only when its ingest_commit row exists (spec §3.2).

The row is written after every table append succeeded; readers select rows whose ingest_commit_id is
committed, and per log only the last commit (spec §5).
"""

import hashlib
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg

from dp.canonical import digest
from dp.catalog import pyiceberg_catalog, spark_session
from dp.config import load
from dp.normalise.spark_job import TABLES, normalise_log

DDL = """create table if not exists ingest_commit (
  id uuid primary key, log_id text not null, source_digest text not null, transform_version text not null,
  snapshots jsonb not null, committed_at timestamptz not null default clock_timestamp(),
  unique (log_id, source_digest, transform_version))"""
_FIND = "select id from ingest_commit where log_id=%s and source_digest=%s and transform_version=%s"


def ensure_schema(conn: psycopg.Connection) -> None:
    conn.execute(DDL)


def _file_item(log_dir: Path, p: Path) -> bytes:
    with p.open("rb") as f:
        sha = hashlib.file_digest(f, "sha256").hexdigest()
    return f"{p.relative_to(log_dir)}|{sha}".encode()


def source_digest(log_dir: Path) -> str:
    """Digest of (relative path, content sha256) for every file, so a same-size edit is a new source."""
    files = sorted(p for p in log_dir.rglob("*") if p.is_file())
    with ThreadPoolExecutor() as pool:  # hashlib releases the GIL on large reads
        return digest(list(pool.map(lambda p: _file_item(log_dir, p), files)))


def committed_ids(conn: psycopg.Connection) -> set[str]:
    return {str(r[0]) for r in conn.execute("select id from ingest_commit")}


def latest_commit(conn: psycopg.Connection, log_id: str) -> str | None:
    row = conn.execute(
        "select id from ingest_commit where log_id=%s order by committed_at desc, id limit 1", (log_id,)
    ).fetchone()
    return str(row[0]) if row else None


def ingest(log_dir: Path, transform_version: str) -> str:
    s = load()
    sd = source_digest(log_dir)
    key = (log_dir.name, sd, transform_version)
    with psycopg.connect(s.pg_dsn, autocommit=True) as conn:
        ensure_schema(conn)
        hit = conn.execute(_FIND, key).fetchone()
        if hit:
            return str(hit[0])
        cid = str(uuid.uuid4())
        normalise_log(spark_session(s, "ingest"), log_dir, cid)
        cat = pyiceberg_catalog(s)
        snaps = {t: cat.load_table(f"av2.{t}").current_snapshot().snapshot_id for t in TABLES}
        conn.execute(
            "insert into ingest_commit(id, log_id, source_digest, transform_version, snapshots) "
            "values (%s, %s, %s, %s, %s) on conflict (log_id, source_digest, transform_version) do nothing",
            (cid, *key, json.dumps(snaps)),
        )
        return str(conn.execute(_FIND, key).fetchone()[0])
