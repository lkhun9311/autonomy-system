import subprocess

import psycopg
import pytest
import trino

from dp.config import load

pytestmark = pytest.mark.integration


def test_postgres_has_both_databases():
    with psycopg.connect(load().pg_dsn) as c:
        names = {r[0] for r in c.execute("select datname from pg_database")}
    assert {"dp", "iceberg_catalog"} <= names


def test_postgres_is_ready_for_logical_decoding():
    with psycopg.connect(load().pg_dsn) as c:
        assert c.execute("show wal_level").fetchone()[0] == "logical"


def test_minio_buckets_exist_and_blobs_is_locked():
    out = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "minio-init",
            "mc",
            "retention",
            "info",
            "local/blobs",
            "--default",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert out.returncode == 0, out.stderr[-500:]
    assert "COMPLIANCE" in out.stdout.upper() or "GOVERNANCE" in out.stdout.upper(), out.stdout


def test_volumes_survive_restart():
    with psycopg.connect(load().pg_dsn, autocommit=True) as c:
        c.execute("create table if not exists stack_probe(x int)")
        c.execute("insert into stack_probe values (1)")
    r = subprocess.run(
        ["docker", "compose", "restart", "postgres"], capture_output=True, text=True, check=False
    )
    assert r.returncode == 0, r.stderr[-500:]
    subprocess.run(["docker", "compose", "exec", "-T", "postgres", "pg_isready", "-t", "30"], check=True)
    with psycopg.connect(load().pg_dsn) as c:
        assert c.execute("select count(*) from stack_probe").fetchone()[0] >= 1


def _trino(sql: str) -> list:
    cur = trino.dbapi.connect(host="localhost", port=8080, user="dp", catalog="iceberg").cursor()
    cur.execute(sql)
    return cur.fetchall()


def test_trino_catalog_survives_postgres_restart():
    # Postgres ends pooled sessions with SQLSTATE 57P01; Trino must reconnect, not fail every query.
    _trino("create schema if not exists iceberg.stack_probe")
    _trino("create table if not exists iceberg.stack_probe.t (x integer)")
    r = subprocess.run(
        ["docker", "compose", "restart", "postgres"], capture_output=True, text=True, check=False
    )
    assert r.returncode == 0, r.stderr[-500:]
    subprocess.run(["docker", "compose", "exec", "-T", "postgres", "pg_isready", "-t", "30"], check=True)
    for _ in range(5):
        assert _trino("select count(*) from iceberg.stack_probe.t") == [[0]]
