import pytest
import trino

from dp.catalog import pyiceberg_catalog, spark_session
from dp.config import load

pytestmark = pytest.mark.integration
NS, TBL = "compat", "probe"


def test_spark_write_pyiceberg_tag_trino_time_travel():
    s = load()
    spark = spark_session(s, "compat")
    spark.sql(f"create namespace if not exists dp.{NS}")
    spark.sql(f"drop table if exists dp.{NS}.{TBL}")
    spark.sql(f"create table dp.{NS}.{TBL} (k bigint, v string) using iceberg")
    spark.sql(f"insert into dp.{NS}.{TBL} values (1, 'a')")
    spark.sql(f"insert into dp.{NS}.{TBL} values (2, 'b')")

    cat = pyiceberg_catalog(s)
    t = cat.load_table(f"{NS}.{TBL}")
    snaps = [x.snapshot_id for x in t.snapshots()]
    assert len(snaps) == 2
    first = snaps[0]
    t.manage_snapshots().create_tag(first, "release/probe").commit()
    assert cat.load_table(f"{NS}.{TBL}").metadata.refs["release/probe"].snapshot_id == first

    conn = trino.dbapi.connect(host="localhost", port=8080, user="dp", catalog="iceberg", schema=NS)
    cur = conn.cursor()
    cur.execute(f"select k from {TBL} for version as of {first} order by k")
    assert [r[0] for r in cur.fetchall()] == [1]
    cur.execute(f"select k from {TBL} order by k")
    assert [r[0] for r in cur.fetchall()] == [1, 2]
