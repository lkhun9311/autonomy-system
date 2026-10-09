"""Read every pinned blob version back and hash it (spec §8 · §11).

The key's current version is never consulted: a pin is valid when its own bytes match the row's checksum.
"""

import hashlib
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from botocore.exceptions import ClientError

from dp.blob import BlobRef, BlobStore


@dataclass(frozen=True)
class Failure:
    object: str
    version_id: str | None
    expected: str
    actual: str


@dataclass
class VerifyReport:
    checked: int = 0
    failures: list[Failure] = field(default_factory=list)


def _check(store: BlobStore, row: Mapping) -> Failure | None:
    uri, vid, expected = row["blob_uri"], row["blob_version_id"], row["checksum"]
    if not uri or not vid:
        return Failure(uri or "-", vid, expected, "no blob pinned")
    try:
        actual = hashlib.sha256(store.get(BlobRef(uri, vid))).hexdigest()
    except ClientError as e:
        return Failure(uri, vid, expected, f"version missing: {e.response['Error'].get('Code', '?')}")
    return None if actual == expected else Failure(uri, vid, expected, actual)


def verify_rows(store: BlobStore, rows: Iterable[Mapping]) -> VerifyReport:
    rows = list(rows)
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda r: _check(store, r), rows))
    return VerifyReport(checked=len(rows), failures=[f for f in results if f])


def pinned_rows(spark, table: str, log_id: str, commit_id: str) -> list[dict]:
    """Rows to verify. A table from before S2 has no blob columns; its rows read as unpinned (null)
    so they fail as "no blob pinned" instead of the query failing. Read-only: no migration here."""
    have = set(spark.table(table).columns)
    cols = [c if c in have else f"cast(null as string) as {c}" for c in ("blob_uri", "blob_version_id")]
    return [
        r.asDict()
        for r in spark.sql(
            f"select {', '.join(cols)}, checksum from {table} "
            f"where log_id = '{log_id}' and ingest_commit_id = '{commit_id}'"
        ).collect()
    ]
