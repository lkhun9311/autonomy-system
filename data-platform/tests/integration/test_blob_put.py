import hashlib
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from dp.blob import BUCKET, BlobStore, key_for, s3_client
from dp.config import load

pytestmark = pytest.mark.integration


def _tomorrow():
    return datetime.now(UTC) + timedelta(days=1)


@pytest.fixture(scope="module")
def s3():
    return s3_client(load())


def _fresh():
    data = f"put-probe {uuid.uuid4()}".encode()
    return data, hashlib.sha256(data).hexdigest()


def _versions(s3, key):
    r = s3.list_object_versions(Bucket=BUCKET, Prefix=key)
    return [v["VersionId"] for v in r.get("Versions", []) if v["Key"] == key]


def test_bytes_that_do_not_match_the_claimed_hash_are_refused_and_not_written(s3):
    data, sha = _fresh()
    with pytest.raises(ValueError, match=sha):
        BlobStore(s3).put(data + b"!", sha, retain_until=_tomorrow())
    assert _versions(s3, key_for(sha)) == []


def test_the_same_bytes_twice_reuse_one_version(s3):
    data, sha = _fresh()
    a = BlobStore(s3).put(data, sha, retain_until=_tomorrow())
    b = BlobStore(s3).put(data, sha, retain_until=_tomorrow())
    assert a == b
    assert _versions(s3, key_for(sha)) == [a.version_id]


def test_a_tampered_current_version_gets_a_new_version_written(s3):
    data, sha = _fresh()
    a = BlobStore(s3).put(data, sha, retain_until=_tomorrow())
    s3.put_object(
        Bucket=BUCKET,
        Key=key_for(sha),
        Body=b"x",
        ObjectLockMode="COMPLIANCE",
        ObjectLockRetainUntilDate=_tomorrow(),
    )
    b = BlobStore(s3).put(data, sha, retain_until=_tomorrow())
    assert b.version_id != a.version_id
    assert BlobStore(s3).get(b) == data


def test_a_delete_marker_gets_a_new_version_written(s3):
    data, sha = _fresh()
    a = BlobStore(s3).put(data, sha, retain_until=_tomorrow())
    s3.delete_object(Bucket=BUCKET, Key=key_for(sha))
    b = BlobStore(s3).put(data, sha, retain_until=_tomorrow())
    assert b.version_id != a.version_id
    assert BlobStore(s3).get(b) == data
