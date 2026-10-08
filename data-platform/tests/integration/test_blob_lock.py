import hashlib
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from botocore.exceptions import ClientError

from dp.blob import BUCKET, BlobRef, BlobStore, key_for, s3_client
from dp.config import load

pytestmark = pytest.mark.integration


def _tomorrow() -> datetime:
    return datetime.now(UTC) + timedelta(days=1)


@pytest.fixture(scope="module")
def s3():
    return s3_client(load())


@pytest.fixture
def pinned(s3):
    data = f"lock-probe {uuid.uuid4()}".encode()
    sha = hashlib.sha256(data).hexdigest()
    return data, BlobStore(s3).put(data, sha, retain_until=_tomorrow())


def _key(ref: BlobRef) -> str:
    return ref.uri.removeprefix(f"s3://{BUCKET}/")


def test_key_is_named_by_content():
    sha = "ab" + "0" * 62
    assert key_for(sha) == f"sha256/ab/{sha}"


def test_overwrite_adds_a_version_and_the_pin_still_reads_the_original(s3, pinned):
    data, ref = pinned
    s3.put_object(
        Bucket=BUCKET,
        Key=_key(ref),
        Body=b"someone else's bytes",
        ObjectLockMode="COMPLIANCE",
        ObjectLockRetainUntilDate=_tomorrow(),
    )
    assert s3.get_object(Bucket=BUCKET, Key=_key(ref))["Body"].read() == b"someone else's bytes"
    assert BlobStore(s3).get(ref) == data


def test_delete_marker_hides_the_key_but_not_the_pinned_version(s3, pinned):
    data, ref = pinned
    r = s3.delete_object(Bucket=BUCKET, Key=_key(ref))
    assert r.get("DeleteMarker") is True
    with pytest.raises(ClientError):
        s3.get_object(Bucket=BUCKET, Key=_key(ref))
    assert BlobStore(s3).get(ref) == data


def _is_lock_refusal(err: dict) -> bool:
    # Spec §11 as amended 2026-10-09: MinIO refuses with 400 InvalidRequest ("WORM protected"),
    # AWS S3 with 403 AccessDenied. Any other error is not the lock speaking.
    status, code = err["ResponseMetadata"]["HTTPStatusCode"], err["Error"].get("Code")
    return (status, code) == (403, "AccessDenied") or (
        (status, code) == (400, "InvalidRequest") and "WORM" in err["Error"].get("Message", "")
    )


@pytest.mark.parametrize("bypass", [False, True])
def test_deleting_a_pinned_version_is_refused_by_the_lock_and_the_bytes_stay(s3, pinned, bypass):
    data, ref = pinned
    kw = {"BypassGovernanceRetention": True} if bypass else {}
    with pytest.raises(ClientError) as e:
        s3.delete_object(Bucket=BUCKET, Key=_key(ref), VersionId=ref.version_id, **kw)
    assert _is_lock_refusal(e.value.response), e.value.response["Error"]
    assert BlobStore(s3).get(ref) == data


def test_get_refuses_a_version_that_does_not_exist(s3, pinned):
    _, ref = pinned
    with pytest.raises(ClientError):
        BlobStore(s3).get(BlobRef(ref.uri, "00000000-0000-0000-0000-000000000000"))
