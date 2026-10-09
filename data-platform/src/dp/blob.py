"""Content-addressed blob store on MinIO (spec §2, §3.1, §8).

Keys are named by the sha256 of their bytes. The bucket has versioning and object lock, so a key can
gain new versions or a delete marker, but a written version cannot be removed or changed. Rows pin
(uri, version_id); readers read that version, never the key's current one.
"""

import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from dp.config import Settings

BUCKET = "blobs"


@dataclass(frozen=True)
class BlobRef:
    uri: str
    version_id: str


def key_for(sha256_hex: str) -> str:
    return f"sha256/{sha256_hex[:2]}/{sha256_hex}"


def s3_client(s: Settings):
    return boto3.client(
        "s3",
        endpoint_url=s.s3_endpoint,
        aws_access_key_id=s.s3_access_key,
        aws_secret_access_key=s.s3_secret_key,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


class BlobStore:
    def __init__(self, s3, bucket: str = BUCKET) -> None:
        self.s3 = s3
        self.bucket = bucket

    def _key(self, ref: BlobRef) -> str:
        prefix = f"s3://{self.bucket}/"
        if not ref.uri.startswith(prefix):
            raise ValueError(f"{ref.uri} is not in bucket {self.bucket}")
        return ref.uri.removeprefix(prefix)

    def _current(self, key: str) -> tuple[str, str] | None:
        """(version_id, recorded sha256) of the key's current version; None for no key or a delete marker."""
        try:
            h = self.s3.head_object(Bucket=self.bucket, Key=key)
        except ClientError as e:
            if e.response["ResponseMetadata"]["HTTPStatusCode"] in (404, 405):
                return None
            raise
        return h["VersionId"], h.get("Metadata", {}).get("sha256", "")

    def put(self, data: bytes | Path, sha256_hex: str, retain_until: datetime | None = None) -> BlobRef:
        """Store bytes under their own hash. Refuses bytes that do not match it. Reuses the key's current
        version when its recorded sha256 matches; the bytes are not re-read, so a forged version with
        matching metadata is reused here and caught by verification (dp.verify)."""
        body = data.read_bytes() if isinstance(data, Path) else data
        actual = hashlib.sha256(body).hexdigest()
        if actual != sha256_hex:
            raise ValueError(f"refusing to store bytes as {sha256_hex}: they hash to {actual}")
        key = key_for(sha256_hex)
        current = self._current(key)
        if current and current[1] == sha256_hex:
            return BlobRef(f"s3://{self.bucket}/{key}", current[0])
        lock = (
            {"ObjectLockMode": "COMPLIANCE", "ObjectLockRetainUntilDate": retain_until}
            if retain_until
            else {}
        )
        r = self.s3.put_object(
            Bucket=self.bucket, Key=key, Body=body, Metadata={"sha256": sha256_hex}, **lock
        )
        return BlobRef(f"s3://{self.bucket}/{key}", r["VersionId"])

    def get(self, ref: BlobRef) -> bytes:
        return self.s3.get_object(Bucket=self.bucket, Key=self._key(ref), VersionId=ref.version_id)[
            "Body"
        ].read()
