"""Runtime settings. Every value comes from the environment with a local-stack default."""
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    pg_dsn: str
    catalog_uri: str
    warehouse: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    trino_url: str


def load() -> Settings:
    e = os.environ.get
    return Settings(
        data_dir=Path(e("DP_DATA", str(Path.home() / "data" / "av2"))),
        pg_dsn=e("DP_PG_DSN", "postgresql://dp:dp@localhost:5432/dp"),
        catalog_uri=e("DP_CATALOG_URI", "postgresql+psycopg://dp:dp@localhost:5432/iceberg_catalog"),
        warehouse=e("DP_WAREHOUSE", "s3://warehouse/"),
        s3_endpoint=e("DP_S3_ENDPOINT", "http://localhost:9000"),
        s3_access_key=e("DP_S3_ACCESS_KEY", "dp"),
        s3_secret_key=e("DP_S3_SECRET_KEY", "dp-secret-key"),
        trino_url=e("DP_TRINO_URL", "http://localhost:8080"),
    )
