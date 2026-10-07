"""One Iceberg catalog shared by Spark, PyIceberg and Trino: JDBC on Postgres, warehouse on MinIO."""

from pyiceberg.catalog.sql import SqlCatalog
from pyspark.sql import SparkSession

from dp.config import Settings

CATALOG_NAME = "dp"
ICEBERG_SPARK = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"
ICEBERG_AWS = "org.apache.iceberg:iceberg-aws-bundle:1.10.0"
PG_JDBC = "org.postgresql:postgresql:42.7.4"


def pyiceberg_catalog(s: Settings) -> SqlCatalog:
    return SqlCatalog(
        CATALOG_NAME,
        uri=s.catalog_uri,
        warehouse=s.warehouse,
        **{
            "s3.endpoint": s.s3_endpoint,
            "s3.access-key-id": s.s3_access_key,
            "s3.secret-access-key": s.s3_secret_key,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


def spark_session(s: Settings, app: str) -> SparkSession:
    jdbc = s.catalog_uri.replace("postgresql+psycopg://", "jdbc:postgresql://")
    user_pw, host_db = jdbc.split("//", 1)[1].split("@", 1)
    user, pw = user_pw.split(":", 1)
    c = f"spark.sql.catalog.{CATALOG_NAME}"
    return (
        SparkSession.builder.appName(app)
        .master("local[*]")
        .config("spark.jars.packages", f"{ICEBERG_SPARK},{ICEBERG_AWS},{PG_JDBC}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(c, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{c}.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config(f"{c}.uri", "jdbc:postgresql://" + host_db)
        .config(f"{c}.jdbc.user", user)
        .config(f"{c}.jdbc.password", pw)
        .config(f"{c}.jdbc.schema-version", "V1")
        .config(f"{c}.warehouse", s.warehouse)
        # Other processes commit to these tables; a cached table would hide their snapshots.
        .config(f"{c}.cache-enabled", "false")
        .config(f"{c}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{c}.s3.endpoint", s.s3_endpoint)
        .config(f"{c}.s3.path-style-access", "true")
        .config(f"{c}.client.region", "us-east-1")
        .config(f"{c}.s3.access-key-id", s.s3_access_key)
        .config(f"{c}.s3.secret-access-key", s.s3_secret_key)
        .getOrCreate()
    )
