"""DDL for the canonical tables (spec §3.1). Kept as SQL so Trino and Spark read the same text."""

NS = "dp.av2"
DDL = {
    "log": f"""create table if not exists {NS}.log (
        log_id string, city string, start_ns bigint, end_ns bigint, source_path string,
        source_digest string, ingest_commit_id string) using iceberg""",
    "sensor_data": f"""create table if not exists {NS}.sensor_data (
        log_id string, sensor string, timestamp_ns bigint, source_path string, byte_size bigint,
        checksum string, codec string, calibration_id string, ingest_commit_id string,
        blob_uri string, blob_version_id string)
        using iceberg partitioned by (log_id)""",
    "pose": f"""create table if not exists {NS}.pose (
        log_id string, timestamp_ns bigint, qw double, qx double, qy double, qz double,
        tx_m double, ty_m double, tz_m double, ingest_commit_id string) using iceberg""",
    "calibration": f"""create table if not exists {NS}.calibration (
        log_id string, sensor string, calibration_id string, qw double, qx double, qy double, qz double,
        tx_m double, ty_m double, tz_m double, fx_px double, fy_px double, cx_px double, cy_px double,
        ingest_commit_id string) using iceberg""",
    "sample": f"""create table if not exists {NS}.sample (
        log_id string, lidar_ts_ns bigint, cam_frame_ts map<string, bigint>, cam_skew_ns map<string, bigint>,
        ingest_commit_id string) using iceberg partitioned by (log_id)""",
    "track": f"""create table if not exists {NS}.track (
        log_id string, timestamp_ns bigint, track_uuid string, category string,
        length_m double, width_m double, height_m double, qw double, qx double, qy double, qz double,
        tx_m double, ty_m double, tz_m double, num_interior_pts bigint, ingest_commit_id string)
        using iceberg partitioned by (log_id)""",
}

# Columns added after a table first shipped. ensure_tables adds them where missing; Iceberg appends
# them at the end, which is why the DDL above lists them last too.
ADDED_COLUMNS = {"sensor_data": [("blob_uri", "string"), ("blob_version_id", "string")]}
