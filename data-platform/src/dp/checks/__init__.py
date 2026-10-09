"""Sensor checks (spec §9). Bump a check's version when its values change meaning; bump the set version
whenever any check version changes, so a new check commit is written instead of reusing the old one."""

CHECK_VERSIONS = {
    "laser_returns": 1,
    "points_range": 1,
    "sweep_timing": 1,
    "camera_skew": 1,
    "pose_continuity": 1,
    "ground_residual": 1,
}
CHECK_SET_VERSION = "s3a-1"
