"""Dataset exploration that the blog and spec cite. Not a platform measurement."""

from pathlib import Path


def _timestamps(folder: Path) -> list[int]:
    return sorted(int(p.stem) for p in folder.iterdir() if p.stem.isdigit())


def camera_lidar_skew_ms(log_dir: Path, lidar_ts_ns: int) -> dict[str, float]:
    """Camera name -> signed ms from the lidar sweep to that camera's nearest frame."""
    cams = log_dir / "sensors" / "cameras"
    out: dict[str, float] = {}
    for cam in sorted(p for p in cams.iterdir() if p.is_dir()):
        ts = _timestamps(cam)
        nearest = min(ts, key=lambda t: abs(t - lidar_ts_ns))
        out[cam.name] = (nearest - lidar_ts_ns) / 1e6
    return out
