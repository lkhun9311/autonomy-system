"""Source-side keys for Argoverse 2 Sensor logs, read from the file layout directly.

This is the independent side of reconciliation (README measurement rules): it must not import the
normalisation code, so a bug there cannot make both sides agree.
"""

from collections import Counter
from collections.abc import Iterator
from pathlib import Path

SourceKey = tuple[str, str, int]
RING = (
    "ring_front_center",
    "ring_front_left",
    "ring_front_right",
    "ring_rear_left",
    "ring_rear_right",
    "ring_side_left",
    "ring_side_right",
)
STEREO = ("stereo_front_left", "stereo_front_right")
EXPECTED_SENSORS = frozenset((*RING, *STEREO, "lidar"))


def _stamps(folder: Path) -> Iterator[int]:
    if not folder.is_dir():
        return
    for p in folder.iterdir():
        if p.is_file() and p.stem.isdigit():
            yield int(p.stem)


def source_keys(log_dir: Path) -> Counter[SourceKey]:
    """(log_id, sensor, timestamp_ns) -> file count, straight from the directory tree."""
    log = log_dir.name
    keys: Counter[SourceKey] = Counter()
    for ts in _stamps(log_dir / "sensors" / "lidar"):
        keys[(log, "lidar", ts)] += 1
    cams = log_dir / "sensors" / "cameras"
    if cams.is_dir():
        for cam in cams.iterdir():
            for ts in _stamps(cam):
                keys[(log, cam.name, ts)] += 1
    return keys
