"""A sample is one lidar sweep plus, per camera, the nearest frame (spec §3.1).

Ties pick the earlier frame, so the choice is deterministic.
"""

from bisect import bisect_left


def nearest_frames(lidar_ts_ns: int, frames: dict[str, list[int]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for cam, ts in frames.items():
        if not ts:
            continue
        i = bisect_left(ts, lidar_ts_ns)
        candidates = ts[max(i - 1, 0) : i + 1]
        out[cam] = min(candidates, key=lambda t: (abs(t - lidar_ts_ns), t))
    return out
