"""Cross-sensor timing (spec §9). Neighbour gaps carry the frame-continuity signal: a dropped frame
doubles a gap; a whole-period shift of one camera does not show in skew (spec §9 limitation)."""

from bisect import bisect_left

from dp.sample import nearest_frames


def camera_skew(lidar_ts_ns: int, frames: dict[str, list[int]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for cam, ft in nearest_frames(lidar_ts_ns, frames).items():
        ts = frames[cam]
        i = bisect_left(ts, ft)
        out[f"skew_ms.{cam}"] = (ft - lidar_ts_ns) / 1e6
        out[f"gap_prev_ms.{cam}"] = (ft - ts[i - 1]) / 1e6 if i > 0 else float("nan")
        out[f"gap_next_ms.{cam}"] = (ts[i + 1] - ft) / 1e6 if i + 1 < len(ts) else float("nan")
    return out
