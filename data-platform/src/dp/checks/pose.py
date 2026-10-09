"""Ego-pose continuity (spec §9). av2-api looks a sweep's pose up by exact timestamp (semantics note)."""

from bisect import bisect_left

import numpy as np


def quat_to_rot(qw: float, qx: float, qy: float, qz: float) -> np.ndarray:
    n = np.sqrt(qw * qw + qx * qx + qy * qy + qz * qz)
    w, x, y, z = qw / n, qx / n, qy / n, qz / n
    return np.array(
        [
            [1 - 2 * (y * y + z * z), 2 * (x * y - z * w), 2 * (x * z + y * w)],
            [2 * (x * y + z * w), 1 - 2 * (x * x + z * z), 2 * (y * z - x * w)],
            [2 * (x * z - y * w), 2 * (y * z + x * w), 1 - 2 * (x * x + y * y)],
        ]
    )


def pose_continuity(pose_ts: np.ndarray, pose_xyz: np.ndarray, lidar_ts_ns: int) -> dict[str, float]:
    i = bisect_left(pose_ts.tolist(), lidar_ts_ns)
    has = i < len(pose_ts) and int(pose_ts[i]) == lidar_ts_ns
    if not has or i == 0:
        return {"has_pose_at_sweep": float(has), "gap_prev_ms": float("nan"), "speed_mps": float("nan")}
    dt = (int(pose_ts[i]) - int(pose_ts[i - 1])) / 1e9
    dist = float(np.linalg.norm(pose_xyz[i] - pose_xyz[i - 1]))
    return {"has_pose_at_sweep": 1.0, "gap_prev_ms": dt * 1e3, "speed_mps": dist / dt}
