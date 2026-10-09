"""Lidar checks (spec §9). Values only; judging is the gate's job (S3b)."""

import numpy as np

LASERS = 64  # laser_number is in [0, 63] (av2-api sweep.py)


def laser_returns(laser_number: np.ndarray) -> dict[str, float]:
    counts = np.bincount(laser_number.astype(np.int64), minlength=LASERS)[:LASERS]
    out = {f"laser_{i:02d}": float(c) for i, c in enumerate(counts)}
    out["lasers_zero"] = float((counts == 0).sum())
    return out


def points_range(xyz: np.ndarray) -> dict[str, float]:
    if len(xyz) == 0:
        return {"points": 0.0, "range_q1": np.nan, "range_q2": np.nan, "range_q3": np.nan}
    r = np.linalg.norm(xyz.astype(np.float64), axis=1)
    q1, q2, q3 = np.quantile(r, [0.25, 0.5, 0.75])
    return {"points": float(len(r)), "range_q1": float(q1), "range_q2": float(q2), "range_q3": float(q3)}


def sweep_timing(laser_number: np.ndarray, offset_ns: np.ndarray) -> dict[str, float]:
    """Point order within a file is not documented as time order, so per-laser decreases are a value to
    record, not a rule (semantics note)."""
    off = offset_ns.astype(np.int64)
    pairs = decreasing = 0
    for laser in np.unique(laser_number):
        o = off[laser_number == laser]  # file order within the laser
        pairs += max(len(o) - 1, 0)
        decreasing += int((np.diff(o) < 0).sum())
    lo, hi = (float(off.min()), float(off.max())) if len(off) else (np.nan, np.nan)
    return {
        "offset_min_ns": lo,
        "offset_max_ns": hi,
        "span_ns": hi - lo,
        "decreasing_pair_ratio": decreasing / pairs if pairs else np.nan,
    }
