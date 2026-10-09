import math

import numpy as np

from dp.checks.ground import Sim2, ground_residual
from dp.checks.pose import pose_continuity, quat_to_rot
from dp.checks.sync import camera_skew

IDENTITY = Sim2(np.eye(2), np.zeros(2), 1.0)


def test_skew_and_neighbour_gaps_per_camera():
    v = camera_skew(1_000_000_000, {"a": [950_000_000, 1_010_000_000, 1_060_000_000], "b": []})
    assert v["skew_ms.a"] == 10.0 and v["gap_prev_ms.a"] == 60.0 and v["gap_next_ms.a"] == 50.0
    assert not any(k.endswith(".b") for k in v)


def test_identity_quaternion_and_a_quarter_turn():
    assert np.allclose(quat_to_rot(1, 0, 0, 0), np.eye(3))
    assert np.allclose(quat_to_rot(math.cos(math.pi / 4), 0, 0, math.sin(math.pi / 4)) @ [1, 0, 0], [0, 1, 0])


def test_pose_gap_and_speed():
    ts = np.array([0, 100_000_000, 200_000_000])
    xyz = np.array([[0.0, 0, 0], [1.0, 0, 0], [3.0, 0, 0]])
    v = pose_continuity(ts, xyz, 200_000_000)
    assert v == {"has_pose_at_sweep": 1.0, "gap_prev_ms": 100.0, "speed_mps": 20.0}


def test_a_sweep_without_a_pose_is_recorded_not_raised():
    v = pose_continuity(np.array([0, 100]), np.zeros((2, 3)), 50)
    assert v["has_pose_at_sweep"] == 0.0 and math.isnan(v["speed_mps"])


def test_ground_residual_excludes_points_outside_the_raster():
    raster = np.full((10, 10), 1.0)
    pts = np.array([[2.0, 2.0, 1.1], [3.0, 3.0, 0.8], [50.0, 50.0, 1.0], [4.0, 4.0, 5.0]])
    v = ground_residual(pts, np.eye(3), np.zeros(3), raster, IDENTITY)
    assert v["ground_points"] == 2 and v["median_residual_m"] == ((1.1 - 1) + (0.8 - 1)) / 2
    assert v["nan_fraction"] == 0.25


def test_no_pose_gives_nan_not_a_crash():
    v = ground_residual(np.zeros((3, 3)), None, None, np.zeros((2, 2)), IDENTITY)
    assert v["ground_points"] == 0 and math.isnan(v["median_residual_m"])
