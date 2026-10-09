import math

import numpy as np

from dp.checks.lidar import laser_returns, points_range, sweep_timing


def test_counts_every_laser_and_the_dead_ones():
    v = laser_returns(np.array([0, 0, 5, 63], dtype=np.uint8))
    assert v["laser_00"] == 2 and v["laser_05"] == 1 and v["laser_63"] == 1 and v["laser_01"] == 0
    assert v["lasers_zero"] == 61


def test_points_and_range_quartiles():
    xyz = np.array([[3.0, 4.0, 0.0], [0.0, 0.0, 1.0], [0.0, 0.0, 2.0], [0.0, 0.0, 3.0]])
    v = points_range(xyz)
    assert v["points"] == 4 and v["range_q2"] == 2.5


def test_an_empty_sweep_has_zero_points_and_nan_quartiles():
    v = points_range(np.zeros((0, 3)))
    assert v["points"] == 0 and math.isnan(v["range_q2"])


def test_timing_span_and_decreasing_pairs_per_laser():
    laser = np.array([0, 0, 0, 1, 1], dtype=np.uint8)
    offset = np.array([10, 20, 15, 5, 30], dtype=np.int32)
    v = sweep_timing(laser, offset)
    assert v["span_ns"] == 25 and v["decreasing_pair_ratio"] == 1 / 3


def test_interleaved_lasers_do_not_count_as_decreasing():
    laser = np.array([0, 1, 0, 1], dtype=np.uint8)
    offset = np.array([10, 5, 20, 6], dtype=np.int32)
    assert sweep_timing(laser, offset)["decreasing_pair_ratio"] == 0.0
