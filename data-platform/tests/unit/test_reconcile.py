from collections import Counter

import pytest

from dp.reconcile import compare

A = ("L", "lidar", 1)
B = ("L", "lidar", 2)
C = ("L", "ring_front_center", 1)


def test_equal_multisets_are_complete():
    r = compare(Counter({A: 1, B: 1}), Counter({A: 1, B: 1}))
    assert r.completeness == 1.0 and not r.missing and not r.extra and not r.duplicate


def test_one_missing_and_one_extra_do_not_cancel():
    r = compare(Counter({A: 1, B: 1}), Counter({A: 1, C: 1}))
    assert r.missing == Counter({B: 1}) and r.extra == Counter({C: 1})
    assert r.completeness == 0.5


def test_duplicates_are_counted_separately():
    r = compare(Counter({A: 1}), Counter({A: 2}))
    assert r.duplicate == Counter({A: 1}) and r.completeness == 1.0 and not r.extra


def test_empty_source_is_an_error_not_100_percent():
    with pytest.raises(ValueError):
        compare(Counter(), Counter())
