import math

import pytest

from dp.canonical import digest, encode_row, row_hash


def test_column_order_does_not_matter():
    assert encode_row({"a": 1, "b": "x"}) == encode_row({"b": "x", "a": 1})


def test_null_is_explicit_and_differs_from_empty_string():
    assert row_hash({"a": None}) != row_hash({"a": ""})


def test_negative_zero_and_nan_are_distinct_and_stable():
    h0, hneg, hnan = row_hash({"x": 0.0}), row_hash({"x": -0.0}), row_hash({"x": float("nan")})
    assert len({h0, hneg, hnan}) == 3
    assert hnan == row_hash({"x": float("nan")})
    assert hnan == row_hash({"x": math.copysign(float("nan"), -1.0)})  # one canonical NaN


def test_int_and_float_with_same_value_differ():
    assert row_hash({"x": 1}) != row_hash({"x": 1.0})


def test_map_keys_are_sorted_and_lists_keep_order():
    assert encode_row({"m": {"b": 1, "a": 2}}) == encode_row({"m": {"a": 2, "b": 1}})
    assert encode_row({"l": [1, 2]}) != encode_row({"l": [2, 1]})


def test_unknown_types_are_refused():
    with pytest.raises(TypeError):
        encode_row({"x": object()})


def test_golden_vector():
    # Frozen from the first passing run. Any change to the encoding must change this deliberately.
    assert (
        row_hash({"log_id": "0bae3b5e", "timestamp_ns": 315969538559783000, "v": -0.0, "n": None}) == GOLDEN
    )


def test_digest_is_order_independent_and_length_prefixed():
    assert digest([b"ab", b"c"]) == digest([b"c", b"ab"])
    assert digest([b"a", b"bc"]) != digest([b"ab", b"c"])


GOLDEN = "8e28ca6475dcf4fdc1c98bb546b0955563eeb9721893b6bd7a979521b836e5ad"
