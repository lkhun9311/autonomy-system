from collections import Counter

from dp.source.av2 import EXPECTED_SENSORS, source_keys


def _touch(p):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"x")


def _log(tmp_path, log="L1"):
    d = tmp_path / log
    _touch(d / "sensors" / "lidar" / "100.feather")
    _touch(d / "sensors" / "cameras" / "ring_front_center" / "100.jpg")
    _touch(d / "sensors" / "cameras" / "ring_rear_left" / "100.jpg")
    return d


def test_keys_are_per_sensor_and_timestamp(tmp_path):
    k = source_keys(_log(tmp_path))
    assert k == Counter(
        {("L1", "lidar", 100): 1, ("L1", "ring_front_center", 100): 1, ("L1", "ring_rear_left", 100): 1}
    )


def test_same_timestamp_on_two_cameras_is_two_keys(tmp_path):
    k = source_keys(_log(tmp_path))
    assert sum(1 for key in k if key[2] == 100) == 3


def test_missing_camera_folder_is_visible_as_absent_keys(tmp_path):
    d = _log(tmp_path)
    k = source_keys(d)
    assert "ring_side_left" not in {s for _, s, _ in k}
    assert "ring_side_left" in EXPECTED_SENSORS


def test_non_numeric_files_are_ignored_not_counted(tmp_path):
    d = _log(tmp_path)
    _touch(d / "sensors" / "lidar" / "README.txt")
    assert sum(source_keys(d).values()) == 3
