import pytest

from dp.config import load
from dp.explore import camera_lidar_skew_ms

pytestmark = pytest.mark.integration
LOG = "0bae3b5e-417d-3b03-abaa-806b433233b8"
LIDAR_TS = 315969538559783000


def test_recomputes_the_published_skew_range():
    d = load().data_dir / "sensor" / "val" / LOG
    if not d.exists():
        pytest.skip("log not fetched")
    skew = camera_lidar_skew_ms(d, LIDAR_TS)
    assert len(skew) == 9
    assert round(min(skew.values()), 2) == -22.36
    assert round(max(skew.values()), 2) == 17.70
