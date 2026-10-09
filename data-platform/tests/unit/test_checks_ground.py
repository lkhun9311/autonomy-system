import math

import numpy as np

from dp.checks.ground import Sim2, raster_height


def _sim2():
    return Sim2.from_json('{"R": [1.0, 0.0, 0.0, 1.0], "t": [-100.0, -200.0], "s": 3.3333333333333335}')


def test_city_to_image_is_s_times_r_p_plus_t():
    xy = np.array([[101.0, 202.0]])
    assert np.allclose(_sim2().to_img(xy), [[3.3333333, 6.6666667]])


def test_lookup_truncates_and_indexes_row_y_column_x():
    raster = np.arange(12, dtype=float).reshape(3, 4)  # 3 rows (y) x 4 columns (x)
    xy = np.array([[100.0 + 0.95 * 0.3 * 3, 200.0 + 0.3 * 2 + 0.01]])  # img x = 2.85 -> 2, img y ~ 2.03 -> 2
    assert raster_height(raster, _sim2(), xy).tolist() == [raster[2, 2]]


def test_outside_the_raster_is_nan():
    raster = np.zeros((3, 4))
    out = raster_height(raster, _sim2(), np.array([[0.0, 0.0], [100.0, 200.0]]))
    assert math.isnan(out[0]) and out[1] == 0.0
