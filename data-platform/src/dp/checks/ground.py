"""Ground-height raster lookup, as av2-api map_api.py does it (semantics note, av2-api b7321d1f71f6)."""

import json
from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class Sim2:
    R: np.ndarray
    t: np.ndarray
    s: float

    @classmethod
    def from_json(cls, text: str) -> "Sim2":
        d = json.loads(text)
        return cls(
            np.array(d["R"], dtype=float).reshape(2, 2),
            np.array(d["t"], dtype=float).reshape(2),
            float(d["s"]),
        )

    def to_img(self, xy: np.ndarray) -> np.ndarray:
        return self.s * (xy @ self.R.T + self.t)


def raster_height(raster: np.ndarray, sim2: Sim2, xy: np.ndarray) -> np.ndarray:
    img = sim2.to_img(xy[:, :2]).astype(np.int64)  # truncate, as av2-api does; never round
    out = np.full(len(xy), np.nan)
    ok = (img[:, 1] >= 0) & (img[:, 1] < raster.shape[0]) & (img[:, 0] >= 0) & (img[:, 0] < raster.shape[1])
    out[ok] = raster[img[ok, 1], img[ok, 0]]
    return out


GROUND_HEIGHT_THRESHOLD_M = 0.3  # av2-api map_api.py


def ground_residual(
    points_ego: np.ndarray,
    R_city_ego: np.ndarray | None,
    t_city_ego: np.ndarray | None,
    raster: np.ndarray,
    sim2: Sim2,
) -> dict[str, float]:
    """Median z − h of ground points in the city frame. Points outside the raster have no height; they
    are excluded and counted, not allowed to turn the median into NaN."""
    if R_city_ego is None or len(points_ego) == 0:
        return {"ground_points": 0.0, "median_residual_m": float("nan"), "nan_fraction": float("nan")}
    city = points_ego.astype(np.float64) @ R_city_ego.T + t_city_ego
    h = raster_height(raster, sim2, city[:, :2])
    known = ~np.isnan(h)
    z = city[:, 2]
    ground = known & ((np.abs(z - h) <= GROUND_HEIGHT_THRESHOLD_M) | (z < h))
    res = z[ground] - h[ground]
    return {
        "ground_points": float(ground.sum()),
        "median_residual_m": float(np.median(res)) if len(res) else float("nan"),
        "nan_fraction": float((~known).mean()),
    }
