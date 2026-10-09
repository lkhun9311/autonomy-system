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
