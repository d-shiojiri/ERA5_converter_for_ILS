from __future__ import annotations

from typing import Tuple

import numpy as np


def validate_clip_bounds(
    clip_bounds: Tuple[float, float] | None,
    *,
    use_clip_gauss: bool,
    default_low: float,
    default_high: float,
) -> Tuple[float, float] | None:
    if not use_clip_gauss:
        return None
    if clip_bounds is None:
        clip_bounds = (default_low, default_high)
    low, high = clip_bounds
    if low >= high:
        raise ValueError("clip lower bound must be less than upper bound.")
    return (float(low), float(high))


def apply_clip_gauss(
    z: np.ndarray,
    sigma: float,
    bounds: Tuple[float, float],
    *,
    mode: str,
) -> np.ndarray:
    clipped = clip_scaled_gaussian(z, sigma, bounds)
    key = mode.lower().replace("_", "-")
    if key == "multiplicative":
        values = 1.0 + clipped
        mean = np.mean(values)
        if np.isclose(mean, 0.0):
            raise ValueError("Clipped multiplicative mean is zero; cannot normalize.")
        return values / mean
    if key == "additive":
        return clipped - clipped.mean()
    raise ValueError("mode must be 'multiplicative' or 'additive'.")


def clip_scaled_gaussian(
    values: np.ndarray,
    sigma: float,
    bounds: Tuple[float, float],
) -> np.ndarray:
    if sigma == 0.0:
        return np.zeros_like(values)
    low, high = bounds
    if low >= high:
        raise ValueError("Invalid clipping bounds for Gaussian clipping.")
    normalized = values / sigma
    clipped = np.clip(normalized, low, high)
    return clipped * sigma
