from __future__ import annotations

from typing import Tuple

import numpy as np
from scipy.special import erf, erfinv

SQRT_2 = np.sqrt(2.0)


def validate_bounds(
    trunc_bounds: Tuple[float, float] | None,
    *,
    use_trunc_gauss: bool,
    default_low: float,
    default_high: float,
) -> Tuple[float, float] | None:
    if not use_trunc_gauss:
        return None
    if trunc_bounds is None:
        trunc_bounds = (default_low, default_high)
    low, high = trunc_bounds
    if low >= high:
        raise ValueError("truncation lower bound must be less than upper bound.")
    return (float(low), float(high))


def apply_trunc_gauss(
    z: np.ndarray,
    sigma: float,
    bounds: Tuple[float, float],
    *,
    mode: str,
) -> np.ndarray:
    truncated = truncate_scaled_gaussian(z, sigma, bounds)
    key = mode.lower().replace("_", "-")
    if key == "multiplicative":
        values = 1.0 + truncated
        mean = np.mean(values)
        if np.isclose(mean, 0.0):
            raise ValueError("Truncated multiplicative mean is zero; cannot normalize.")
        return values / mean
    if key == "additive":
        return truncated - truncated.mean()
    raise ValueError("mode must be 'multiplicative' or 'additive'.")


def truncate_scaled_gaussian(
    values: np.ndarray,
    sigma: float,
    bounds: Tuple[float, float],
) -> np.ndarray:
    if sigma == 0.0:
        return np.zeros_like(values)
    low, high = bounds
    if low >= high:
        raise ValueError("Invalid truncation bounds for truncated Gaussian.")

    normalized = values / sigma
    cdf_low = _standard_normal_cdf(low)
    cdf_high = _standard_normal_cdf(high)
    width = cdf_high - cdf_low
    if width <= 0.0:
        raise ValueError("Truncation bounds yield zero probability mass.")

    u = _standard_normal_cdf(normalized)
    truncated_prob = cdf_low + width * u
    truncated = _standard_normal_ppf(truncated_prob)
    return truncated * sigma


def _standard_normal_cdf(x: np.ndarray | float) -> np.ndarray:
    return 0.5 * (1.0 + erf(np.asarray(x) / SQRT_2))


def _standard_normal_ppf(p: np.ndarray) -> np.ndarray:
    clipped = np.clip(p, 1e-12, 1.0 - 1e-12)
    return SQRT_2 * erfinv(2.0 * clipped - 1.0)
