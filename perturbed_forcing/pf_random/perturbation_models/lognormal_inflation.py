from __future__ import annotations

import numpy as np


def apply_lognormal(
    z: np.ndarray,
    sigma: float,
    *,
    mode: str,
    value_cap: float,
) -> np.ndarray:
    key = mode.lower().replace("_", "-")
    if key != "multiplicative":
        raise ValueError("lognormal model supports only mode='multiplicative'.")
    mu = -0.5 * sigma * sigma
    out = np.exp(mu + z)
    return np.clip(out, None, value_cap)
