from __future__ import annotations

from typing import Tuple

import numpy as np

from .clipped_gauss_inflation import apply_clip_gauss
from .lognormal_inflation import apply_lognormal
from .truncated_gauss_inflation import apply_trunc_gauss


def model_apply(
    *,
    z: np.ndarray,
    model: str,
    mode: str,
    bounds: Tuple[float, float] | None,
    value_cap: float | None,
    std_norm: float,
) -> np.ndarray:
    """
    Apply perturbation transform selected by model/mode and return factors.

    Parameters
    ----------
    z:
        Scaled Gaussian perturbation array for one variable.
    model:
        One of: "lognormal", "trunc-gauss", "clip-gauss".
    mode:
        One of: "multiplicative", "additive".
    bounds:
        Bounds in sigma units for trunc-gauss / clip-gauss. Ignored for lognormal.
    value_cap:
        Upper cap used by lognormal model. Set None for non-lognormal models.
    std_norm:
        Standard deviation scale (sigma) for the variable.
    """
    model_key = model.lower().replace("_", "-")
    mode_key = mode.lower().replace("_", "-")
    sigma = float(std_norm)

    if model_key == "lognormal":
        if value_cap is None:
            raise ValueError("value_cap is required for model='lognormal'.")
        return apply_lognormal(
            z,
            sigma,
            mode=mode_key,
            value_cap=float(value_cap),
        )

    if model_key == "trunc-gauss":
        if bounds is None:
            raise ValueError("bounds is required for model='trunc-gauss'.")
        return apply_trunc_gauss(
            z,
            sigma,
            bounds=bounds,
            mode=mode_key,
        )

    if model_key == "clip-gauss":
        if bounds is None:
            raise ValueError("bounds is required for model='clip-gauss'.")
        return apply_clip_gauss(
            z,
            sigma,
            bounds=bounds,
            mode=mode_key,
        )

    raise ValueError(
        f"Unsupported model '{model}'. Use 'lognormal', 'trunc-gauss', or 'clip-gauss'."
    )
