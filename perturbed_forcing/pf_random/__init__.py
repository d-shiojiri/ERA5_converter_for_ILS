"""pf_random: reusable correlated Gaussian AR(1) random generator package."""

from .ar1_gaussian import (
    AR1GaussianConfig,
    CorrelatedAR1Gaussian,
    ar1_phi,
    build_time_axis,
    clear_default_process,
    generate_correlated_ar1_series,
    get_default_process,
    reset_default_process,
    reset_process,
    validate_correlation_matrix,
)
from .perturbation_models import (
    apply_clip_gauss,
    apply_lognormal,
    apply_trunc_gauss,
    clip_scaled_gaussian,
    model_apply,
    truncate_scaled_gaussian,
    validate_bounds,
    validate_clip_bounds,
)

__all__ = [
    "AR1GaussianConfig",
    "CorrelatedAR1Gaussian",
    "ar1_phi",
    "build_time_axis",
    "generate_correlated_ar1_series",
    "get_default_process",
    "reset_default_process",
    "reset_process",
    "clear_default_process",
    "validate_correlation_matrix",
    "apply_lognormal",
    "apply_trunc_gauss",
    "apply_clip_gauss",
    "model_apply",
    "validate_bounds",
    "truncate_scaled_gaussian",
    "validate_clip_bounds",
    "clip_scaled_gaussian",
]
