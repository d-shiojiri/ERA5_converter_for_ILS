from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import numpy as np

_DEFAULT_PROCESS: "CorrelatedAR1Gaussian | None" = None
_DEFAULT_KEY: tuple | None = None


@dataclass(frozen=True)
class AR1GaussianConfig:
    corr: np.ndarray
    interval_seconds: int
    ar1_period_hours: float
    center_each_step: bool = True


class CorrelatedAR1Gaussian:
    """Stateful generator for correlated Gaussian AR(1) sequences."""

    def __init__(
        self,
        *,
        n_ens: int,
        config: AR1GaussianConfig,
        seed: Optional[int] = None,
    ) -> None:
        if n_ens <= 0:
            raise ValueError("n_ens must be positive.")
        self._n_ens = int(n_ens)
        self._corr = validate_correlation_matrix(config.corr)
        self._center_each_step = bool(config.center_each_step)
        self._phi = ar1_phi(config.interval_seconds, config.ar1_period_hours)
        self._noise_scale = np.sqrt(max(0.0, 1.0 - self._phi * self._phi))
        self._initial_seed = seed
        self._rng = np.random.default_rng(seed)
        self._chol = np.linalg.cholesky(self._corr).astype(np.float64)
        self._state: np.ndarray | None = None

    @property
    def n_vars(self) -> int:
        return self._corr.shape[0]

    def sample_step(self) -> np.ndarray:
        if self._state is None:
            state = _draw_correlated_normals(self._rng, self._n_ens, self._chol)
        else:
            noise = _draw_correlated_normals(self._rng, self._n_ens, self._chol)
            state = self._phi * self._state + self._noise_scale * noise

        if self._center_each_step:
            state = state - state.mean(axis=0, keepdims=True)

        self._state = state
        return state.copy()

    def sample_series(self, n_steps: int) -> np.ndarray:
        if n_steps <= 0:
            raise ValueError("n_steps must be positive.")
        out = np.empty((n_steps, self._n_ens, self.n_vars), dtype=np.float64)
        for step in range(n_steps):
            out[step] = self.sample_step()
        return out

    def reset(self, seed: Optional[int] = None) -> None:
        """Explicitly reset AR(1) state and RNG."""
        if seed is None:
            seed = self._initial_seed
        else:
            self._initial_seed = seed
        self._rng = np.random.default_rng(seed)
        self._state = None


def validate_correlation_matrix(
    corr: np.ndarray,
    *,
    expected_dim: Optional[int] = None,
) -> np.ndarray:
    arr = np.asarray(corr, dtype=np.float64)
    if arr.ndim != 2 or arr.shape[0] != arr.shape[1]:
        raise ValueError("Correlation matrix must be square.")
    if expected_dim is not None and arr.shape[0] != expected_dim:
        raise ValueError(
            f"Correlation matrix must be {expected_dim}x{expected_dim}, got {arr.shape}."
        )
    if not np.allclose(arr, arr.T, atol=1e-10):
        raise ValueError("Correlation matrix must be symmetric.")
    eigvals = np.linalg.eigvalsh(arr)
    if np.any(eigvals <= 0):
        raise ValueError("Correlation matrix must be positive definite.")
    return arr


def ar1_phi(interval_seconds: int, period_hours: float) -> float:
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive.")
    if period_hours <= 0:
        raise ValueError("period_hours must be positive.")
    dt_hours = interval_seconds / 3600.0
    return float(np.exp(-dt_hours / period_hours))


def build_time_axis(start: datetime, interval_seconds: int, n_steps: int) -> np.ndarray:
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive.")
    if n_steps <= 0:
        raise ValueError("n_steps must be positive.")
    base = np.datetime64(start, "s")
    step = np.timedelta64(interval_seconds, "s")
    return base + np.arange(n_steps, dtype=np.int64) * step


def generate_correlated_ar1_series(
    *,
    n_steps: int,
    n_ens: int,
    corr: np.ndarray,
    interval_seconds: int,
    ar1_period_hours: float,
    seed: Optional[int] = None,
    center_each_step: bool = True,
    process: CorrelatedAR1Gaussian | None = None,
) -> np.ndarray:
    if process is None:
        process = get_default_process(
            n_ens=n_ens,
            corr=corr,
            interval_seconds=interval_seconds,
            ar1_period_hours=ar1_period_hours,
            seed=seed,
            center_each_step=center_each_step,
        )
    return process.sample_series(n_steps)


def get_default_process(
    *,
    n_ens: int,
    corr: np.ndarray,
    interval_seconds: int,
    ar1_period_hours: float,
    seed: Optional[int] = None,
    center_each_step: bool = True,
) -> CorrelatedAR1Gaussian:
    """
    Return a cached process for continuous generation across repeated calls.
    A new process is created only when the configuration key changes.
    """
    global _DEFAULT_PROCESS, _DEFAULT_KEY
    key = _build_process_key(
        n_ens=n_ens,
        corr=corr,
        interval_seconds=interval_seconds,
        ar1_period_hours=ar1_period_hours,
        seed=seed,
        center_each_step=center_each_step,
    )
    if _DEFAULT_PROCESS is None or _DEFAULT_KEY != key:
        _DEFAULT_PROCESS = CorrelatedAR1Gaussian(
            n_ens=n_ens,
            seed=seed,
            config=AR1GaussianConfig(
                corr=np.asarray(corr, dtype=np.float64),
                interval_seconds=interval_seconds,
                ar1_period_hours=ar1_period_hours,
                center_each_step=center_each_step,
            ),
        )
        _DEFAULT_KEY = key
    return _DEFAULT_PROCESS


def reset_default_process(seed: Optional[int] = None) -> None:
    """
    Explicit reset for cached default process.
    If no process exists yet, this is a no-op.
    """
    if _DEFAULT_PROCESS is None:
        return
    _DEFAULT_PROCESS.reset(seed=seed)


def reset_process(process: CorrelatedAR1Gaussian, seed: Optional[int] = None) -> None:
    """Explicit reset for a user-managed process."""
    process.reset(seed=seed)


def clear_default_process() -> None:
    """Drop cached default process completely."""
    global _DEFAULT_PROCESS, _DEFAULT_KEY
    _DEFAULT_PROCESS = None
    _DEFAULT_KEY = None


def _build_process_key(
    *,
    n_ens: int,
    corr: np.ndarray,
    interval_seconds: int,
    ar1_period_hours: float,
    seed: Optional[int],
    center_each_step: bool,
) -> tuple:
    arr = validate_correlation_matrix(np.asarray(corr, dtype=np.float64))
    return (
        int(n_ens),
        arr.shape,
        arr.tobytes(),
        int(interval_seconds),
        float(ar1_period_hours),
        bool(center_each_step),
        seed,
    )


def _draw_correlated_normals(
    rng: np.random.Generator,
    n_ens: int,
    chol: np.ndarray,
) -> np.ndarray:
    raw = rng.standard_normal(size=(n_ens, chol.shape[0]))
    return raw @ chol.T
