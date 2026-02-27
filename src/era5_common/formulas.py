"""Physical formula helpers used by Program A/B."""

from __future__ import annotations

import numpy as np
import xarray as xr


def compute_qair_from_d2m_sp(d2m: xr.DataArray, sp: xr.DataArray) -> xr.DataArray:
    """ECMWF Guideline 10 with IFS CY41R2 eq. 7.4/7.5."""
    d2m = d2m.astype("float32")
    sp = sp.astype("float32")

    eps = np.float32(0.621981)
    a1 = np.float32(611.21)
    a3 = np.float32(17.502)
    a4 = np.float32(32.19)
    t0 = np.float32(273.16)

    es = a1 * np.exp(a3 * (d2m - t0) / (d2m - a4))
    denom = sp - (np.float32(1.0) - eps) * es

    qair = xr.where(denom > np.float32(0.0), eps * es / denom, np.float32(np.nan))
    qair = xr.where(qair < np.float32(0.0), np.float32(0.0), qair)
    return qair.astype("float32")


def compute_wind_from_uv(u10: xr.DataArray, v10: xr.DataArray) -> xr.DataArray:
    u10 = u10.astype("float32")
    v10 = v10.astype("float32")
    wind = np.hypot(u10, v10)
    wind = xr.where(wind < np.float32(0.0), np.float32(0.0), wind)
    return wind.astype("float32")


def compute_rainf_from_precip_snowf(
    precip: xr.DataArray,
    snowf: xr.DataArray,
) -> xr.DataArray:
    precip = precip.astype("float32")
    snowf = snowf.astype("float32")
    rainf = xr.where(precip - snowf > np.float32(0.0), precip - snowf, np.float32(0.0))
    return rainf.astype("float32")
