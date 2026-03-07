from __future__ import annotations

import numpy as np
import xarray as xr

from era5_common.formulas import (
    compute_qair_from_d2m_sp,
    compute_rainf_from_precip_snowf,
    compute_wind_from_uv,
)


def test_compute_wind_from_uv():
    u = xr.DataArray(np.array([3.0], dtype=np.float32))
    v = xr.DataArray(np.array([4.0], dtype=np.float32))
    out = compute_wind_from_uv(u, v)
    assert float(out.values[0]) == 5.0


def test_compute_rainf_non_negative():
    p = xr.DataArray(np.array([1.0, 0.5], dtype=np.float32))
    s = xr.DataArray(np.array([0.2, 1.0], dtype=np.float32))
    out = compute_rainf_from_precip_snowf(p, s)
    assert np.allclose(out.values, np.array([0.8, 0.0], dtype=np.float32))


def test_compute_qair_non_negative():
    d2m = xr.DataArray(np.array([290.0], dtype=np.float32))
    sp = xr.DataArray(np.array([101325.0], dtype=np.float32))
    q = compute_qair_from_d2m_sp(d2m, sp)
    assert float(q.values[0]) >= 0.0
