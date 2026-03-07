from __future__ import annotations

import numpy as np
import xarray as xr

from era5_common.formulas import compute_qair_from_d2m_sp


def test_qair_reasonable_range():
    d2m = xr.DataArray(np.array([273.15, 293.15], dtype=np.float64))
    sp = xr.DataArray(np.array([100000.0, 100000.0], dtype=np.float64))
    q = compute_qair_from_d2m_sp(d2m, sp)
    assert np.all(q.values >= 0.0)
    assert np.all(q.values <= 0.1)
