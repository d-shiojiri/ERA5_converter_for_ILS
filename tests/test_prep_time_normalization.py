from __future__ import annotations

import numpy as np
import xarray as xr

from era5_prep.normalize_time import validate_time_axis


def test_validate_time_axis_hourly_with_boundary():
    time = np.arange(
        np.datetime64("2001-01-01T00:00:00"),
        np.datetime64("2002-01-01T01:00:00"),
        np.timedelta64(1, "h"),
    )
    da = xr.DataArray(
        np.zeros((time.size, 1, 1), dtype=np.float32),
        dims=("time", "latitude", "longitude"),
        coords={"time": time, "latitude": [0.0], "longitude": [0.0]},
    )
    validate_time_axis(da, 2001)
