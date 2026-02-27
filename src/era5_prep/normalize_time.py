"""Time-axis checks for Stage1 datasets."""

from __future__ import annotations

import numpy as np
import xarray as xr


def validate_time_axis(da: xr.DataArray, year: int) -> None:
    if "time" not in da.dims:
        raise ValueError("time dimension is missing")

    times = da.time.values
    if times.size < 2:
        raise ValueError("time coordinate is too short")

    expected_last = np.datetime64(f"{year + 1:04d}-01-01T00:00:00")
    if times[-1] != expected_last:
        raise ValueError(
            f"Boundary time mismatch: got {times[-1]!r}, expected {expected_last!r}"
        )

    diffs = np.diff(times.astype("datetime64[s]").astype("int64"))
    if diffs.min() != diffs.max() or int(diffs[0]) != 3600:
        raise ValueError("time step must be uniform 1 hour")
