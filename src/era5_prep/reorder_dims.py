"""Dimension-order helpers for Program A."""

from __future__ import annotations

import xarray as xr


def reorder_to_canonical(da: xr.DataArray) -> xr.DataArray:
    return da.transpose("time", "latitude", "longitude")
