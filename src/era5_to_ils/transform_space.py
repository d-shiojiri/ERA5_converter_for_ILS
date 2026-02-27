"""Spatial transform utilities for Program B."""

from __future__ import annotations

import numpy as np
import xarray as xr


def target_lat_lon() -> tuple[np.ndarray, np.ndarray]:
    lat = np.arange(-89.75, 90.0, 0.5, dtype=np.float64)
    lon = np.arange(0.25, 360.0, 0.5, dtype=np.float64)
    return lat, lon


def _regrid_with_xesmf(da: xr.DataArray, method: str) -> xr.DataArray:
    import xesmf as xe

    lat_out, lon_out = target_lat_lon()

    src = da.rename({"latitude": "lat", "longitude": "lon"})
    src = src.sortby("lat", ascending=True).sortby("lon")

    out_grid = xr.Dataset(
        {
            "lat": ("lat", lat_out),
            "lon": ("lon", lon_out),
        }
    )

    xe_method = "conservative_normed" if method == "conservative" else "bilinear"
    regridder = xe.Regridder(src, out_grid, xe_method, periodic=True, reuse_weights=False)
    out = regridder(src)
    out = out.rename({"lat": "latitude", "lon": "longitude"})
    return out


def _regrid_with_interp(da: xr.DataArray) -> xr.DataArray:
    lat_out, lon_out = target_lat_lon()
    src = da.sortby("latitude", ascending=True).sortby("longitude")
    return src.interp(latitude=lat_out, longitude=lon_out, method="linear")


def _regrid_block_mean_2x2(da: xr.DataArray) -> xr.DataArray:
    lat_out, lon_out = target_lat_lon()
    src = da.sortby("latitude", ascending=True).sortby("longitude")

    # Fast fallback: 2x2 coarsening then coordinate overwrite to target grid.
    coarse = src.coarsen(latitude=2, longitude=2, boundary="trim").mean()

    # Align shape to expected target grid as closely as possible.
    coarse = coarse.isel(latitude=slice(0, min(coarse.sizes["latitude"], lat_out.size)))
    coarse = coarse.isel(longitude=slice(0, min(coarse.sizes["longitude"], lon_out.size)))

    if coarse.sizes["latitude"] != lat_out.size or coarse.sizes["longitude"] != lon_out.size:
        coarse = coarse.interp(latitude=lat_out, longitude=lon_out, method="linear")
    else:
        coarse = coarse.assign_coords(latitude=lat_out, longitude=lon_out)

    return coarse


def regrid_to_half_degree(da: xr.DataArray, method: str = "conservative") -> xr.DataArray:
    if method == "block_mean_2x2":
        return _regrid_block_mean_2x2(da)

    if method in {"conservative", "bilinear"}:
        try:
            return _regrid_with_xesmf(da, method)
        except Exception:
            return _regrid_with_interp(da)

    raise ValueError(f"Unsupported regridding method: {method}")
