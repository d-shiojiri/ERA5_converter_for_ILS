"""Shared I/O helpers for ERA5 inputs and Stage outputs."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import numpy as np
import xarray as xr

from .constants import RAW_PARAMETER_TO_VAR


def find_era5_file(input_root: str | Path, year: int, parameter: str) -> Path:
    base = Path(input_root)
    name = f"reanalysis-era5-single-levels_{parameter}_{year}.nc"
    candidates = [
        base / name,
        base / str(year) / name,
    ]
    for cand in candidates:
        if cand.exists():
            return cand
    raise FileNotFoundError(
        f"ERA5 input file not found for parameter={parameter} year={year}: "
        f"checked {', '.join(str(p) for p in candidates)}"
    )


def open_raw_dataarray(
    input_root: str | Path,
    year: int,
    parameter: str,
    *,
    chunks_time: int = 168,
) -> xr.DataArray:
    path = find_era5_file(input_root, year, parameter)
    ds = xr.open_dataset(
        path,
        decode_cf=True,
        mask_and_scale=True,
    )
    ds = normalize_raw_dataset(ds)
    if chunks_time > 0:
        ds = ds.chunk({"time": chunks_time})
    var_name = RAW_PARAMETER_TO_VAR[parameter]
    if var_name not in ds.data_vars:
        available = ", ".join(ds.data_vars)
        raise KeyError(
            f"Variable '{var_name}' not found in {path}. Available: {available}"
        )
    da = ds[var_name]
    return da.transpose("time", "latitude", "longitude")


def normalize_raw_dataset(ds: xr.Dataset) -> xr.Dataset:
    rename_map = {}
    if "valid_time" in ds.dims or "valid_time" in ds.coords:
        rename_map["valid_time"] = "time"
    if "lat" in ds.dims or "lat" in ds.coords:
        rename_map["lat"] = "latitude"
    if "lon" in ds.dims or "lon" in ds.coords:
        rename_map["lon"] = "longitude"
    if rename_map:
        ds = ds.rename(rename_map)

    if "time" not in ds.coords:
        raise ValueError("Input dataset has no recognizable time coordinate")
    if "latitude" not in ds.coords or "longitude" not in ds.coords:
        raise ValueError("Input dataset must contain latitude/longitude coordinates")

    ds = ds.sortby("time")
    ds = normalize_longitude(ds)
    return ds


def normalize_longitude(ds: xr.Dataset) -> xr.Dataset:
    lon = ds["longitude"]
    if float(lon.min()) < 0.0:
        lon_new = (lon % 360.0).astype("float64")
        ds = ds.assign_coords(longitude=lon_new)
    return ds.sortby("longitude")


def with_year_boundary(da: xr.DataArray, boundary_da: xr.DataArray, year: int) -> xr.DataArray:
    start = np.datetime64(f"{year:04d}-01-01T00:00:00")
    boundary = np.datetime64(f"{year + 1:04d}-01-01T00:00:00")
    year_da = da.where((da.time >= start) & (da.time < boundary), drop=True)

    b = boundary_da.where(boundary_da.time == boundary, drop=True)
    if b.sizes.get("time", 0) == 0:
        first = boundary_da.isel(time=0)
        first_time = np.asarray([first.time.values])
        b = first.expand_dims(time=first_time)
    else:
        b = b.isel(time=0).expand_dims(time=[boundary])

    out = xr.concat([year_da, b], dim="time")
    return out


def load_parameter_with_boundary(
    input_root: str | Path,
    year: int,
    parameter: str,
    *,
    chunks_time: int = 168,
) -> xr.DataArray:
    current = open_raw_dataarray(
        input_root=input_root,
        year=year,
        parameter=parameter,
        chunks_time=chunks_time,
    )
    nxt = open_raw_dataarray(
        input_root=input_root,
        year=year + 1,
        parameter=parameter,
        chunks_time=chunks_time,
    )
    return with_year_boundary(current, nxt, year)


def ensure_coord_order(
    da: xr.DataArray,
    *,
    lat_order: str = "descending",
) -> xr.DataArray:
    out = da.transpose("time", "latitude", "longitude")
    out = out.sortby("longitude")

    if lat_order == "ascending":
        out = out.sortby("latitude", ascending=True)
    elif lat_order == "descending":
        out = out.sortby("latitude", ascending=False)
    else:
        raise ValueError(f"Unsupported lat_order: {lat_order}")

    return out


def infer_timestep_seconds(time_values: Iterable[np.datetime64]) -> float:
    arr = np.asarray(list(time_values))
    if arr.size < 2:
        return 3600.0
    seconds = arr.astype("datetime64[s]").astype(np.int64)
    diffs = np.diff(seconds)
    unique = np.unique(diffs)
    if unique.size != 1:
        raise ValueError(f"Non-uniform time step detected: {unique}")
    return float(unique[0])


def stage1_file_path(stage1_root: str | Path, year: int, target_var: str) -> Path:
    return Path(stage1_root) / str(year) / f"ERA5.STAGE1.{target_var}.{year}.nc"


def output_file_path(output_root: str | Path, year: int, target_var: str) -> Path:
    return Path(output_root) / str(year) / f"GSWP3.BC.{target_var}.1hrMap.ILS.{year}.nc"
