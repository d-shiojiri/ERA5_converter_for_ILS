"""Validation routines for Program B outputs."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import xarray as xr

from era5_common.constants import TARGET_METADATA, TARGET_VARS
from era5_common.io import output_file_path


def is_leap_year(year: int) -> bool:
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def validate_output_file(path: Path, year: int, target_var: str) -> list[str]:
    issues: list[str] = []
    if not path.exists():
        return [f"missing: {path}"]

    ds = xr.open_dataset(path, decode_cf=True)
    if target_var not in ds.data_vars:
        issues.append(f"{path}: missing variable {target_var}")
        return issues

    da = ds[target_var]
    if da.dims != ("time", "lat", "lon"):
        issues.append(f"{path}: dims mismatch {da.dims}")

    if da.sizes.get("lat") != 360 or da.sizes.get("lon") != 720:
        issues.append(
            f"{path}: shape mismatch lat/lon=({da.sizes.get('lat')}, {da.sizes.get('lon')})"
        )

    expected_time_count = 8785 if is_leap_year(year) else 8761
    if da.sizes.get("time") != expected_time_count:
        issues.append(
            f"{path}: time length mismatch {da.sizes.get('time')} != {expected_time_count}"
        )

    expected_last = np.datetime64(f"{year + 1:04d}-01-01T00:00:00")
    if ds["time"].values[-1] != expected_last:
        issues.append(
            f"{path}: last time mismatch {ds['time'].values[-1]!r} != {expected_last!r}"
        )

    expected_units = TARGET_METADATA[target_var]["units"]
    if da.attrs.get("units") != expected_units:
        issues.append(f"{path}: units mismatch {da.attrs.get('units')} != {expected_units}")

    return issues


def validate_year(output_root: str, year: int, target_vars: list[str] | None = None) -> list[str]:
    vars_to_check = target_vars or list(TARGET_VARS)
    issues: list[str] = []
    for var in vars_to_check:
        path = output_file_path(output_root=output_root, year=year, target_var=var)
        issues.extend(validate_output_file(path, year, var))
    return issues
