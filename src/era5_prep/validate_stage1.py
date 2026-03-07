"""Validation routines for Stage1 outputs."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import xarray as xr

from era5_common.constants import TARGET_VARS
from era5_common.io import stage1_file_path


def validate_stage1_file(path: Path, year: int, target_var: str) -> list[str]:
    issues: list[str] = []
    if not path.exists():
        return [f"missing: {path}"]

    ds = xr.open_dataset(path, decode_cf=True)
    if target_var not in ds.data_vars:
        issues.append(f"{path}: variable {target_var} is missing")
        return issues

    da = ds[target_var]
    expected_dims = ("time", "latitude", "longitude")
    if da.dims != expected_dims:
        issues.append(f"{path}: dims mismatch {da.dims} != {expected_dims}")

    expected_last = np.datetime64(f"{year + 1:04d}-01-01T00:00:00")
    if da.time.values[-1] != expected_last:
        issues.append(
            f"{path}: last time {da.time.values[-1]!r}, expected {expected_last!r}"
        )

    times = da.time.values.astype("datetime64[s]").astype("int64")
    diffs = np.diff(times)
    if diffs.size and (diffs.min() != 3600 or diffs.max() != 3600):
        issues.append(f"{path}: time step is not hourly")

    expected_time_count = 8785 if is_leap_year(year) else 8761
    if da.sizes.get("time") != expected_time_count:
        issues.append(
            f"{path}: unexpected time size {da.sizes.get('time')} != {expected_time_count}"
        )

    return issues


def is_leap_year(year: int) -> bool:
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def validate_year(stage1_root: str, year: int, target_vars: list[str] | None = None) -> list[str]:
    vars_to_check = target_vars or list(TARGET_VARS)
    issues: list[str] = []
    for var in vars_to_check:
        issues.extend(
            validate_stage1_file(
                stage1_file_path(stage1_root=stage1_root, year=year, target_var=var),
                year,
                var,
            )
        )
    return issues
