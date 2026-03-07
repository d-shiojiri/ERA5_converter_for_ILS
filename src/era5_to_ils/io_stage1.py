"""Stage1 input loader for Program B."""

from __future__ import annotations

import xarray as xr

from era5_common.io import stage1_file_path


def load_stage1_variable(
    *,
    stage1_root: str,
    year: int,
    target_var: str,
    chunks_time: int = 168,
) -> xr.DataArray:
    path = stage1_file_path(stage1_root=stage1_root, year=year, target_var=target_var)
    if not path.exists():
        raise FileNotFoundError(f"Stage1 file not found: {path}")

    ds = xr.open_dataset(path, decode_cf=True, chunks={"time": chunks_time})
    if target_var not in ds.data_vars:
        raise KeyError(f"Variable {target_var} not found in {path}")

    da = ds[target_var]
    if da.dims != ("time", "latitude", "longitude"):
        da = da.transpose("time", "latitude", "longitude")
    return da
