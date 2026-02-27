"""ILS output writer for Program B."""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path

import xarray as xr

from era5_common.constants import (
    DEFAULT_ATTRS,
    FILL_VALUE_F32,
    TIME_OUTPUT_CALENDAR,
    TIME_OUTPUT_UNITS,
)
from era5_common.io import output_file_path
from era5_common.progress import dask_progress


def _strip_fillvalue_attrs(ds: xr.Dataset) -> xr.Dataset:
    ds_out = ds.copy(deep=False)
    for name in ds_out.data_vars:
        ds_out[name].attrs.pop("_FillValue", None)
    return ds_out


def _dask_compute_kwargs() -> dict[str, object]:
    scheduler = os.environ.get("ERA5_DASK_SCHEDULER", "threads").strip() or "threads"
    workers_raw = os.environ.get("ERA5_DASK_NUM_WORKERS", "1").strip() or "1"
    try:
        workers = max(1, int(workers_raw))
    except ValueError:
        workers = 1

    if scheduler == "single-threaded":
        return {"scheduler": "single-threaded"}
    return {"scheduler": scheduler, "num_workers": workers}


def build_ils_dataset(target_var: str, da: xr.DataArray, year: int) -> xr.Dataset:
    var = da.transpose("time", "latitude", "longitude").rename(
        {"latitude": "lat", "longitude": "lon"}
    )
    ds = var.to_dataset(name=target_var)

    ds["time"].attrs.update(
        {
            "standard_name": "time",
            "long_name": "Time",
            "units": TIME_OUTPUT_UNITS,
            "calendar": TIME_OUTPUT_CALENDAR,
            "axis": "T",
        }
    )
    ds["lat"].attrs.update(
        {
            "standard_name": "latitude",
            "long_name": "Latitude",
            "units": "degrees_north",
            "axis": "Y",
        }
    )
    ds["lon"].attrs.update(
        {
            "standard_name": "longitude",
            "long_name": "Longitude",
            "units": "degrees_east",
            "axis": "X",
        }
    )

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ds.attrs.update(DEFAULT_ATTRS)
    ds.attrs["id"] = f"GSWP3.BC.{target_var}.1hrMap.{year}"
    ds.attrs["history"] = f"{now} ProgramB convert-var run for {target_var} {year}"
    return ds


def write_ils_dataset(
    ds: xr.Dataset,
    *,
    output_root: str,
    year: int,
    target_var: str,
    overwrite: bool,
    use_dask_progress: bool,
) -> Path:
    out_path = output_file_path(output_root=output_root, year=year, target_var=target_var)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and not overwrite:
        raise FileExistsError(f"Output already exists: {out_path}. Use --overwrite")

    lat_size = int(ds.sizes.get("lat", 1))
    lon_size = int(ds.sizes.get("lon", 1))

    encoding = {
        target_var: {
            "zlib": True,
            "complevel": 4,
            "shuffle": True,
            "dtype": "float32",
            "_FillValue": FILL_VALUE_F32,
            # Optimize downstream ILS usage (step-wise time access).
            "chunksizes": (1, lat_size, lon_size),
        },
        "time": {
            "dtype": "float64",
            "units": TIME_OUTPUT_UNITS,
            "calendar": TIME_OUTPUT_CALENDAR,
        },
    }

    ds_write = _strip_fillvalue_attrs(ds)
    tmp_path = out_path.with_name(f"{out_path.name}.tmp.{os.getpid()}")

    try:
        delayed = ds_write.to_netcdf(
            tmp_path, format="NETCDF4", encoding=encoding, compute=False
        )
        with dask_progress(use_dask_progress):
            delayed.compute(**_dask_compute_kwargs())
        tmp_path.replace(out_path)
    except Exception:
        # Remove partial output so reruns are not blocked by broken files.
        if tmp_path.exists():
            tmp_path.unlink()
        raise

    return out_path
