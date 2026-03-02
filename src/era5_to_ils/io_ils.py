"""ILS output writer for Program B."""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path

import numpy as np
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


def _write_time_block() -> int:
    raw = os.environ.get("ERA5_WRITE_TIME_BLOCK", "24").strip() or "24"
    try:
        return max(1, int(raw))
    except ValueError:
        return 24


def _compute_array(data, compute_kwargs: dict[str, object]):
    if hasattr(data, "compute"):
        return data.compute(**compute_kwargs)
    return np.asarray(data)


def _iter_time_starts(n_time: int, time_block: int, show_progress: bool):
    starts = range(0, n_time, time_block)
    if not show_progress:
        return starts
    try:
        from tqdm import tqdm

        return tqdm(starts, total=(n_time + time_block - 1) // time_block, desc="write", unit="blk")
    except Exception:
        return starts


def _write_ils_streaming_netcdf(
    ds: xr.Dataset,
    *,
    tmp_path: Path,
    target_var: str,
    encoding: dict[str, dict[str, object]],
    compute_kwargs: dict[str, object],
    time_block: int,
    show_progress: bool,
) -> None:
    from datetime import UTC, datetime
    from netCDF4 import Dataset, date2num

    lat_name = "lat"
    lon_name = "lon"
    time_name = "time"
    da = ds[target_var].transpose(time_name, lat_name, lon_name)

    lat_vals = np.asarray(ds[lat_name].values)
    lon_vals = np.asarray(ds[lon_name].values)
    time_vals = np.asarray(ds[time_name].values).astype("datetime64[s]")
    time_sec = time_vals.astype("int64")
    time_units = TIME_OUTPUT_UNITS
    time_calendar = TIME_OUTPUT_CALENDAR
    time_py = [datetime.fromtimestamp(int(s), tz=UTC).replace(tzinfo=None) for s in time_sec]
    time_num = np.asarray(date2num(time_py, units=time_units, calendar=time_calendar), dtype=np.float64)

    lat_size = int(lat_vals.size)
    lon_size = int(lon_vals.size)
    n_time = int(time_vals.size)

    var_enc = encoding[target_var]
    with Dataset(tmp_path, mode="w", format="NETCDF4") as nc:
        nc.createDimension(time_name, None)
        nc.createDimension(lat_name, lat_size)
        nc.createDimension(lon_name, lon_size)

        v_time = nc.createVariable(time_name, "f8", (time_name,))
        v_lat = nc.createVariable(lat_name, "f8", (lat_name,))
        v_lon = nc.createVariable(lon_name, "f8", (lon_name,))
        v_main = nc.createVariable(
            target_var,
            "f4",
            (time_name, lat_name, lon_name),
            zlib=bool(var_enc.get("zlib", True)),
            complevel=int(var_enc.get("complevel", 4)),
            shuffle=bool(var_enc.get("shuffle", True)),
            fill_value=var_enc.get("_FillValue", FILL_VALUE_F32),
            chunksizes=tuple(var_enc.get("chunksizes", (1, lat_size, lon_size))),
        )

        for k, v in ds.attrs.items():
            nc.setncattr(k, v)
        for k, v in ds[time_name].attrs.items():
            if k not in {"units", "calendar"}:
                v_time.setncattr(k, v)
        for k, v in ds[lat_name].attrs.items():
            v_lat.setncattr(k, v)
        for k, v in ds[lon_name].attrs.items():
            v_lon.setncattr(k, v)
        for k, v in da.attrs.items():
            if k != "_FillValue":
                v_main.setncattr(k, v)

        v_time.units = time_units
        v_time.calendar = time_calendar

        v_lat[:] = lat_vals
        v_lon[:] = lon_vals
        v_time[:] = time_num

        for start in _iter_time_starts(n_time, time_block, show_progress):
            end = min(start + time_block, n_time)
            arr = _compute_array(da.isel(time=slice(start, end)).data, compute_kwargs)
            v_main[start:end, :, :] = np.asarray(arr, dtype=np.float32)


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
    compute_kwargs = _dask_compute_kwargs()
    time_block = _write_time_block()

    try:
        if all(k in ds_write.coords for k in ("time", "lat", "lon")):
            _write_ils_streaming_netcdf(
                ds_write,
                tmp_path=tmp_path,
                target_var=target_var,
                encoding=encoding,
                compute_kwargs=compute_kwargs,
                time_block=time_block,
                show_progress=use_dask_progress,
            )
        else:
            delayed = ds_write.to_netcdf(
                tmp_path, format="NETCDF4", encoding=encoding, compute=False
            )
            with dask_progress(use_dask_progress):
                delayed.compute(**compute_kwargs)
        tmp_path.replace(out_path)
    except Exception:
        # Remove partial output so reruns are not blocked by broken files.
        if tmp_path.exists():
            tmp_path.unlink()
        raise

    return out_path
