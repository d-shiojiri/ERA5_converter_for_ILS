from __future__ import annotations

from datetime import datetime
from pathlib import Path

import numpy as np
import xarray as xr
from tqdm import tqdm

from pf_random import (
    build_time_axis,
    generate_correlated_ar1_series,
    model_apply,
    reset_default_process,
)


def main() -> None:
    # Parameters follow README.md "Minimal iterative example".
    corr = np.array(
        [
            [1.0, -0.8, 0.5],
            [-0.8, 1.0, -0.5],
            [0.5, -0.5, 1.0],
        ],
        dtype=np.float64,
    )
    variables = ("Precip", "SWdown", "LWdown")
    std_norm = {"Precip": 0.50, "SWdown": 0.30, "LWdown": 50.0}
    variable_settings = {
        "Precip": {
            "model": "lognormal",
            "mode": "multiplicative",
            "bounds": None,
            "value_cap": 2.5,
        },
        "SWdown": {
            "model": "lognormal",
            "mode": "multiplicative",
            "bounds": None,
            "value_cap": 2.5,
        },
        "LWdown": {
            "model": "trunc-gauss",
            "mode": "additive",
            "bounds": (-2.5, 2.5),
            "value_cap": None,
        },
    }

    interval_seconds = 3600
    ar1_period_hours = 24.0
    seed = 100
    n_ens = 20
    n_vars = len(variables)
    n_steps_per_chunk = 144
    start_time = datetime(1900, 1, 1, 0, 0, 0)
    end_time = datetime(2026, 1, 1, 0, 0, 0)

    if end_time < start_time:
        raise ValueError("end_time must be later than or equal to start_time.")
    total_seconds = int((end_time - start_time).total_seconds())
    if total_seconds % interval_seconds != 0:
        raise ValueError("end_time must align with interval_seconds from start_time.")
    n_steps = total_seconds // interval_seconds + 1
    n_chunks = (n_steps + n_steps_per_chunk - 1) // n_steps_per_chunk

    reset_default_process(seed=seed)

    z_raw = np.empty((n_steps, n_ens, n_vars), dtype=np.float32)
    z_scaled = np.empty((n_steps, n_ens, n_vars), dtype=np.float32)
    factor_data = {v: np.empty((n_steps, n_ens), dtype=np.float32) for v in variables}
    std_arr = np.array([std_norm[v] for v in variables], dtype=np.float32)

    write_pos = 0
    for _ in tqdm(range(n_chunks)):
        chunk_steps = min(n_steps_per_chunk, n_steps - write_pos)
        z_chunk = generate_correlated_ar1_series(
            n_steps=chunk_steps,
            n_ens=n_ens,
            corr=corr,
            interval_seconds=interval_seconds,
            ar1_period_hours=ar1_period_hours,
            seed=seed,
        )
        z_chunk_f32 = z_chunk.astype(np.float32)
        z_raw[write_pos : write_pos + chunk_steps, :, :] = z_chunk_f32
        z_scaled[write_pos : write_pos + chunk_steps, :, :] = (
            z_chunk_f32 * std_arr[None, None, :]
        )

        for idx, var in enumerate(variables):
            cfg = variable_settings[var]
            z_scaled_chunk = z_chunk[:, :, idx] * std_norm[var]
            factor_chunk = model_apply(
                z=z_scaled_chunk,
                model=cfg["model"],
                mode=cfg["mode"],
                bounds=cfg["bounds"],
                value_cap=cfg["value_cap"],
                std_norm=std_norm[var],
            )
            factor_data[var][write_pos : write_pos + chunk_steps, :] = factor_chunk.astype(
                np.float32
            )

        write_pos += chunk_steps

    times = build_time_axis(start_time, interval_seconds, n_steps)

    ds = xr.Dataset(
        data_vars={
            "z_raw": (("time", "ensemble", "variable"), z_raw),
            "z_scaled": (("time", "ensemble", "variable"), z_scaled),
            "factor_Precip": (("time", "ensemble"), factor_data["Precip"]),
            "factor_SWdown": (("time", "ensemble"), factor_data["SWdown"]),
            "factor_LWdown": (("time", "ensemble"), factor_data["LWdown"]),
        },
        coords={
            "time": times,
            "ensemble": np.arange(1, n_ens + 1, dtype=np.int32),
            "variable": np.array(variables, dtype="<U10"),
        },
        attrs={
            "title": "Continuous AR(1) random sequence from 1900-01-01 00:00",
            "method": "pf_random Minimal iterative example parameters",
            "start_time": "1900-01-01 00:00:00",
            "end_time": "2026-01-01 00:00:00",
            "interval_seconds": interval_seconds,
            "ar1_period_hours": ar1_period_hours,
            "seed": seed,
            "n_chunks": n_chunks,
            "n_steps_per_chunk": n_steps_per_chunk,
            "n_steps": n_steps,
        },
    )

    out_path = Path(__file__).resolve().parent / "continuous_random_19000101_20260101.nc"
    encoding = {
        "z_raw": {"zlib": True, "complevel": 4},
        "z_scaled": {"zlib": True, "complevel": 4},
        "factor_Precip": {"zlib": True, "complevel": 4},
        "factor_SWdown": {"zlib": True, "complevel": 4},
        "factor_LWdown": {"zlib": True, "complevel": 4},
    }
    ds.to_netcdf(out_path, engine="netcdf4", encoding=encoding)
    print(f"Saved: {out_path}")
    print(
        "shape(z_raw)={}, time[0]={}, time[-1]={}".format(
            ds["z_raw"].shape,
            str(ds["time"].values[0]),
            str(ds["time"].values[-1]),
        )
    )


if __name__ == "__main__":
    main()
