"""Variable and unit transformation for Program B."""

from __future__ import annotations

import xarray as xr

from era5_common.constants import (
    SECONDS_PER_HOUR,
    STAGE1_UNITS,
    TARGET_METADATA,
    WATER_DENSITY,
)
from era5_common.io import infer_timestep_seconds


def convert_to_ils_units(target_var: str, da: xr.DataArray) -> xr.DataArray:
    out = da.astype("float32")
    dt = infer_timestep_seconds(out.time.values)

    if target_var in {"SWdown", "LWdown"}:
        # J m-2 (accumulated over dt) -> W m-2
        out = out / dt
    elif target_var in {"Precip", "Snowf", "Rainf"}:
        # m water equivalent (over dt) -> kg m-2 s-1
        out = (WATER_DENSITY * out) / dt
    elif target_var == "CCover":
        out = out.clip(0.0, 1.0)
    elif target_var == "Qair":
        out = out.clip(min=0.0)

    if dt != SECONDS_PER_HOUR and target_var in {"SWdown", "LWdown", "Precip", "Snowf", "Rainf"}:
        out.attrs["note"] = f"Converted using dt={dt} seconds inferred from input time axis"

    attrs = TARGET_METADATA[target_var].copy()
    attrs["source_stage1_units"] = STAGE1_UNITS[target_var]
    out.attrs.update(attrs)
    return out
