"""Target-variable construction for Stage1 outputs."""

from __future__ import annotations

import xarray as xr

from era5_common.constants import STAGE1_UNITS, TARGET_METADATA
from era5_common.formulas import (
    compute_qair_from_d2m_sp,
    compute_rainf_from_precip_snowf,
    compute_wind_from_uv,
)


def build_stage1_variable(target_var: str, inputs: dict[str, xr.DataArray]) -> xr.DataArray:
    if target_var == "Tair":
        da = inputs["t2m"]
    elif target_var == "Qair":
        da = compute_qair_from_d2m_sp(inputs["d2m"], inputs["sp"])
    elif target_var == "PSurf":
        da = inputs["sp"]
    elif target_var == "Wind":
        da = compute_wind_from_uv(inputs["u10"], inputs["v10"])
    elif target_var == "SWdown":
        da = inputs["ssrd"]
    elif target_var == "LWdown":
        da = inputs["strd"]
    elif target_var == "Precip":
        da = inputs["tp"]
    elif target_var == "Rainf":
        da = compute_rainf_from_precip_snowf(inputs["tp"], inputs["sf"])
    elif target_var == "Snowf":
        da = inputs["sf"]
    elif target_var == "CCover":
        da = inputs["tcc"].clip(0.0, 1.0)
    else:
        raise ValueError(f"Unsupported target variable: {target_var}")

    da = da.astype("float32").rename(target_var)
    metadata = TARGET_METADATA[target_var].copy()
    metadata["units"] = STAGE1_UNITS[target_var]
    da.attrs.update(metadata)
    return da
