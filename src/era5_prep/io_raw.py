"""Raw ERA5 input loading for Program A."""

from __future__ import annotations

from typing import Dict

import xarray as xr

from era5_common.constants import RAW_PARAMETER_TO_VAR, TARGET_TO_INPUT_PARAMETERS, TARGET_VARS
from era5_common.io import ensure_coord_order, load_parameter_with_boundary


def resolve_target_var(*, parameter: str | None = None, target_var: str | None = None) -> str:
    if parameter and target_var:
        raise ValueError("Specify only one of --parameter or --target-var")

    if target_var:
        if target_var not in TARGET_VARS:
            raise ValueError(f"Unsupported target variable: {target_var}")
        return target_var

    if parameter is None:
        raise ValueError("Either --parameter or --target-var must be provided")

    if parameter == "2m_temperature":
        return "Tair"
    if parameter == "surface_pressure":
        return "PSurf"
    if parameter == "total_cloud_cover":
        return "CCover"
    if parameter == "surface_solar_radiation_downwards":
        return "SWdown"
    if parameter == "surface_thermal_radiation_downwards":
        return "LWdown"
    if parameter == "total_precipitation":
        return "Precip"
    if parameter == "snowfall":
        return "Snowf"

    # Raw vars used only as inputs for derived targets are not directly emitted.
    derived_hint = {
        "2m_dewpoint_temperature": "Qair",
        "10m_u_component_of_wind": "Wind",
        "10m_v_component_of_wind": "Wind",
    }
    if parameter in derived_hint:
        return derived_hint[parameter]

    raise ValueError(f"Cannot resolve target variable for parameter: {parameter}")


def load_inputs_for_target(
    *,
    input_root: str,
    year: int,
    target_var: str,
    chunks_time: int,
    lat_order: str,
) -> Dict[str, xr.DataArray]:
    if target_var not in TARGET_TO_INPUT_PARAMETERS:
        raise ValueError(f"Unsupported target variable: {target_var}")

    out: Dict[str, xr.DataArray] = {}
    for parameter in TARGET_TO_INPUT_PARAMETERS[target_var]:
        raw_var = RAW_PARAMETER_TO_VAR[parameter]
        da = load_parameter_with_boundary(
            input_root=input_root,
            year=year,
            parameter=parameter,
            chunks_time=chunks_time,
        )
        da = ensure_coord_order(da, lat_order=lat_order)
        # Keep compute memory bounded for large global fields.
        da = da.astype("float32")
        out[raw_var] = da

    return out
