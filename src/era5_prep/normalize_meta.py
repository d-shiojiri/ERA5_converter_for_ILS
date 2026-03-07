"""Metadata normalization for Program A outputs."""

from __future__ import annotations

from datetime import datetime, timezone

import xarray as xr


def add_stage1_global_attrs(ds: xr.Dataset, *, year: int, target_var: str) -> xr.Dataset:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ds.attrs.update(
        {
            "Conventions": "CF-1.6",
            "stage": "stage1",
            "source": "ERA5 single-levels",
            "id": f"ERA5.STAGE1.{target_var}.{year}",
            "history": f"{now} ProgramA prep run for {target_var} {year}",
        }
    )
    return ds
