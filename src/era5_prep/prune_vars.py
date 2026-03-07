"""Pruning helpers for Program A.

Program A writes one target variable per file, so pruning is implicit.
"""

from __future__ import annotations

import xarray as xr


def keep_single_var(ds: xr.Dataset, var_name: str) -> xr.Dataset:
    return ds[[var_name]]
