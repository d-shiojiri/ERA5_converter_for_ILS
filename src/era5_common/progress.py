"""Progress reporting helpers (tqdm or dask)."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator


def has_dask_progress() -> bool:
    try:
        from dask.diagnostics import ProgressBar  # noqa: F401

        return True
    except Exception:
        return False


@contextmanager
def dask_progress(enabled: bool) -> Iterator[None]:
    if not enabled:
        yield
        return

    try:
        from dask.diagnostics import ProgressBar

        with ProgressBar():
            yield
    except Exception:
        yield


def step_iterator(items: list[str], mode: str):
    if mode != "tqdm":
        for item in items:
            yield item
        return

    try:
        from tqdm import tqdm

        yield from tqdm(items, desc="steps", unit="step")
    except Exception:
        for item in items:
            yield item
