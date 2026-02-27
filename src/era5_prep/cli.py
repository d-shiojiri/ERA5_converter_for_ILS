"""CLI for Program A (ERA5 preprocessing)."""

from __future__ import annotations

import argparse
from typing import Iterable

import xarray as xr

from era5_common.progress import has_dask_progress, step_iterator

from .derive_vars import build_stage1_variable
from .io_raw import load_inputs_for_target, resolve_target_var
from .normalize_meta import add_stage1_global_attrs
from .normalize_time import validate_time_axis
from .reorder_dims import reorder_to_canonical
from .validate_stage1 import validate_year
from .write_stage1 import write_stage1_dataset


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="era5-prep", description="Program A")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="run Program A for one year-variable")
    run.add_argument("--year", type=int, required=True)
    run.add_argument("--parameter", type=str, default=None)
    run.add_argument("--target-var", type=str, default=None)
    run.add_argument(
        "--input-root",
        type=str,
        default="/data02/shiojiri/DATA/ERA5/download/reanalysis-era5-single-levels/",
    )
    run.add_argument(
        "--output-root",
        type=str,
        default="/data02/shiojiri/ILS/ILS_data/ERA5/era5_canonical",
    )
    run.add_argument("--lat-order", choices=["ascending", "descending"], default="descending")
    run.add_argument("--chunks-time", type=int, default=168)
    run.add_argument(
        "--progress",
        choices=["auto", "none", "tqdm", "dask"],
        default="auto",
    )
    run.add_argument("--overwrite", action="store_true")

    validate = sub.add_parser("validate", help="validate Stage1 outputs")
    validate.add_argument("--year", type=int, required=True)
    validate.add_argument(
        "--canonical-root",
        "--stage1-root",
        dest="canonical_root",
        type=str,
        default="/data02/shiojiri/ILS/ILS_data/ERA5/era5_canonical",
        help="Program A canonical ERA5 output root",
    )
    validate.add_argument("--vars", type=str, default="")

    return parser.parse_args(argv)


def decide_progress_mode(mode: str) -> str:
    if mode != "auto":
        return mode
    return "dask" if has_dask_progress() else "tqdm"


def cmd_run(args: argparse.Namespace) -> int:
    target_var = resolve_target_var(parameter=args.parameter, target_var=args.target_var)
    progress_mode = decide_progress_mode(args.progress)

    steps = ["load_inputs", "derive_variable", "normalize", "write"]
    step_iter = step_iterator(steps, progress_mode)

    loaded_inputs = None
    stage1_var = None
    for step in step_iter:
        if step == "load_inputs":
            loaded_inputs = load_inputs_for_target(
                input_root=args.input_root,
                year=args.year,
                target_var=target_var,
                chunks_time=args.chunks_time,
                lat_order=args.lat_order,
            )
        elif step == "derive_variable":
            assert loaded_inputs is not None
            aligned = xr.align(*loaded_inputs.values(), join="exact")
            loaded_inputs = dict(zip(loaded_inputs.keys(), aligned))
            stage1_var = build_stage1_variable(target_var=target_var, inputs=loaded_inputs)
        elif step == "normalize":
            assert stage1_var is not None
            stage1_var = reorder_to_canonical(stage1_var)
            validate_time_axis(stage1_var, args.year)
        elif step == "write":
            assert stage1_var is not None
            ds = stage1_var.to_dataset(name=target_var)
            ds = add_stage1_global_attrs(ds, year=args.year, target_var=target_var)
            out = write_stage1_dataset(
                ds,
                stage1_root=args.output_root,
                year=args.year,
                target_var=target_var,
                overwrite=args.overwrite,
                use_dask_progress=(progress_mode == "dask"),
            )
            print(f"[prep] wrote {out}")

    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    target_vars = [v.strip() for v in args.vars.split(",") if v.strip()] or None
    issues = validate_year(stage1_root=args.canonical_root, year=args.year, target_vars=target_vars)
    if issues:
        print("[prep][validate] FAILED")
        for issue in issues:
            print(f" - {issue}")
        return 1

    print("[prep][validate] OK")
    return 0


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "run":
        return cmd_run(args)
    if args.command == "validate":
        return cmd_validate(args)
    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
