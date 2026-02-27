"""CLI for Program B (align canonical ILS fields to 0.5-degree default grid)."""

from __future__ import annotations

import argparse
from typing import Iterable

from era5_common.constants import TARGET_VARS
from era5_common.progress import has_dask_progress, step_iterator

from .io_ils import build_ils_dataset, write_ils_dataset
from .io_stage1 import load_stage1_variable
from .transform_space import regrid_to_half_degree
from .transform_vars import convert_to_ils_units
from .validate_stage2 import validate_year


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="ils-resample",
        description="Program B: resample canonical ILS fields to default 0.5-degree grid",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    convert_var = sub.add_parser("convert-var", help="convert one year-variable")
    convert_var.add_argument("--year", type=int, required=True)
    convert_var.add_argument("--target-var", type=str, required=True)
    convert_var.add_argument(
        "--canonical-root",
        "--stage1-root",
        dest="canonical_root",
        type=str,
        default="/data02/shiojiri/ILS/ILS_data/ERA5/era5_canonical",
        help="Program A canonical ERA5 output root",
    )
    convert_var.add_argument(
        "--output-root",
        type=str,
        default="/data02/shiojiri/ILS/ILS_data/ERA5/out",
    )
    convert_var.add_argument("--chunks-time", type=int, default=168)
    convert_var.add_argument(
        "--method",
        choices=["conservative", "bilinear", "block_mean_2x2"],
        default="conservative",
    )
    convert_var.add_argument(
        "--progress",
        choices=["auto", "none", "tqdm", "dask"],
        default="auto",
    )
    convert_var.add_argument("--overwrite", action="store_true")

    convert = sub.add_parser("convert", help="convert all target vars for a year")
    convert.add_argument("--year", type=int, required=True)
    convert.add_argument(
        "--canonical-root",
        "--stage1-root",
        dest="canonical_root",
        type=str,
        default="/data02/shiojiri/ILS/ILS_data/ERA5/era5_canonical",
        help="Program A canonical ERA5 output root",
    )
    convert.add_argument(
        "--output-root",
        type=str,
        default="/data02/shiojiri/ILS/ILS_data/ERA5/out",
    )
    convert.add_argument("--chunks-time", type=int, default=168)
    convert.add_argument(
        "--method",
        choices=["conservative", "bilinear", "block_mean_2x2"],
        default="conservative",
    )
    convert.add_argument(
        "--progress",
        choices=["auto", "none", "tqdm", "dask"],
        default="auto",
    )
    convert.add_argument("--overwrite", action="store_true")

    validate = sub.add_parser("validate", help="validate Program B outputs")
    validate.add_argument("--year", type=int, required=True)
    validate.add_argument(
        "--output-root",
        type=str,
        default="/data02/shiojiri/ILS/ILS_data/ERA5/out",
    )
    validate.add_argument("--vars", type=str, default="")

    return parser.parse_args(argv)


def decide_progress_mode(mode: str) -> str:
    if mode != "auto":
        return mode
    return "dask" if has_dask_progress() else "tqdm"


def run_single(
    *,
    year: int,
    target_var: str,
    canonical_root: str,
    output_root: str,
    chunks_time: int,
    method: str,
    progress_mode: str,
    overwrite: bool,
) -> None:
    if target_var not in TARGET_VARS:
        raise ValueError(f"Unsupported target variable: {target_var}")

    steps = ["load_stage1", "regrid", "units", "write"]
    step_iter = step_iterator(steps, progress_mode)

    stage1_da = None
    regridded = None
    final = None

    for step in step_iter:
        if step == "load_stage1":
            stage1_da = load_stage1_variable(
                stage1_root=canonical_root,
                year=year,
                target_var=target_var,
                chunks_time=chunks_time,
            )
        elif step == "regrid":
            assert stage1_da is not None
            regridded = regrid_to_half_degree(stage1_da, method=method)
        elif step == "units":
            assert regridded is not None
            final = convert_to_ils_units(target_var=target_var, da=regridded)
        elif step == "write":
            assert final is not None
            ds = build_ils_dataset(target_var=target_var, da=final, year=year)
            out = write_ils_dataset(
                ds,
                output_root=output_root,
                year=year,
                target_var=target_var,
                overwrite=overwrite,
                use_dask_progress=(progress_mode == "dask"),
            )
            print(f"[convert] wrote {out}")


def cmd_convert_var(args: argparse.Namespace) -> int:
    progress_mode = decide_progress_mode(args.progress)
    run_single(
        year=args.year,
        target_var=args.target_var,
        canonical_root=args.canonical_root,
        output_root=args.output_root,
        chunks_time=args.chunks_time,
        method=args.method,
        progress_mode=progress_mode,
        overwrite=args.overwrite,
    )
    return 0


def cmd_convert(args: argparse.Namespace) -> int:
    progress_mode = decide_progress_mode(args.progress)
    vars_iter = step_iterator(list(TARGET_VARS), "tqdm" if progress_mode == "tqdm" else "none")
    for var in vars_iter:
        run_single(
            year=args.year,
            target_var=var,
            canonical_root=args.canonical_root,
            output_root=args.output_root,
            chunks_time=args.chunks_time,
            method=args.method,
            progress_mode=progress_mode,
            overwrite=args.overwrite,
        )
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    target_vars = [v.strip() for v in args.vars.split(",") if v.strip()] or None
    issues = validate_year(output_root=args.output_root, year=args.year, target_vars=target_vars)
    if issues:
        print("[convert][validate] FAILED")
        for issue in issues:
            print(f" - {issue}")
        return 1

    print("[convert][validate] OK")
    return 0


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "convert-var":
        return cmd_convert_var(args)
    if args.command == "convert":
        return cmd_convert(args)
    if args.command == "validate":
        return cmd_validate(args)
    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
