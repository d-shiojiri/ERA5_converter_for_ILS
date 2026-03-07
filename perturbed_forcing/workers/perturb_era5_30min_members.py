#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import netCDF4 as nc
import numpy as np
import xarray as xr

WORKER_DIR = Path(__file__).resolve().parent
PERTURBED_FORCING_DIR = WORKER_DIR.parent


@dataclass(frozen=True)
class PerturbSpec:
    factor_var: str
    operation: str  # multiplicative | additive
    clip_min: float | None = None


TARGET_SPECS: dict[str, PerturbSpec] = {
    "Rainf": PerturbSpec(factor_var="factor_Precip", operation="multiplicative"),
    "Snowf": PerturbSpec(factor_var="factor_Precip", operation="multiplicative"),
    "SWdown": PerturbSpec(factor_var="factor_SWdown", operation="multiplicative"),
    "LWdown": PerturbSpec(factor_var="factor_LWdown", operation="additive", clip_min=0.0),
}


def parse_int_tokens(spec: str) -> list[int]:
    values: list[int] = []
    for token in (t.strip() for t in spec.split(",")):
        if not token:
            continue
        if "-" in token:
            a, b = token.split("-", 1)
            start, end = int(a), int(b)
            if start > end:
                raise ValueError(f"Invalid range token: {token}")
            values.extend(range(start, end + 1))
        else:
            values.append(int(token))
    if not values:
        raise ValueError("No valid integer tokens found.")
    seen: set[int] = set()
    unique: list[int] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            unique.append(v)
    return unique


def build_input_path(input_root: Path, year: int, var_name: str) -> Path:
    return input_root / str(year) / f"ERA5.30min.{var_name}.1hrMap.ILS.{year}.nc"


def build_output_path(output_root: Path, year: int, var_name: str, member: int) -> Path:
    return (
        output_root
        / str(year)
        / f"member_{member:02d}"
        / f"ERA5.30min.{var_name}.1hrMap.ILS.{year}.nc"
    )


def read_time_ns(path: Path) -> np.ndarray:
    with xr.open_dataset(path, decode_times=True, engine="netcdf4") as ds:
        return ds["time"].values.astype("datetime64[ns]").astype(np.int64)


def load_aligned_factors(
    *,
    random_path: Path,
    factor_var: str,
    source_time_ns: np.ndarray,
    members: list[int],
) -> np.ndarray:
    with xr.open_dataset(random_path, decode_times=True, engine="netcdf4") as rds:
        if factor_var not in rds.data_vars:
            raise KeyError(f"factor variable not found in random file: {factor_var}")
        random_time_ns = rds["time"].values.astype("datetime64[ns]").astype(np.int64)
        idx = np.searchsorted(random_time_ns, source_time_ns)
        valid_idx = (idx >= 0) & (idx < random_time_ns.size)
        if not np.all(valid_idx):
            raise ValueError("Some source timesteps are outside random time coverage.")
        if not np.all(random_time_ns[idx] == source_time_ns):
            raise ValueError(
                "Random file does not have exact timestamp matches for source timesteps."
            )

        available_members = set(int(v) for v in rds["ensemble"].values.tolist())
        missing = [m for m in members if m not in available_members]
        if missing:
            raise ValueError(
                f"Members not found in random file: {missing}. "
                f"Available range: {min(available_members)}-{max(available_members)}"
            )

        da = rds[factor_var].sel(ensemble=members).isel(
            time=xr.DataArray(idx, dims="time_index")
        )
        out = da.transpose("time_index", "ensemble").values.astype(np.float32)
        if out.shape != (source_time_ns.size, len(members)):
            raise ValueError(
                f"Unexpected factor shape: {out.shape}, "
                f"expected {(source_time_ns.size, len(members))}"
            )
        return out


def apply_perturbation(
    data_chunk: np.ma.MaskedArray | np.ndarray,
    factor_1d: np.ndarray,
    spec: PerturbSpec,
) -> np.ma.MaskedArray:
    data = np.ma.array(data_chunk, copy=True).astype(np.float32)
    factor = factor_1d.astype(np.float32)[:, None, None]

    if spec.operation == "multiplicative":
        out = data * factor
    elif spec.operation == "additive":
        out = data + factor
        if spec.clip_min is not None:
            out = np.ma.maximum(out, np.float32(spec.clip_min))
    else:
        raise ValueError(f"Unsupported operation: {spec.operation}")
    return out.astype(np.float32)


def iter_chunks(n_time: int, chunk: int) -> Iterable[tuple[int, int]]:
    start = 0
    while start < n_time:
        end = min(start + chunk, n_time)
        yield start, end
        start = end


def process_one_year_var(
    *,
    year: int,
    var_name: str,
    members: list[int],
    input_root: Path,
    output_root: Path,
    random_path: Path,
    time_chunk: int,
    overwrite: bool,
    dry_run: bool,
) -> None:
    spec = TARGET_SPECS[var_name]
    input_path = build_input_path(input_root, year, var_name)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    if not random_path.exists():
        raise FileNotFoundError(f"Random file not found: {random_path}")

    source_time_ns = read_time_ns(input_path)
    factors = load_aligned_factors(
        random_path=random_path,
        factor_var=spec.factor_var,
        source_time_ns=source_time_ns,
        members=members,
    )

    requested: list[tuple[int, Path, Path]] = []
    for member in members:
        out_path = build_output_path(output_root, year, var_name, member)
        tmp_path = out_path.with_name(f"{out_path.name}.tmp.{os.getpid()}")
        if out_path.exists() and not overwrite:
            continue
        requested.append((member, out_path, tmp_path))

    if not requested:
        print(f"[perturb] skip year={year} var={var_name}: all outputs already exist")
        return

    if dry_run:
        for member, out_path, _ in requested:
            print(
                f"[dry-run] year={year} var={var_name} member={member:02d} -> {out_path}"
            )
        return

    for _, out_path, tmp_path in requested:
        tmp_path.parent.mkdir(parents=True, exist_ok=True)
        if tmp_path.exists():
            tmp_path.unlink()
        shutil.copy2(input_path, tmp_path)

    dst_list: list[tuple[int, nc.Dataset, Path, Path]] = []
    member_col = {m: i for i, m in enumerate(members)}
    try:
        with nc.Dataset(input_path, "r") as src:
            if var_name not in src.variables:
                raise KeyError(f"Variable not found in input: {var_name}")
            src_var = src.variables[var_name]
            n_time = int(src_var.shape[0])
            if n_time != source_time_ns.size:
                raise ValueError(
                    f"time length mismatch: data={n_time}, time coord={source_time_ns.size}"
                )

            for member, out_path, tmp_path in requested:
                dst = nc.Dataset(tmp_path, "r+")
                dst.setncattr("perturb_member", int(member))
                dst.setncattr("perturb_source_random_file", str(random_path))
                dst.setncattr("perturb_factor_variable", spec.factor_var)
                dst.setncattr("perturb_operation", spec.operation)
                if spec.clip_min is not None:
                    dst.setncattr("perturb_clip_min", float(spec.clip_min))
                dst_list.append((member, dst, out_path, tmp_path))

            for chunk_i, (start, end) in enumerate(iter_chunks(n_time, time_chunk), start=1):
                src_chunk = src_var[start:end, :, :]
                for member, dst, _, _ in dst_list:
                    col = member_col[member]
                    factor_1d = factors[start:end, col]
                    out_chunk = apply_perturbation(src_chunk, factor_1d, spec)
                    dst.variables[var_name][start:end, :, :] = out_chunk
                print(f"[perturb] year={year} var={var_name} chunk={chunk_i} time={start}:{end}")
    except Exception:
        known_tmp_paths = {tmp_path for _, _, _, tmp_path in dst_list}
        for _, _, tmp_path in requested:
            if tmp_path in known_tmp_paths or tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
        raise
    finally:
        for _, dst, _, _ in dst_list:
            dst.close()

    for member, _, out_path, tmp_path in dst_list:
        if out_path.exists() and overwrite:
            out_path.unlink()
        shutil.move(str(tmp_path), str(out_path))
        print(f"[perturb] wrote year={year} var={var_name} member={member:02d} -> {out_path}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Apply generated random perturbations to ERA5 30min files. "
            "Rainf/Snowf use factor_Precip (multiplicative), "
            "SWdown uses factor_SWdown (multiplicative), "
            "LWdown uses factor_LWdown (additive, clipped at 0)."
        )
    )
    p.add_argument("--year", type=int, required=True, help="Target year, e.g. 2016")
    p.add_argument(
        "--var",
        required=True,
        choices=sorted(TARGET_SPECS.keys()),
        help="Target variable",
    )
    p.add_argument(
        "--members",
        default="1-20",
        help="Member list/range, e.g. 1-20 or 1,3,5",
    )
    p.add_argument(
        "--input-root",
        default="/data02/shiojiri/ILS/ILS_data/ERA5/30min",
        help="ERA5 30min root directory",
    )
    p.add_argument(
        "--output-root",
        default=str(PERTURBED_FORCING_DIR / "ERA5_ptb_30min"),
        help="Output root directory",
    )
    p.add_argument(
        "--random-file",
        default=str(PERTURBED_FORCING_DIR / "continuous_random_19000101_20260101.nc"),
        help="Random NetCDF path",
    )
    p.add_argument(
        "--time-chunk",
        type=int,
        default=48,
        help="Time chunk size for I/O processing",
    )
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing output files")
    p.add_argument("--dry-run", action="store_true", help="Show actions without writing")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.time_chunk <= 0:
        raise ValueError("--time-chunk must be > 0")

    members = parse_int_tokens(args.members)
    if any(m <= 0 for m in members):
        raise ValueError("All members must be positive integers (1-based).")

    process_one_year_var(
        year=args.year,
        var_name=args.var,
        members=members,
        input_root=Path(args.input_root),
        output_root=Path(args.output_root),
        random_path=Path(args.random_file),
        time_chunk=args.time_chunk,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
    )
    print("[perturb] done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
