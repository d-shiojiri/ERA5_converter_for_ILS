#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PERTURBED_FORCING_DIR = SCRIPT_DIR.parent

DEFAULT_INPUT_ROOT = Path("/data02/shiojiri/ILS/ILS_data/ERA5/30min")
DEFAULT_OUTPUT_ROOT = PERTURBED_FORCING_DIR / "ERA5_ptb_30min"
DEFAULT_PERTURBED_VARS = ("Rainf", "Snowf", "SWdown", "LWdown")

FILE_RE = re.compile(r"^ERA5\.30min\.(?P<var>[A-Za-z0-9_]+)\.1hrMap\.ILS\.(?P<year>\d{4})\.nc$")
MEMBER_RE = re.compile(r"^member_(?P<member>\d+)$")


@dataclass
class Stats:
    created: int = 0
    skipped_same_link: int = 0
    skipped_existing: int = 0
    conflicts: int = 0
    years_skipped: int = 0
    members_processed: int = 0
    source_files: int = 0


def parse_int_tokens(spec: str) -> list[int]:
    values: list[int] = []
    for token in (t.strip() for t in spec.split(",")):
        if not token:
            continue
        if "-" in token:
            a, b = token.split("-", 1)
            start = int(a)
            end = int(b)
            if start > end:
                raise ValueError(f"Invalid range token: {token}")
            values.extend(range(start, end + 1))
        else:
            values.append(int(token))

    if not values:
        raise ValueError("No integer token parsed.")
    if any(v <= 0 for v in values):
        raise ValueError("All values must be positive.")

    seen: set[int] = set()
    unique: list[int] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            unique.append(v)
    return unique


def parse_csv(value: str) -> list[str]:
    out = [v.strip() for v in value.split(",") if v.strip()]
    if not out:
        raise ValueError("CSV value is empty.")
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Create symlinks for ERA5 30min files that are not perturbed into "
            "each year/member directory under ERA5_ptb_30min."
        )
    )
    p.add_argument("--input-root", default=str(DEFAULT_INPUT_ROOT), help="Source ERA5 30min root")
    p.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="Destination ERA5_ptb_30min root",
    )
    p.add_argument(
        "--years",
        default="",
        help="Optional year list/range (e.g. 2013-2022 or 2013,2015). "
        "If omitted, years are detected from output-root.",
    )
    p.add_argument(
        "--members",
        default="",
        help="Optional member list/range (e.g. 1-20). "
        "If omitted, existing member_* directories are used.",
    )
    p.add_argument(
        "--perturbed-vars",
        default=",".join(DEFAULT_PERTURBED_VARS),
        help="Comma-separated variables treated as already perturbed and excluded from linking.",
    )
    p.add_argument("--overwrite", action="store_true", help="Replace existing files/symlinks")
    p.add_argument("--dry-run", action="store_true", help="Show planned actions only")
    p.add_argument("--verbose", action="store_true", help="Print each create/skip action")
    return p.parse_args()


def detect_years_from_output(output_root: Path) -> list[int]:
    years: list[int] = []
    for child in sorted(output_root.iterdir(), key=lambda p: p.name):
        if child.is_dir() and child.name.isdigit() and len(child.name) == 4:
            years.append(int(child.name))
    return years


def detect_members_from_year_dir(year_dir: Path) -> list[int]:
    members: list[int] = []
    for child in sorted(year_dir.iterdir(), key=lambda p: p.name):
        if not child.is_dir():
            continue
        m = MEMBER_RE.match(child.name)
        if m is None:
            continue
        members.append(int(m.group("member")))
    return members


def collect_unperturbed_sources(
    source_year_dir: Path,
    year: int,
    perturbed_vars: set[str],
) -> list[Path]:
    out: list[Path] = []
    for path in sorted(source_year_dir.iterdir(), key=lambda p: p.name):
        if not path.is_file():
            continue
        m = FILE_RE.match(path.name)
        if m is None:
            continue
        file_year = int(m.group("year"))
        if file_year != year:
            continue
        var_name = m.group("var")
        if var_name in perturbed_vars:
            continue
        out.append(path)
    return out


def same_destination(dst: Path, src: Path) -> bool:
    if not dst.exists() and not dst.is_symlink():
        return False
    try:
        return dst.resolve(strict=True) == src.resolve(strict=True)
    except FileNotFoundError:
        return False


def ensure_symlink(
    *,
    src: Path,
    dst: Path,
    overwrite: bool,
    dry_run: bool,
    verbose: bool,
    stats: Stats,
) -> None:
    if same_destination(dst, src):
        stats.skipped_same_link += 1
        if verbose:
            print(f"[skip:same] {dst}")
        return

    if dst.exists() or dst.is_symlink():
        if not overwrite:
            stats.skipped_existing += 1
            if verbose:
                print(f"[skip:exists] {dst}")
            return
        if dst.is_dir() and not dst.is_symlink():
            stats.conflicts += 1
            print(f"[conflict:dir] cannot overwrite directory: {dst}")
            return
        if dry_run:
            print(f"[dry-run:replace] {dst} -> {src}")
        else:
            dst.unlink()
    else:
        if dry_run and verbose:
            print(f"[dry-run:create] {dst} -> {src}")

    if dry_run:
        stats.created += 1
        return

    dst.parent.mkdir(parents=True, exist_ok=True)
    os.symlink(str(src), str(dst))
    stats.created += 1
    if verbose:
        print(f"[linked] {dst} -> {src}")


def main() -> int:
    args = parse_args()

    input_root = Path(args.input_root).expanduser().resolve()
    output_root = Path(args.output_root).expanduser().resolve()
    perturbed_vars = set(parse_csv(args.perturbed_vars))

    if not input_root.exists():
        raise FileNotFoundError(f"input-root not found: {input_root}")
    if not output_root.exists():
        raise FileNotFoundError(f"output-root not found: {output_root}")

    if args.years.strip():
        years = parse_int_tokens(args.years)
    else:
        years = detect_years_from_output(output_root)

    if not years:
        raise ValueError("No years to process.")

    members_filter: list[int] | None = None
    if args.members.strip():
        members_filter = parse_int_tokens(args.members)

    stats = Stats()
    for year in years:
        source_year_dir = input_root / str(year)
        if not source_year_dir.is_dir():
            print(f"[warn] source year directory not found, skip: {source_year_dir}")
            stats.years_skipped += 1
            continue

        output_year_dir = output_root / str(year)
        if not output_year_dir.exists():
            if members_filter is None:
                print(f"[warn] output year directory not found, skip: {output_year_dir}")
                stats.years_skipped += 1
                continue
            if not args.dry_run:
                output_year_dir.mkdir(parents=True, exist_ok=True)

        unperturbed_sources = collect_unperturbed_sources(source_year_dir, year, perturbed_vars)
        if not unperturbed_sources:
            print(f"[warn] no unperturbed source files found for year={year}")
            stats.years_skipped += 1
            continue
        stats.source_files += len(unperturbed_sources)

        if members_filter is None:
            members = detect_members_from_year_dir(output_year_dir)
        else:
            members = members_filter

        if not members:
            print(f"[warn] no member directories for year={year}")
            stats.years_skipped += 1
            continue

        for member in members:
            member_dir = output_year_dir / f"member_{member:02d}"
            if not member_dir.exists() and not args.dry_run:
                member_dir.mkdir(parents=True, exist_ok=True)
            stats.members_processed += 1
            for src_path in unperturbed_sources:
                dst_path = member_dir / src_path.name
                ensure_symlink(
                    src=src_path,
                    dst=dst_path,
                    overwrite=args.overwrite,
                    dry_run=args.dry_run,
                    verbose=args.verbose,
                    stats=stats,
                )

        print(
            f"[year={year}] members={len(members)} link_targets_per_member={len(unperturbed_sources)}"
        )

    print(
        "[done] "
        f"created={stats.created} "
        f"skipped_same_link={stats.skipped_same_link} "
        f"skipped_existing={stats.skipped_existing} "
        f"conflicts={stats.conflicts} "
        f"members_processed={stats.members_processed} "
        f"years_skipped={stats.years_skipped}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
