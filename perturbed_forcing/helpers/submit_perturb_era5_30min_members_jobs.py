#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import yaml

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = (
    SCRIPT_DIR / "configs" / "submit_perturb_era5_30min_members_default.yaml"
)
DEFAULT_VARS = ("Rainf", "Snowf", "SWdown", "LWdown")
CONFIG_SECTION_KEY = "submit_perturb_era5_30min_members"

DEFAULT_SETTINGS: dict[str, Any] = {
    "years": None,
    "vars": ",".join(DEFAULT_VARS),
    "members": "1-20",
    "input_root": "/data02/shiojiri/ILS/ILS_data/ERA5/30min",
    "output_root": str(SCRIPT_DIR / "ERA5_ptb_30min"),
    "random_file": str(SCRIPT_DIR / "continuous_random_19000101_20260101.nc"),
    "time_chunk": 48,
    "executor": "sbatch",
    "max_parallel_jobs": 8,
    "cpus": 1,
    "mem": "auto",
    "time": "06:00:00",
    "partition": "",
    "log_dir": str(SCRIPT_DIR / "ERA5_ptb_30min" / "logs"),
    "overwrite": False,
    "worker_script": "workers/perturb_era5_30min_members.py",
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


def parse_years(spec: str) -> list[int]:
    return parse_int_tokens(spec)


def parse_vars(spec: str) -> list[str]:
    out = [v.strip() for v in spec.split(",") if v.strip()]
    if not out:
        raise ValueError("No variables specified.")
    unknown = [v for v in out if v not in DEFAULT_VARS]
    if unknown:
        raise ValueError(f"Unsupported variables: {unknown}. Supported: {list(DEFAULT_VARS)}")
    return out


def output_exists_for_all_members(
    *,
    output_root: Path,
    year: int,
    var_name: str,
    members: list[int],
) -> bool:
    for m in members:
        p = (
            output_root
            / str(year)
            / f"member_{m:02d}"
            / f"ERA5.30min.{var_name}.1hrMap.ILS.{year}.nc"
        )
        if not p.exists():
            return False
    return True


def _as_positive_int(name: str, value: Any) -> int:
    try:
        out = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer: {value}") from exc
    if out <= 0:
        raise ValueError(f"{name} must be > 0: {out}")
    return out


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if value is None:
        return False
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off", ""}:
        return False
    raise ValueError(f"Invalid boolean value: {value}")


def _normalize_mem(value: Any) -> str:
    text = str(value or "").strip()
    if text.lower() in {"auto", "none", "unset"}:
        return ""
    return text


def _load_yaml_config(config_path: Path, *, strict_missing: bool) -> dict[str, Any]:
    if not config_path.exists():
        if strict_missing:
            raise FileNotFoundError(f"Config file not found: {config_path}")
        return {}

    with config_path.open("r", encoding="utf-8") as f:
        loaded = yaml.safe_load(f) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"Config must be a mapping: {config_path}")

    if CONFIG_SECTION_KEY in loaded:
        section = loaded[CONFIG_SECTION_KEY]
        if not isinstance(section, dict):
            raise ValueError(f"'{CONFIG_SECTION_KEY}' section must be a mapping.")
        loaded = section

    normalized: dict[str, Any] = {}
    for key, val in loaded.items():
        normalized[str(key).replace("-", "_")] = val
    return normalized


def _resolve_worker_path(worker_spec: str) -> Path:
    worker = Path(worker_spec)
    if not worker.is_absolute():
        worker = SCRIPT_DIR / worker
    return worker.resolve()


def _build_parser(defaults: dict[str, Any], config_path: Path) -> argparse.ArgumentParser:
    years_default = defaults.get("years")
    years_required = not bool(str(years_default).strip()) if years_default is not None else True

    p = argparse.ArgumentParser(
        description=(
            "Submit ERA5 30min perturbation jobs in parallel by year and variable using sbatch."
        )
    )
    p.add_argument(
        "--config",
        default=str(config_path),
        help=(
            "YAML config path. Defaults to "
            f"{config_path}. If file exists, values are used as defaults."
        ),
    )
    p.add_argument("--years", required=years_required, default=years_default, help="e.g. 2013-2022")
    p.add_argument("--vars", default=str(defaults["vars"]), help="comma-separated target vars")
    p.add_argument(
        "--members",
        default=str(defaults["members"]),
        help="member list/range, e.g. 1-20 or 1,2,5",
    )
    p.add_argument("--input-root", default=str(defaults["input_root"]), help="ERA5 30min root")
    p.add_argument("--output-root", default=str(defaults["output_root"]), help="Output root")
    p.add_argument("--random-file", default=str(defaults["random_file"]), help="Random NetCDF path")
    p.add_argument("--time-chunk", default=defaults["time_chunk"], help="Worker time chunk")
    p.add_argument(
        "--executor",
        choices=("sbatch", "local"),
        default=str(defaults["executor"]),
        help="Execution backend",
    )
    p.add_argument("--max-parallel-jobs", default=defaults["max_parallel_jobs"], help="Parallel cap")
    p.add_argument("--cpus", default=defaults["cpus"], help="sbatch --cpus-per-task")
    p.add_argument("--mem", default=str(defaults["mem"]), help="sbatch --mem")
    p.add_argument("--time", dest="time_limit", default=str(defaults["time"]), help="sbatch --time")
    p.add_argument("--partition", default=str(defaults["partition"]), help="sbatch --partition")
    p.add_argument("--log-dir", default=str(defaults["log_dir"]), help="Log directory")
    p.add_argument(
        "--worker-script",
        default=str(defaults["worker_script"]),
        help="Worker script path (relative to perturbed_forcing if not absolute)",
    )
    p.add_argument("--overwrite", dest="overwrite", action="store_true", default=None)
    p.add_argument("--no-overwrite", dest="overwrite", action="store_false")
    p.add_argument("--dry-run", action="store_true", help="Print commands only")
    return p


def run_local(tasks: list[list[str]], max_parallel_jobs: int, dry_run: bool, log_dir: Path) -> None:
    if dry_run:
        for cmd in tasks:
            print("[dry-run][local]", " ".join(cmd))
        return

    log_dir.mkdir(parents=True, exist_ok=True)
    running: list[tuple[subprocess.Popen[str], object]] = []
    for idx, cmd in enumerate(tasks, start=1):
        year = cmd[cmd.index("--year") + 1]
        var_name = cmd[cmd.index("--var") + 1]
        log_path = log_dir / f"perturb_{year}_{var_name}.local.log"
        fp = open(log_path, "w", encoding="utf-8")
        proc = subprocess.Popen(cmd, stdout=fp, stderr=subprocess.STDOUT, text=True)
        running.append((proc, fp))
        print(f"[submit][local] started pid={proc.pid} task={idx}/{len(tasks)} log={log_path}")

        while len(running) >= max_parallel_jobs:
            next_running: list[tuple[subprocess.Popen[str], object]] = []
            for p, p_fp in running:
                rc = p.poll()
                if rc is None:
                    next_running.append((p, p_fp))
                else:
                    p_fp.close()
                    if rc != 0:
                        raise RuntimeError(f"Local task failed: pid={p.pid} rc={rc}")
            running = next_running
            if len(running) >= max_parallel_jobs:
                time.sleep(1.0)

    for p, p_fp in running:
        rc = p.wait()
        p_fp.close()
        if rc != 0:
            raise RuntimeError(f"Local task failed: pid={p.pid} rc={rc}")


def run_sbatch(
    *,
    tasks: list[list[str]],
    max_parallel_jobs: int,
    cpus: int,
    mem: str,
    time_limit: str,
    partition: str,
    log_dir: Path,
    dry_run: bool,
) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    submitted_ids: list[str] = []

    for i, task_cmd in enumerate(tasks):
        year = task_cmd[task_cmd.index("--year") + 1]
        var_name = task_cmd[task_cmd.index("--var") + 1]
        job_name = f"ptb30_{year}_{var_name}"
        dep_args: list[str] = []
        if i >= max_parallel_jobs:
            dep_id = submitted_ids[i - max_parallel_jobs]
            dep_args = ["--dependency", f"afterany:{dep_id}"]

        submit_cmd = [
            "sbatch",
            "--parsable",
            "--export=ALL",
            "--job-name",
            job_name,
            "--cpus-per-task",
            str(cpus),
            "--time",
            time_limit,
            "--output",
            str(log_dir / f"{job_name}.%j.out"),
        ]
        if mem:
            submit_cmd.extend(["--mem", mem])
        if partition:
            submit_cmd.extend(["--partition", partition])
        submit_cmd.extend(dep_args)
        submit_cmd.extend(task_cmd)

        if dry_run:
            print("[dry-run][sbatch]", " ".join(submit_cmd))
            submitted_ids.append(f"DRYRUN_{i}")
            continue

        out = subprocess.check_output(submit_cmd, text=True).strip()
        job_id = out.split(";", 1)[0]
        if not job_id.isdigit():
            raise RuntimeError(f"Failed to parse sbatch job id: {out}")
        submitted_ids.append(job_id)
        print(f"[submit][sbatch] job_id={job_id} name={job_name}")


def parse_args() -> argparse.Namespace:
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    pre_args, _ = pre.parse_known_args()
    config_path = Path(pre_args.config).expanduser()
    strict_missing = "--config" in sys.argv[1:]

    loaded = _load_yaml_config(config_path, strict_missing=strict_missing)
    defaults = {**DEFAULT_SETTINGS, **loaded}
    parser = _build_parser(defaults, config_path)
    args = parser.parse_args()
    args._config_defaults = defaults
    return args


def main() -> int:
    args = parse_args()
    defaults = args._config_defaults

    max_parallel_jobs = _as_positive_int("--max-parallel-jobs", args.max_parallel_jobs)
    cpus = _as_positive_int("--cpus", args.cpus)
    time_chunk = _as_positive_int("--time-chunk", args.time_chunk)
    overwrite = _as_bool(defaults.get("overwrite", False)) if args.overwrite is None else args.overwrite
    mem = _normalize_mem(args.mem)

    years = parse_years(str(args.years))
    vars_list = parse_vars(str(args.vars))
    members = parse_int_tokens(str(args.members))
    if any(m <= 0 for m in members):
        raise ValueError("All members must be positive integers (1-based).")

    worker = _resolve_worker_path(str(args.worker_script))
    if not worker.exists():
        raise FileNotFoundError(f"Worker script not found: {worker}")

    output_root = Path(args.output_root)
    tasks: list[list[str]] = []
    skipped = 0
    for year in years:
        for var_name in vars_list:
            if (not overwrite) and output_exists_for_all_members(
                output_root=output_root, year=year, var_name=var_name, members=members
            ):
                skipped += 1
                print(f"[submit] skip existing year={year} var={var_name}")
                continue

            cmd = [
                str(worker),
                "--year",
                str(year),
                "--var",
                var_name,
                "--members",
                str(args.members),
                "--input-root",
                str(args.input_root),
                "--output-root",
                str(args.output_root),
                "--random-file",
                str(args.random_file),
                "--time-chunk",
                str(time_chunk),
            ]
            if overwrite:
                cmd.append("--overwrite")
            tasks.append(cmd)

    print(
        f"[submit] executor={args.executor} tasks={len(tasks)} skipped_existing={skipped} "
        f"years={len(years)} vars={len(vars_list)} members={len(members)}"
    )
    if not tasks:
        print("[submit] nothing to do")
        return 0

    if args.executor == "local":
        run_local(
            tasks=tasks,
            max_parallel_jobs=max_parallel_jobs,
            dry_run=args.dry_run,
            log_dir=Path(args.log_dir),
        )
    else:
        run_sbatch(
            tasks=tasks,
            max_parallel_jobs=max_parallel_jobs,
            cpus=cpus,
            mem=mem,
            time_limit=str(args.time_limit),
            partition=str(args.partition),
            log_dir=Path(args.log_dir),
            dry_run=args.dry_run,
        )

    print("[submit] done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
