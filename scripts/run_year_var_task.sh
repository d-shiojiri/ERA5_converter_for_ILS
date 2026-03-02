#!/usr/bin/env bash
set -euo pipefail

COMMON_SH=""
for cand in \
  "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh" \
  "${ERA5_SCRIPT_DIR:-}/_common.sh" \
  "${SLURM_SUBMIT_DIR:-}/scripts/_common.sh" \
  "$(pwd)/scripts/_common.sh"
do
  if [[ -n "$cand" && -f "$cand" ]]; then
    COMMON_SH="$cand"
    break
  fi
done

if [[ -z "$COMMON_SH" ]]; then
  echo "Failed to locate _common.sh. Set ERA5_SCRIPT_DIR or run from repo root." >&2
  exit 2
fi

source "$COMMON_SH"
export ERA5_SCRIPT_DIR="${ERA5_SCRIPT_DIR:-$(cd "$(dirname "$COMMON_SH")" && pwd)}"

prep_config="${ERA5_PREP_CONFIG:-$REPO_ROOT/configs/prep_default.yaml}"
convert_config="${ERA5_CONVERT_CONFIG:-$REPO_ROOT/configs/convert_default.yaml}"

input_root="$(yaml_get "$prep_config" "input_root" "/data02/shiojiri/DATA/ERA5/download/reanalysis-era5-single-levels/")"
canonical_root="$(yaml_get "$convert_config" "canonical_root" "$(yaml_get "$prep_config" "output_root" "/data02/shiojiri/ILS/ILS_data/ERA5/era5_canonical")")"
output_root="$(yaml_get "$convert_config" "output_root" "/data02/shiojiri/ILS/ILS_data/ERA5/30min")"
prep_chunks_time="$(yaml_get "$prep_config" "chunks_time" "24")"
convert_chunks_time="$(yaml_get "$convert_config" "chunks_time" "24")"
prep_chunks_time_rain_snow="$(yaml_get "$prep_config" "rain_snow_chunks_time" "")"
convert_chunks_time_rain_snow="$(yaml_get "$convert_config" "rain_snow_chunks_time" "")"
method="$(yaml_get "$convert_config" "method" "conservative")"
progress="$(yaml_get "$convert_config" "progress" "$(yaml_get "$prep_config" "progress" "auto")")"
prep_python_cmd_cfg="$(yaml_get "$prep_config" "python_cmd" "")"
convert_python_cmd_cfg="$(yaml_get "$convert_config" "python_cmd" "")"
prep_dask_num_workers="$(yaml_get "$prep_config" "dask_num_workers" "1")"
convert_dask_num_workers="$(yaml_get "$convert_config" "dask_num_workers" "$prep_dask_num_workers")"
prep_dask_num_workers_rain_snow="$(yaml_get "$prep_config" "rain_snow_dask_num_workers" "")"
convert_dask_num_workers_rain_snow="$(yaml_get "$convert_config" "rain_snow_dask_num_workers" "")"
prep_dask_scheduler="$(yaml_get "$prep_config" "dask_scheduler" "threads")"
convert_dask_scheduler="$(yaml_get "$convert_config" "dask_scheduler" "$prep_dask_scheduler")"
prep_write_time_block="$(yaml_get "$prep_config" "write_time_block" "24")"
convert_write_time_block="$(yaml_get "$convert_config" "write_time_block" "$prep_write_time_block")"
prep_write_time_block_rain_snow="$(yaml_get "$prep_config" "rain_snow_write_time_block" "")"
convert_write_time_block_rain_snow="$(yaml_get "$convert_config" "rain_snow_write_time_block" "")"

usage() {
  cat <<USAGE
Usage:
  run_year_var_task.sh --stage <prep|convert> --year <YYYY> --var <TARGET_VAR> [options]

Required:
  --stage           prep or convert
  --year            target year (e.g., 2000)
  --var             target variable (Tair,Qair,PSurf,Wind,SWdown,LWdown,Precip,Rainf,Snowf,CCover)

Optional overrides:
  --input-root      raw ERA5 root (default from $prep_config)
  --canonical-root  Program A canonical ERA5 output root (default from $convert_config)
  --stage1-root     deprecated alias of --canonical-root
  --output-root     Program B output root (default from $convert_config)
  --prep-chunks-time     Program A time chunk size (default from $prep_config)
  --convert-chunks-time  Program B time chunk size (default from $convert_config)
  --method          Program B regrid method (default from $convert_config)
  --progress        auto|none|tqdm|dask (default from configs)
  --dry-run         print command only
  --help            show this help

Config files (env override):
  ERA5_PREP_CONFIG=$prep_config
  ERA5_CONVERT_CONFIG=$convert_config

Env override command templates:
  PREP_RUNNER_CMD      default: <python_cmd> -m era5_prep run
  CONVERT_RUNNER_CMD   default: <python_cmd> -m era5_to_ils convert-var
USAGE
}

stage=""
year=""
var_name=""
dry_run=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --stage) stage="$2"; shift 2 ;;
    --year) year="$2"; shift 2 ;;
    --var) var_name="$2"; shift 2 ;;
    --input-root) input_root="$2"; shift 2 ;;
    --canonical-root|--stage1-root) canonical_root="$2"; shift 2 ;;
    --output-root) output_root="$2"; shift 2 ;;
    --prep-chunks-time) prep_chunks_time="$2"; shift 2 ;;
    --convert-chunks-time) convert_chunks_time="$2"; shift 2 ;;
    --method) method="$2"; shift 2 ;;
    --progress) progress="$2"; shift 2 ;;
    --dry-run) dry_run=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "$stage" || -z "$year" || -z "$var_name" ]]; then
  echo "Missing required args: --stage, --year, --var" >&2
  usage
  exit 2
fi
if [[ "$stage" != "prep" && "$stage" != "convert" ]]; then
  echo "--stage must be prep or convert: $stage" >&2
  exit 2
fi
if ! [[ "$year" =~ ^[0-9]{4}$ ]]; then
  echo "--year must be 4-digit year: $year" >&2
  exit 2
fi

if [[ "$stage" == "prep" ]]; then
  python_cmd_cfg="$prep_python_cmd_cfg"
  dask_num_workers="$prep_dask_num_workers"
  dask_scheduler="$prep_dask_scheduler"
  write_time_block="$prep_write_time_block"
else
  python_cmd_cfg="$convert_python_cmd_cfg"
  if [[ -z "$python_cmd_cfg" ]]; then
    python_cmd_cfg="$prep_python_cmd_cfg"
  fi
  dask_num_workers="$convert_dask_num_workers"
  dask_scheduler="$convert_dask_scheduler"
  write_time_block="$convert_write_time_block"
fi

resolve_workers_auto() {
  local raw="$1"
  local lc
  lc="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  if [[ "$lc" != "auto" && "$lc" != "0" && -n "$lc" ]]; then
    if [[ "$lc" =~ ^[0-9]+$ ]] && (( lc > 0 )); then
      printf '%s' "$lc"
      return 0
    fi
  fi

  if [[ -n "${SLURM_CPUS_PER_TASK:-}" ]] && [[ "${SLURM_CPUS_PER_TASK}" =~ ^[0-9]+$ ]] && (( SLURM_CPUS_PER_TASK > 0 )); then
    printf '%s' "$SLURM_CPUS_PER_TASK"
    return 0
  fi

  if command -v nproc >/dev/null 2>&1; then
    local n
    n="$(nproc 2>/dev/null || true)"
    if [[ "$n" =~ ^[0-9]+$ ]] && (( n > 0 )); then
      printf '%s' "$n"
      return 0
    fi
  fi

  printf '%s' "1"
}

apply_rain_snow_overrides() {
  local var="$1"
  local stage_name="$2"

  if [[ "$var" != "Rainf" && "$var" != "Snowf" ]]; then
    return 0
  fi

  if [[ "$stage_name" == "prep" ]]; then
    if [[ -n "$prep_chunks_time_rain_snow" ]]; then
      prep_chunks_time="$prep_chunks_time_rain_snow"
    fi
    if [[ -n "$prep_dask_num_workers_rain_snow" ]]; then
      dask_num_workers="$prep_dask_num_workers_rain_snow"
    fi
    if [[ -n "$prep_write_time_block_rain_snow" ]]; then
      write_time_block="$prep_write_time_block_rain_snow"
    fi
  else
    if [[ -n "$convert_chunks_time_rain_snow" ]]; then
      convert_chunks_time="$convert_chunks_time_rain_snow"
    fi
    if [[ -n "$convert_dask_num_workers_rain_snow" ]]; then
      dask_num_workers="$convert_dask_num_workers_rain_snow"
    fi
    if [[ -n "$convert_write_time_block_rain_snow" ]]; then
      write_time_block="$convert_write_time_block_rain_snow"
    fi
  fi
}

apply_rain_snow_overrides "$var_name" "$stage"

# Ensure local source tree is importable in batch jobs without editable install.
if [[ -n "${PYTHONPATH:-}" ]]; then
  export PYTHONPATH="$REPO_ROOT/src:$PYTHONPATH"
else
  export PYTHONPATH="$REPO_ROOT/src"
fi

# Keep memory bounded on large ERA5 jobs unless user explicitly overrides.
resolved_dask_workers="$(resolve_workers_auto "$dask_num_workers")"
export ERA5_DASK_NUM_WORKERS="${ERA5_DASK_NUM_WORKERS:-$resolved_dask_workers}"
export ERA5_DASK_SCHEDULER="${ERA5_DASK_SCHEDULER:-$dask_scheduler}"
export ERA5_WRITE_TIME_BLOCK="${ERA5_WRITE_TIME_BLOCK:-$write_time_block}"
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
export NUMEXPR_NUM_THREADS="${NUMEXPR_NUM_THREADS:-1}"

find_system_python() {
  if command -v python >/dev/null 2>&1; then
    printf '%s' "python"
    return 0
  fi
  if command -v python3 >/dev/null 2>&1; then
    printf '%s' "python3"
    return 0
  fi
  return 1
}

declare -a python_cmd_parts=()

if [[ -n "$python_cmd_cfg" ]]; then
  if [[ "$python_cmd_cfg" == *" "* ]]; then
    # Support wrapped runners, e.g.:
    # singularity exec /path/to/image.sif python3
    read -r -a python_cmd_parts <<<"$python_cmd_cfg"
    if [[ "${#python_cmd_parts[@]}" -eq 0 ]]; then
      echo "Invalid python_cmd: $python_cmd_cfg" >&2
      exit 2
    fi
    cmd0="${python_cmd_parts[0]}"
    if [[ "$cmd0" == */* ]] && [[ "$cmd0" != /* ]] && [[ -x "$REPO_ROOT/$cmd0" ]]; then
      python_cmd_parts[0]="$REPO_ROOT/$cmd0"
      cmd0="${python_cmd_parts[0]}"
    fi
    if ! command -v "$cmd0" >/dev/null 2>&1 && [[ ! -x "$cmd0" ]]; then
      echo "Configured python_cmd command not found: $cmd0" >&2
      exit 2
    fi
    python_cmd="${python_cmd_parts[*]}"
  else
  cfg_is_default_venv=0
  if [[ "$python_cmd_cfg" == ".venv/bin/python" || "$python_cmd_cfg" == "./.venv/bin/python" ]]; then
    cfg_is_default_venv=1
  fi

  # Resolve relative paths against repo root so Slurm spool execution still works.
  if [[ "$python_cmd_cfg" == */* ]]; then
    if [[ "$python_cmd_cfg" == /* ]]; then
      resolved_python_cmd="$python_cmd_cfg"
    else
      resolved_python_cmd="$REPO_ROOT/$python_cmd_cfg"
    fi
    if [[ -x "$resolved_python_cmd" ]]; then
      python_cmd="$resolved_python_cmd"
    elif [[ "$cfg_is_default_venv" -eq 1 ]]; then
      if system_python_cmd="$(find_system_python)"; then
        python_cmd="$system_python_cmd"
        echo "Configured .venv interpreter not found; falling back to $python_cmd." >&2
      else
        echo "Configured .venv interpreter not found: $resolved_python_cmd" >&2
        echo "Neither python nor python3 was found in PATH." >&2
        exit 2
      fi
    else
      echo "Configured python_cmd not found: $python_cmd_cfg" >&2
      echo "Set python_cmd in config to a valid interpreter path (e.g. .venv/bin/python)." >&2
      exit 2
    fi
  elif command -v "$python_cmd_cfg" >/dev/null 2>&1; then
    python_cmd="$python_cmd_cfg"
  elif [[ "$cfg_is_default_venv" -eq 1 ]]; then
    if system_python_cmd="$(find_system_python)"; then
      python_cmd="$system_python_cmd"
      echo "Configured .venv interpreter not found; falling back to $python_cmd." >&2
    else
      echo "Configured .venv interpreter not found: $python_cmd_cfg" >&2
      echo "Neither python nor python3 was found in PATH." >&2
      exit 2
    fi
  else
    echo "Configured python_cmd not found: $python_cmd_cfg" >&2
    echo "Set python_cmd in config to a valid interpreter path (e.g. .venv/bin/python)." >&2
    exit 2
  fi
  python_cmd_parts=("$python_cmd")
  fi
elif [[ -x "$REPO_ROOT/.venv/bin/python" ]]; then
  python_cmd="$REPO_ROOT/.venv/bin/python"
  python_cmd_parts=("$python_cmd")
elif system_python_cmd="$(find_system_python)"; then
  python_cmd="$system_python_cmd"
  python_cmd_parts=("$python_cmd")
else
  echo "Neither python nor python3 was found in PATH." >&2
  exit 2
fi

prep_runner_cmd="${PREP_RUNNER_CMD:-}"
convert_runner_cmd="${CONVERT_RUNNER_CMD:-}"

if [[ "$stage" == "prep" ]]; then
  if [[ -n "$prep_runner_cmd" ]]; then
    read -r -a runner <<<"$prep_runner_cmd"
  else
    runner=("${python_cmd_parts[@]}" -m era5_prep run)
  fi
  cmd=(
    "${runner[@]}"
    --year "$year"
    --target-var "$var_name"
    --input-root "$input_root"
    --output-root "$canonical_root"
    --chunks-time "$prep_chunks_time"
    --progress "$progress"
  )
else
  if [[ -n "$convert_runner_cmd" ]]; then
    read -r -a runner <<<"$convert_runner_cmd"
  else
    runner=("${python_cmd_parts[@]}" -m era5_to_ils convert-var)
  fi
  cmd=(
    "${runner[@]}"
    --year "$year"
    --target-var "$var_name"
    --canonical-root "$canonical_root"
    --output-root "$output_root"
    --chunks-time "$convert_chunks_time"
    --method "$method"
    --progress "$progress"
  )
fi

echo "[task] stage=$stage year=$year var=$var_name"
if [[ -n "${SLURM_JOB_ID:-}" ]]; then
  echo "[task] slurm_job_id=${SLURM_JOB_ID} cpus_per_task=${SLURM_CPUS_PER_TASK:-n/a} mem_per_node=${SLURM_MEM_PER_NODE:-n/a} mem_per_cpu=${SLURM_MEM_PER_CPU:-n/a}"
fi
echo "[task] dask_scheduler=$ERA5_DASK_SCHEDULER dask_num_workers=$ERA5_DASK_NUM_WORKERS write_time_block=$ERA5_WRITE_TIME_BLOCK"
echo "[task] command: ${cmd[*]}"

if [[ "$dry_run" -eq 1 ]]; then
  exit 0
fi

"${cmd[@]}"
