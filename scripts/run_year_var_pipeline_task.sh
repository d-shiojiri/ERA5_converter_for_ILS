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
method="$(yaml_get "$convert_config" "method" "conservative")"
progress="$(yaml_get "$convert_config" "progress" "$(yaml_get "$prep_config" "progress" "auto")")"

usage() {
  cat <<USAGE
Usage:
  run_year_var_pipeline_task.sh --year <YYYY> --var <TARGET_VAR> [options]

Runs Program A -> Program B sequentially for one year-variable.

Required:
  --year            target year
  --var             target variable (Tair,Qair,PSurf,Wind,SWdown,LWdown,Precip,Rainf,Snowf,CCover)

Optional overrides:
  --input-root      raw ERA5 root (default from $prep_config)
  --canonical-root  Program A canonical ERA5 output root (default from $convert_config)
  --stage1-root     deprecated alias of --canonical-root
  --output-root     Program B output root (default from $convert_config)
  --method          Program B regrid method (default from $convert_config)
  --progress        auto|none|tqdm|dask (default from configs)
  --dry-run         print commands only
USAGE
}

year=""
var_name=""
dry_run=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --year) year="$2"; shift 2 ;;
    --var) var_name="$2"; shift 2 ;;
    --input-root) input_root="$2"; shift 2 ;;
    --canonical-root|--stage1-root) canonical_root="$2"; shift 2 ;;
    --output-root) output_root="$2"; shift 2 ;;
    --method) method="$2"; shift 2 ;;
    --progress) progress="$2"; shift 2 ;;
    --dry-run) dry_run=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "$year" || -z "$var_name" ]]; then
  echo "Missing required args: --year and --var" >&2
  usage
  exit 2
fi

worker_script="$SCRIPT_DIR/run_year_var_task.sh"

stage1_path="$canonical_root/$year/ERA5.${var_name}.${year}.ILS.nc"
output_path="$output_root/$year/ERA5.30min.${var_name}.1hrMap.ILS.${year}.nc"

need_prep=1
need_convert=1
if [[ -f "$stage1_path" ]]; then
  need_prep=0
fi
if [[ -f "$output_path" ]]; then
  need_convert=0
fi

prep_cmd=(
  "$worker_script"
  --stage prep
  --year "$year"
  --var "$var_name"
  --input-root "$input_root"
  --canonical-root "$canonical_root"
  --output-root "$output_root"
  --progress "$progress"
)

convert_cmd=(
  "$worker_script"
  --stage convert
  --year "$year"
  --var "$var_name"
  --input-root "$input_root"
  --canonical-root "$canonical_root"
  --output-root "$output_root"
  --method "$method"
  --progress "$progress"
)

echo "[pipeline] year=$year var=$var_name"
if (( need_prep == 1 )); then
  echo "[pipeline] prep: ${prep_cmd[*]}"
else
  echo "[pipeline] prep: skip (exists: $stage1_path)"
fi
if (( need_convert == 1 )); then
  echo "[pipeline] convert: ${convert_cmd[*]}"
else
  echo "[pipeline] convert: skip (exists: $output_path)"
fi

if (( need_prep == 0 && need_convert == 0 )); then
  echo "[pipeline] done year=$year var=$var_name (already complete)"
  exit 0
fi

if [[ "$dry_run" -eq 1 ]]; then
  exit 0
fi

if (( need_prep == 1 )); then
  "${prep_cmd[@]}"
fi
if (( need_convert == 1 )); then
  "${convert_cmd[@]}"
fi

echo "[pipeline] done year=$year var=$var_name"
