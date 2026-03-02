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

canonical_root="$(yaml_get "$convert_config" "canonical_root" "$(yaml_get "$prep_config" "output_root" "/data02/shiojiri/ILS/ILS_data/ERA5/era5_canonical")")"
output_root="$(yaml_get "$convert_config" "output_root" "/data02/shiojiri/ILS/ILS_data/ERA5/30min")"

dry_run=0

usage() {
  cat <<USAGE
Usage:
  cleanup_incomplete_tmp_files.sh [options]

Delete only in-progress temporary files (*.tmp*) under Program A/B outputs.
No rebuild jobs are submitted by this script.

Options:
  --canonical-root  Program A canonical ERA5 output root
  --output-root     Program B output root
  --dry-run         print targets only
  --help            show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --canonical-root|--stage1-root) canonical_root="$2"; shift 2 ;;
    --output-root) output_root="$2"; shift 2 ;;
    --executor|--log-dir|--max-parallel-jobs|--partition|--cpus|--mem|--time|--include-missing-outputs|--method|--progress|--input-root)
      echo "[rebuild-clean] ignore option: $1=$2"
      shift 2
      ;;
    --dry-run) dry_run=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

scan_roots=()
if [[ -d "$canonical_root" ]]; then
  scan_roots+=("$canonical_root")
fi
if [[ -d "$output_root" ]]; then
  scan_roots+=("$output_root")
fi

if (( ${#scan_roots[@]} == 0 )); then
  echo "[rebuild-clean] no scan root exists: canonical_root=$canonical_root output_root=$output_root"
  exit 0
fi

mapfile -d '' -t tmp_files < <(find "${scan_roots[@]}" -type f -name "*.tmp*" -print0)
count=${#tmp_files[@]}

echo "[rebuild-clean] canonical_root=$canonical_root output_root=$output_root"
echo "[rebuild-clean] found tmp files=$count"

if (( count == 0 )); then
  echo "[rebuild-clean] nothing to delete"
  exit 0
fi

if [[ "$dry_run" -eq 1 ]]; then
  for f in "${tmp_files[@]}"; do
    echo "[dry-run][delete] $f"
  done
  echo "[rebuild-clean] dry-run done"
  exit 0
fi

for f in "${tmp_files[@]}"; do
  rm -f "$f"
  echo "[rebuild-clean][delete] $f"
done

echo "[rebuild-clean] done"
echo "[rebuild-clean] next: run ./scripts/submit_year_var_pipeline_jobs.sh to generate missing outputs"
