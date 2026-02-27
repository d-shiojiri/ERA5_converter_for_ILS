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
submit_config="${ERA5_SUBMIT_PIPELINE_CONFIG:-$REPO_ROOT/configs/submit_pipeline_default.yaml}"

input_root="$(yaml_get "$prep_config" "input_root" "/data02/shiojiri/DATA/ERA5/download/reanalysis-era5-single-levels/")"
canonical_root="$(yaml_get "$convert_config" "canonical_root" "$(yaml_get "$prep_config" "output_root" "/data02/shiojiri/ILS/ILS_data/ERA5/era5_canonical")")"
output_root="$(yaml_get "$convert_config" "output_root" "/data02/shiojiri/ILS/ILS_data/ERA5/out")"
method="$(yaml_get "$convert_config" "method" "conservative")"
progress="$(yaml_get "$convert_config" "progress" "$(yaml_get "$prep_config" "progress" "auto")")"

executor="$(yaml_get "$submit_config" "executor" "sbatch")"
log_dir="$(yaml_get "$submit_config" "log_dir" "/data02/shiojiri/ILS/ILS_data/ERA5/logs")"
max_parallel_jobs="$(yaml_get "$submit_config" "max_parallel_jobs" "$(yaml_get "$submit_config" "local_parallel" "1")")"
partition="$(yaml_get "$submit_config" "partition" "")"
cpus="$(yaml_get "$submit_config" "cpus" "1")"
mem="$(yaml_get "$submit_config" "mem" "8G")"
time_limit="$(yaml_get "$submit_config" "time" "02:00:00")"
auto_adjust_cpus_for_mem="$(yaml_get "$submit_config" "auto_adjust_cpus_for_mem" "true")"
assumed_mem_per_cpu_gb="$(yaml_get "$submit_config" "assumed_mem_per_cpu_gb" "4")"
vars_default_csv="$(yaml_get "$submit_config" "vars" "Tair,Qair,PSurf,Wind,SWdown,LWdown,Precip,Rainf,Snowf,CCover")"
clean_old_logs="$(yaml_get "$submit_config" "clean_old_logs" "true")"

usage() {
  cat <<USAGE
Usage:
  submit_year_var_pipeline_jobs.sh --years <LIST|RANGE> [options]

Submits/runs year-variable pipeline jobs where each job executes:
  Program A (prep) -> Program B (convert)

Required:
  --years             "2000,2001,2002" or "2000-2005"

Optional overrides:
  --vars              comma-separated target vars (default from $submit_config)
  --executor          sbatch or local (default from $submit_config)
  --input-root        raw ERA5 root (default from $prep_config)
  --canonical-root    Program A canonical ERA5 output root (default from $convert_config)
  --stage1-root       deprecated alias of --canonical-root
  --output-root       Program B output root (default from $convert_config)
  --method            Program B regrid method (default from $convert_config)
  --progress          auto|none|tqdm|dask (default from configs)
  --log-dir           default from $submit_config
  --max-parallel-jobs max concurrent jobs (local worker count / sbatch dependency cap)
  --local-parallel    deprecated alias of --max-parallel-jobs
  --partition         sbatch partition (default from $submit_config)
  --cpus              sbatch cpus-per-task (default from $submit_config)
  --mem               sbatch mem (default from $submit_config)
  --time              sbatch time (default from $submit_config)
  auto_adjust_cpus_for_mem  YAML key in $submit_config (true/false)
  assumed_mem_per_cpu_gb    YAML key in $submit_config (e.g., 4)
  clean_old_logs      YAML key in $submit_config (true/false)
  --dry-run           print commands only
  --help              show this help

Config files (env override):
  ERA5_PREP_CONFIG=$prep_config
  ERA5_CONVERT_CONFIG=$convert_config
  ERA5_SUBMIT_PIPELINE_CONFIG=$submit_config
USAGE
}

years_arg=""
vars_arg=""
dry_run=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --years) years_arg="$2"; shift 2 ;;
    --vars) vars_arg="$2"; shift 2 ;;
    --executor) executor="$2"; shift 2 ;;
    --input-root) input_root="$2"; shift 2 ;;
    --canonical-root|--stage1-root) canonical_root="$2"; shift 2 ;;
    --output-root) output_root="$2"; shift 2 ;;
    --method) method="$2"; shift 2 ;;
    --progress) progress="$2"; shift 2 ;;
    --log-dir) log_dir="$2"; shift 2 ;;
    --max-parallel-jobs|--local-parallel) max_parallel_jobs="$2"; shift 2 ;;
    --partition) partition="$2"; shift 2 ;;
    --cpus) cpus="$2"; shift 2 ;;
    --mem) mem="$2"; shift 2 ;;
    --time) time_limit="$2"; shift 2 ;;
    --dry-run) dry_run=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "$years_arg" ]]; then
  echo "Missing required arg: --years" >&2
  usage
  exit 2
fi
if [[ "$executor" != "sbatch" && "$executor" != "local" ]]; then
  echo "--executor must be sbatch or local: $executor" >&2
  exit 2
fi
if [[ "$executor" == "sbatch" ]] && ! command -v sbatch >/dev/null 2>&1; then
  echo "sbatch command not found. Use --executor local or install Slurm client." >&2
  exit 2
fi
if ! is_positive_integer "$max_parallel_jobs"; then
  echo "--max-parallel-jobs must be a positive integer: $max_parallel_jobs" >&2
  exit 2
fi
if ! is_positive_integer "$cpus"; then
  echo "--cpus must be a positive integer: $cpus" >&2
  exit 2
fi
if [[ "$executor" == "sbatch" ]] && is_truthy "$auto_adjust_cpus_for_mem"; then
  if ! is_positive_integer "$assumed_mem_per_cpu_gb"; then
    echo "assumed_mem_per_cpu_gb must be a positive integer: $assumed_mem_per_cpu_gb" >&2
    exit 2
  fi
  if req_mem_mib="$(mem_to_mib "$mem")"; then
    per_cpu_mib=$((assumed_mem_per_cpu_gb * 1024))
    min_cpus_for_mem=$(((req_mem_mib + per_cpu_mib - 1) / per_cpu_mib))
    if (( cpus < min_cpus_for_mem )); then
      echo "[pipeline-submit] cpus=$cpus is too small for mem=$mem under ${assumed_mem_per_cpu_gb}G/cpu policy; auto-adjusting cpus to $min_cpus_for_mem"
      cpus="$min_cpus_for_mem"
    fi
  else
    echo "[pipeline-submit] warning: failed to parse mem value '$mem'; skipping cpu auto-adjust" >&2
  fi
fi

worker_script="$SCRIPT_DIR/run_year_var_pipeline_task.sh"
mapfile -t years < <(parse_years "$years_arg")

if [[ -n "$vars_arg" ]]; then
  mapfile -t vars < <(csv_to_array "$vars_arg")
else
  mapfile -t vars < <(csv_to_array "$vars_default_csv")
fi

mkdir -p "$log_dir"

echo "[pipeline-submit] executor=$executor years=${#years[@]} vars=${#vars[@]} max_parallel_jobs=$max_parallel_jobs cpus=$cpus mem=$mem"

running_local=0
submitted_job_ids=()
task_index=0
for year in "${years[@]}"; do
  for var_name in "${vars[@]}"; do
    job_name="era5_pipeline_${year}_${var_name}"
    if [[ "$dry_run" -eq 0 ]] && is_truthy "$clean_old_logs"; then
      cleanup_job_logs "$log_dir" "$job_name"
    fi
    task_cmd=(
      "$worker_script"
      --year "$year"
      --var "$var_name"
      --input-root "$input_root"
      --canonical-root "$canonical_root"
      --output-root "$output_root"
      --method "$method"
      --progress "$progress"
    )

    if [[ "$executor" == "sbatch" ]]; then
      dep_args=()
      if (( task_index >= max_parallel_jobs )); then
        dep_id="${submitted_job_ids[$((task_index - max_parallel_jobs))]}"
        dep_args=(--dependency "afterany:${dep_id}")
      fi
      submit_cmd=(
        sbatch
        --parsable
        --export=ALL
        --job-name "$job_name"
        --cpus-per-task "$cpus"
        --mem "$mem"
        --time "$time_limit"
        --output "$log_dir/${job_name}.%j.out"
      )
      if [[ -n "$partition" ]]; then
        submit_cmd+=(--partition "$partition")
      fi
      if [[ "${#dep_args[@]}" -gt 0 ]]; then
        submit_cmd+=("${dep_args[@]}")
      fi
      submit_cmd+=("${task_cmd[@]}")

      if [[ "$dry_run" -eq 1 ]]; then
        echo "[dry-run] ${submit_cmd[*]}"
        submitted_job_ids+=("DRYRUN_${task_index}")
      else
        submit_out="$("${submit_cmd[@]}")"
        job_id="${submit_out%%;*}"
        if ! [[ "$job_id" =~ ^[0-9]+$ ]]; then
          echo "[pipeline-submit] failed to parse sbatch job id: $submit_out" >&2
          exit 2
        fi
        submitted_job_ids+=("$job_id")
        echo "[pipeline-submit] submitted job_id=$job_id name=$job_name"
      fi
      task_index=$((task_index + 1))
    else
      if [[ "$dry_run" -eq 1 ]]; then
        echo "[dry-run] ${task_cmd[*]}"
      else
        "${task_cmd[@]}" >"$log_dir/${job_name}.local.out" 2>&1 &
        running_local=$((running_local + 1))
        if (( running_local >= max_parallel_jobs )); then
          wait -n
          running_local=$((running_local - 1))
        fi
      fi
    fi
  done
done

if [[ "$executor" == "local" && "$dry_run" -eq 0 ]]; then
  wait
fi

echo "[pipeline-submit] done"
