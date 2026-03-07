#!/usr/bin/env bash
set -euo pipefail

PF_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$PF_SCRIPT_DIR/.." && pwd)"
COMMON_SH="$REPO_ROOT/scripts/_common.sh"

if [[ ! -f "$COMMON_SH" ]]; then
  echo "Failed to locate common helper: $COMMON_SH" >&2
  exit 2
fi
source "$COMMON_SH"

DEFAULT_CONFIG="$PF_SCRIPT_DIR/configs/submit_perturb_era5_30min_members_default.yaml"
DEFAULT_PREP_CONFIG="$REPO_ROOT/configs/prep_default.yaml"
LINK_SCRIPT="$PF_SCRIPT_DIR/helpers/link_unperturbed_era5_30min_files.py"
config_file="$DEFAULT_CONFIG"
dry_run=0

load_defaults_from_config() {
  years="$(yaml_get "$config_file" "years" "")"
  vars_csv="$(yaml_get "$config_file" "vars" "Rainf,Snowf,SWdown,LWdown")"
  members_spec="$(yaml_get "$config_file" "members" "1-20")"
  input_root="$(yaml_get "$config_file" "input_root" "/data02/shiojiri/ILS/ILS_data/ERA5/30min")"
  output_root="$(yaml_get "$config_file" "output_root" "$PF_SCRIPT_DIR/ERA5_ptb_30min")"
  random_file="$(yaml_get "$config_file" "random_file" "$PF_SCRIPT_DIR/continuous_random_19000101_20260101.nc")"
  time_chunk="$(yaml_get "$config_file" "time_chunk" "48")"
  executor="$(yaml_get "$config_file" "executor" "sbatch")"
  max_parallel_jobs="$(yaml_get "$config_file" "max_parallel_jobs" "8")"
  cpus="$(yaml_get "$config_file" "cpus" "1")"
  mem="$(yaml_get "$config_file" "mem" "auto")"
  time_limit="$(yaml_get "$config_file" "time" "06:00:00")"
  partition="$(yaml_get "$config_file" "partition" "")"
  log_dir="$(yaml_get "$config_file" "log_dir" "$PF_SCRIPT_DIR/ERA5_ptb_30min/logs")"
  overwrite_raw="$(yaml_get "$config_file" "overwrite" "false")"
  worker_script="$(yaml_get "$config_file" "worker_script" "workers/perturb_era5_30min_members.py")"
  python_cmd="$(yaml_get "$config_file" "python_cmd" "$(yaml_get "$DEFAULT_PREP_CONFIG" "python_cmd" "python3")")"
}

usage() {
  cat <<EOF
Usage:
  submit_perturb_era5_30min_members_jobs.sh [options]

Reads defaults from YAML config and submits sbatch jobs for year x variable tasks.
Only sbatch submission is supported by this script.

Options:
  --config <path>            YAML config path (default: $DEFAULT_CONFIG)
  --years <LIST|RANGE>       e.g. 2013-2022 or 2013,2014
  --vars <csv>               e.g. Rainf,Snowf,SWdown,LWdown
  --members <LIST|RANGE>     e.g. 1-20 or 1,2,5
  --input-root <path>
  --output-root <path>
  --random-file <path>
  --time-chunk <int>
  --max-parallel-jobs <int>
  --cpus <int>
  --mem <str>                e.g. 24G, auto, none
  --time <HH:MM:SS>
  --partition <name>
  --log-dir <path>
  --worker-script <path>     relative paths are resolved from perturbed_forcing/
  --python-cmd <command>     command used to run worker script
  --overwrite
  --no-overwrite
  --dry-run
  --help
EOF
}

parse_members() {
  local spec="$1"
  local token start end m
  local -a out=()
  IFS=',' read -r -a toks <<<"$spec"
  for token in "${toks[@]}"; do
    token="$(trim "$token")"
    [[ -z "$token" ]] && continue
    if [[ "$token" == *"-"* ]]; then
      start="${token%-*}"
      end="${token#*-}"
      if ! [[ "$start" =~ ^[0-9]+$ && "$end" =~ ^[0-9]+$ && "$start" -le "$end" ]]; then
        echo "Invalid member range token: $token" >&2
        return 1
      fi
      for ((m=start; m<=end; m++)); do
        out+=("$m")
      done
    else
      if ! [[ "$token" =~ ^[0-9]+$ ]]; then
        echo "Invalid member token: $token" >&2
        return 1
      fi
      out+=("$token")
    fi
  done
  if [[ "${#out[@]}" -eq 0 ]]; then
    echo "No members parsed from: $spec" >&2
    return 1
  fi
  printf '%s\n' "${out[@]}" | awk '!seen[$0]++'
}

all_outputs_exist_for_task() {
  local year="$1"
  local var_name="$2"
  local member
  for member in "${members_list[@]}"; do
    local out_path="$output_root/$year/member_$(printf '%02d' "$member")/ERA5.30min.${var_name}.1hrMap.ILS.${year}.nc"
    if [[ ! -f "$out_path" ]]; then
      return 1
    fi
  done
  return 0
}

load_defaults_from_config

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      config_file="$2"
      load_defaults_from_config
      shift 2
      ;;
    --years) years="$2"; shift 2 ;;
    --vars) vars_csv="$2"; shift 2 ;;
    --members) members_spec="$2"; shift 2 ;;
    --input-root) input_root="$2"; shift 2 ;;
    --output-root) output_root="$2"; shift 2 ;;
    --random-file) random_file="$2"; shift 2 ;;
    --time-chunk) time_chunk="$2"; shift 2 ;;
    --max-parallel-jobs) max_parallel_jobs="$2"; shift 2 ;;
    --cpus) cpus="$2"; shift 2 ;;
    --mem) mem="$2"; shift 2 ;;
    --time) time_limit="$2"; shift 2 ;;
    --partition) partition="$2"; shift 2 ;;
    --log-dir) log_dir="$2"; shift 2 ;;
    --worker-script) worker_script="$2"; shift 2 ;;
    --python-cmd) python_cmd="$2"; shift 2 ;;
    --overwrite) overwrite_raw="true"; shift ;;
    --no-overwrite) overwrite_raw="false"; shift ;;
    --dry-run) dry_run=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "${years:-}" ]]; then
  echo "Missing years. Set 'years' in config or pass --years." >&2
  exit 2
fi

if [[ "${executor,,}" != "sbatch" ]]; then
  echo "This script supports only sbatch. Set executor: sbatch in config." >&2
  exit 2
fi

if ! command -v sbatch >/dev/null 2>&1; then
  echo "sbatch command not found." >&2
  exit 2
fi

if ! is_positive_integer "$max_parallel_jobs"; then
  echo "--max-parallel-jobs must be positive: $max_parallel_jobs" >&2
  exit 2
fi
if ! is_positive_integer "$cpus"; then
  echo "--cpus must be positive: $cpus" >&2
  exit 2
fi
if ! is_positive_integer "$time_chunk"; then
  echo "--time-chunk must be positive: $time_chunk" >&2
  exit 2
fi

overwrite=0
if is_truthy "$overwrite_raw"; then
  overwrite=1
fi

mem_lc="$(printf '%s' "$mem" | tr '[:upper:]' '[:lower:]')"
if [[ "$mem_lc" == "auto" || "$mem_lc" == "none" || "$mem_lc" == "unset" ]]; then
  mem=""
fi

if [[ "$worker_script" != /* ]]; then
  worker_script="$PF_SCRIPT_DIR/$worker_script"
fi
if [[ ! -x "$worker_script" ]]; then
  if [[ -f "$worker_script" ]]; then
    chmod +x "$worker_script"
  else
    echo "Worker script not found: $worker_script" >&2
    exit 2
  fi
fi

python_cmd="$(trim "$python_cmd")"
if [[ -z "$python_cmd" ]]; then
  echo "python_cmd is empty. Set python_cmd in config or pass --python-cmd." >&2
  exit 2
fi
read -r -a python_cmd_parts <<<"$python_cmd"
if [[ "${#python_cmd_parts[@]}" -eq 0 ]]; then
  echo "Failed to parse python_cmd: $python_cmd" >&2
  exit 2
fi

if [[ ! -f "$random_file" ]]; then
  echo "Random file not found: $random_file" >&2
  exit 2
fi
if [[ ! -f "$LINK_SCRIPT" ]]; then
  echo "Link helper script not found: $LINK_SCRIPT" >&2
  exit 2
fi
if [[ ! -x "$LINK_SCRIPT" ]]; then
  chmod +x "$LINK_SCRIPT"
fi

mapfile -t years_list < <(parse_years "$years")
mapfile -t vars_list < <(csv_to_array "$vars_csv")
mapfile -t members_list < <(parse_members "$members_spec")

if [[ "${#vars_list[@]}" -eq 0 ]]; then
  echo "No vars parsed from: $vars_csv" >&2
  exit 2
fi
if [[ "${#members_list[@]}" -eq 0 ]]; then
  echo "No members parsed from: $members_spec" >&2
  exit 2
fi

mkdir -p "$log_dir"

submitted_job_ids=()
task_index=0
scheduled=0
skipped=0
for year in "${years_list[@]}"; do
  for var_name in "${vars_list[@]}"; do
    if [[ "$overwrite" -eq 0 ]] && all_outputs_exist_for_task "$year" "$var_name"; then
      echo "[submit][skip] year=$year var=$var_name all outputs exist"
      skipped=$((skipped + 1))
      continue
    fi

    job_name="ptb30_${year}_${var_name}"
    dep_args=()
    if (( task_index >= max_parallel_jobs )); then
      dep_id="${submitted_job_ids[$((task_index - max_parallel_jobs))]}"
      dep_args=(--dependency "afterany:${dep_id}")
    fi

    task_cmd=(
      "${python_cmd_parts[@]}"
      "$worker_script"
      --year "$year"
      --var "$var_name"
      --members "$members_spec"
      --input-root "$input_root"
      --output-root "$output_root"
      --random-file "$random_file"
      --time-chunk "$time_chunk"
    )
    if [[ "$overwrite" -eq 1 ]]; then
      task_cmd+=(--overwrite)
    fi
    wrapped_task_cmd="$(printf '%q ' "${task_cmd[@]}")"
    wrapped_task_cmd="${wrapped_task_cmd% }"

    submit_cmd=(
      sbatch
      --parsable
      --export=ALL
      --job-name "$job_name"
      --cpus-per-task "$cpus"
      --time "$time_limit"
      --output "$log_dir/${job_name}.%j.out"
    )
    if [[ -n "$mem" ]]; then
      submit_cmd+=(--mem "$mem")
    fi
    if [[ -n "$partition" ]]; then
      submit_cmd+=(--partition "$partition")
    fi
    if [[ "${#dep_args[@]}" -gt 0 ]]; then
      submit_cmd+=("${dep_args[@]}")
    fi
    submit_cmd+=(--wrap "$wrapped_task_cmd")

    if [[ "$dry_run" -eq 1 ]]; then
      echo "[dry-run] ${submit_cmd[*]}"
      submitted_job_ids+=("DRYRUN_${task_index}")
    else
      submit_out="$("${submit_cmd[@]}")"
      job_id="${submit_out%%;*}"
      if ! [[ "$job_id" =~ ^[0-9]+$ ]]; then
        echo "Failed to parse sbatch job id: $submit_out" >&2
        exit 2
      fi
      submitted_job_ids+=("$job_id")
      echo "[submit] submitted job_id=$job_id name=$job_name"
    fi

    scheduled=$((scheduled + 1))
    task_index=$((task_index + 1))
  done
done

echo "[submit] done scheduled=$scheduled skipped_existing=$skipped"

link_cmd=(
  "$LINK_SCRIPT"
  --years "$years"
  --members "$members_spec"
  --input-root "$input_root"
  --output-root "$output_root"
)
if [[ "$overwrite" -eq 1 ]]; then
  link_cmd+=(--overwrite)
fi
if [[ "$dry_run" -eq 1 ]]; then
  link_cmd+=(--dry-run --verbose)
fi

echo "[submit] creating links for unperturbed files in member directories"
"${link_cmd[@]}"
