#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

trim() {
  local s="$*"
  s="${s#${s%%[![:space:]]*}}"
  s="${s%${s##*[![:space:]]}}"
  printf '%s' "$s"
}

yaml_get() {
  local file="$1"
  local key="$2"
  local default_value="${3:-}"

  if [[ ! -f "$file" ]]; then
    printf '%s' "$default_value"
    return 0
  fi

  local value
  value=$(awk -v key="$key" '
    {
      line=$0
      sub(/#.*/, "", line)
      if (line ~ "^[[:space:]]*" key "[[:space:]]*:") {
        sub("^[[:space:]]*" key "[[:space:]]*:[[:space:]]*", "", line)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", line)
        print line
        exit
      }
    }
  ' "$file")

  value="$(trim "$value")"
  if [[ -z "$value" ]]; then
    printf '%s' "$default_value"
    return 0
  fi

  if [[ "${value:0:1}" == '"' && "${value: -1}" == '"' ]]; then
    value="${value:1:${#value}-2}"
  elif [[ "${value:0:1}" == "'" && "${value: -1}" == "'" ]]; then
    value="${value:1:${#value}-2}"
  fi

  printf '%s' "$value"
}

parse_years() {
  local years_arg="$1"
  if [[ "$years_arg" == *"-"* ]]; then
    local start="${years_arg%-*}"
    local end="${years_arg#*-}"
    if ! [[ "$start" =~ ^[0-9]{4}$ && "$end" =~ ^[0-9]{4}$ && "$start" -le "$end" ]]; then
      echo "Invalid --years range: $years_arg" >&2
      return 1
    fi
    local y
    for ((y=start; y<=end; y++)); do
      echo "$y"
    done
  else
    IFS=',' read -r -a arr <<<"$years_arg"
    local y
    for y in "${arr[@]}"; do
      y="$(trim "$y")"
      if ! [[ "$y" =~ ^[0-9]{4}$ ]]; then
        echo "Invalid year: $y" >&2
        return 1
      fi
      echo "$y"
    done
  fi
}

csv_to_array() {
  local csv="$1"
  IFS=',' read -r -a _tmp <<<"$csv"
  local x
  for x in "${_tmp[@]}"; do
    x="$(trim "$x")"
    if [[ -n "$x" ]]; then
      echo "$x"
    fi
  done
}

is_truthy() {
  local v="${1:-}"
  v="$(printf '%s' "$v" | tr '[:upper:]' '[:lower:]')"
  case "$v" in
    1|true|yes|y|on) return 0 ;;
    *) return 1 ;;
  esac
}

cleanup_job_logs() {
  local log_dir="$1"
  local job_name="$2"
  find "$log_dir" -maxdepth 1 -type f \
    \( -name "${job_name}.*.out" -o -name "${job_name}.local.out" \) \
    -delete
}

is_positive_integer() {
  local v="${1:-}"
  [[ "$v" =~ ^[1-9][0-9]*$ ]]
}

mem_to_mib() {
  local raw="${1:-}"
  raw="$(trim "$raw")"
  if [[ -z "$raw" ]]; then
    return 1
  fi

  local num unit
  if [[ "$raw" =~ ^([0-9]+)([[:alpha:]]*)$ ]]; then
    num="${BASH_REMATCH[1]}"
    unit="${BASH_REMATCH[2]}"
  else
    return 1
  fi

  unit="$(printf '%s' "$unit" | tr '[:lower:]' '[:upper:]')"
  case "$unit" in
    ""|M|MB|MIB) echo "$num" ;;
    G|GB|GIB) echo $((num * 1024)) ;;
    T|TB|TIB) echo $((num * 1024 * 1024)) ;;
    K|KB|KIB) echo $((num / 1024)) ;;
    *) return 1 ;;
  esac
}
