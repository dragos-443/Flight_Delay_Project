#!/usr/bin/env bash
set -euo pipefail

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "Variabile richiesta non impostata: $name" >&2
    exit 1
  fi
}

require_env S3_BUCKET

S3_PREFIX="${S3_PREFIX:-flight-delay-project}"
S3_PREFIX="${S3_PREFIX#/}"
S3_PREFIX="${S3_PREFIX%/}"

if [[ -n "$S3_PREFIX" ]]; then
  AWS_ROOT="s3://${S3_BUCKET}/${S3_PREFIX}"
else
  AWS_ROOT="s3://${S3_BUCKET}"
fi

RAW_CSV_PATH="${RAW_CSV_PATH:-${AWS_ROOT}/raw/flight_data_2024.csv}"
PROCESSED_PARQUET_PATH="${PROCESSED_PARQUET_PATH:-${AWS_ROOT}/processed/flights_2024_clean.parquet}"
PROCESSED_CSV_PATH="${PROCESSED_CSV_PATH:-${AWS_ROOT}/processed/flights_2024_clean_csv}"
SAMPLES_ROOT="${SAMPLES_ROOT:-${AWS_ROOT}/samples}"
SCALED_ROOT="${SCALED_ROOT:-${AWS_ROOT}/scaled}"
AWS_OUTPUTS_ROOT="${AWS_OUTPUTS_ROOT:-${AWS_ROOT}/outputs/aws}"
AWS_BENCHMARKS_ROOT="${AWS_BENCHMARKS_ROOT:-${AWS_ROOT}/benchmarks/aws}"
AWS_LOGS_ROOT="${AWS_LOGS_ROOT:-${AWS_ROOT}/logs}"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
SRC_ROOT="${SRC_ROOT:-${PROJECT_ROOT}/src}"
SPARK_MASTER="${SPARK_MASTER:-yarn}"
SPARK_DEPLOY_MODE="${SPARK_DEPLOY_MODE:-client}"
ENVIRONMENT_LABEL="${ENVIRONMENT_LABEL:-aws_emr}"

csv_escape() {
  local value="${1:-}"
  value="${value//\"/\"\"}"
  printf '"%s"' "$value"
}

append_timing_row() {
  local timing_path="$1"
  local analysis="$2"
  local technology="$3"
  local run_size="$4"
  local input_path="$5"
  local output_path="$6"
  local execution_time_seconds="$7"
  local output_rows="$8"
  local run_timestamp="$9"

  local tmp_file
  tmp_file="$(mktemp)"

  if ! aws s3 cp "$timing_path" "$tmp_file" >/dev/null 2>&1; then
    printf 'analysis,technology,run_size,input_path,output_path,execution_time_seconds,output_rows,run_timestamp,environment\n' > "$tmp_file"
  fi

  {
    csv_escape "$analysis"; printf ','
    csv_escape "$technology"; printf ','
    csv_escape "$run_size"; printf ','
    csv_escape "$input_path"; printf ','
    csv_escape "$output_path"; printf ','
    printf '%s,' "$execution_time_seconds"
    printf '%s,' "$output_rows"
    csv_escape "$run_timestamp"; printf ','
    csv_escape "$ENVIRONMENT_LABEL"; printf '\n'
  } >> "$tmp_file"

  aws s3 cp "$tmp_file" "$timing_path" >/dev/null
  rm -f "$tmp_file"
}

resolve_run_sizes() {
  local run_size="$1"
  case "$run_size" in
    all) printf '%s\n' 100k 500k half full ;;
    scale_all) printf '%s\n' 1x 2x 4x ;;
    100k|500k|half|full|1x|2x|4x) printf '%s\n' "$run_size" ;;
    *)
      echo "Run size non supportata: $run_size" >&2
      exit 1
      ;;
  esac
}

is_scale_run_size() {
  case "$1" in
    1x|2x|4x) return 0 ;;
    *) return 1 ;;
  esac
}

resolve_input_path() {
  local run_size="$1"
  if is_scale_run_size "$run_size"; then
    printf '%s/flights_clean_%s.parquet\n' "$SCALED_ROOT" "$run_size"
  else
    printf '%s/flights_clean_%s.parquet\n' "$SAMPLES_ROOT" "$run_size"
  fi
}

current_timestamp_utc() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

timer_now() {
  python3 -c 'import time; print(time.time())'
}

timer_elapsed() {
  local start="$1"
  python3 - "$start" <<'PY'
import sys
import time

print(f"{time.time() - float(sys.argv[1]):.3f}")
PY
}
