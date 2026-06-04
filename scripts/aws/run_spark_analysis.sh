#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "Uso: $0 <analysis_3_1|analysis_3_2> <spark_sql|spark_core> <run_size|all|scale_all>" >&2
  exit 1
fi

ANALYSIS="$1"
TECHNOLOGY="$2"
REQUESTED_RUN_SIZE="$3"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/emr_common.sh"

case "${ANALYSIS}:${TECHNOLOGY}" in
  analysis_3_1:spark_sql) PY_SCRIPT="analysis_3_1_spark_sql.py" ;;
  analysis_3_1:spark_core) PY_SCRIPT="analysis_3_1_spark_core.py" ;;
  analysis_3_2:spark_sql) PY_SCRIPT="analysis_3_2_spark_sql.py" ;;
  analysis_3_2:spark_core) PY_SCRIPT="analysis_3_2_spark_core.py" ;;
  *)
    echo "Combinazione non supportata: ${ANALYSIS} ${TECHNOLOGY}" >&2
    exit 1
    ;;
esac

if [[ "$REQUESTED_RUN_SIZE" == "all" ]]; then
  aws s3 rm "${AWS_BENCHMARKS_ROOT}/${ANALYSIS}/${TECHNOLOGY}/timings.csv" >/dev/null 2>&1 || true
elif [[ "$REQUESTED_RUN_SIZE" == "scale_all" ]]; then
  aws s3 rm "${AWS_BENCHMARKS_ROOT}/scalability/${ANALYSIS}/${TECHNOLOGY}/timings.csv" >/dev/null 2>&1 || true
fi

while IFS= read -r RUN_SIZE; do
  INPUT_PATH="$(resolve_input_path "$RUN_SIZE")"
  if is_scale_run_size "$RUN_SIZE"; then
    OUTPUT_ROOT="${AWS_OUTPUTS_ROOT}/scalability/${ANALYSIS}/${TECHNOLOGY}"
    TIMING_PATH="${AWS_BENCHMARKS_ROOT}/scalability/${ANALYSIS}/${TECHNOLOGY}/timings.csv"
  else
    OUTPUT_ROOT="${AWS_OUTPUTS_ROOT}/${ANALYSIS}/${TECHNOLOGY}"
    TIMING_PATH="${AWS_BENCHMARKS_ROOT}/${ANALYSIS}/${TECHNOLOGY}/timings.csv"
  fi

  PARQUET_OUTPUT="${OUTPUT_ROOT}/${RUN_SIZE}/parquet"
  CSV_OUTPUT="${OUTPUT_ROOT}/${RUN_SIZE}/csv"
  LOG_PATH="${AWS_LOGS_ROOT}/${ANALYSIS}/${TECHNOLOGY}/${RUN_SIZE}_$(date -u '+%Y%m%dT%H%M%SZ').log"
  LOCAL_LOG="$(mktemp)"
  RUN_TIMESTAMP="$(current_timestamp_utc)"

  echo "Eseguo ${ANALYSIS} ${TECHNOLOGY} (${RUN_SIZE})"
  echo "Input: ${INPUT_PATH}"
  echo "Output: ${OUTPUT_ROOT}/${RUN_SIZE}"

  START_TIME="$(timer_now)"
  set +e
  spark-submit \
    --master "$SPARK_MASTER" \
    --deploy-mode "$SPARK_DEPLOY_MODE" \
    "${SRC_ROOT}/${PY_SCRIPT}" \
    --input "$INPUT_PATH" \
    --parquet-output "$PARQUET_OUTPUT" \
    --csv-output "$CSV_OUTPUT" \
    --run-size "$RUN_SIZE" 2>&1 | tee "$LOCAL_LOG"
  EXIT_CODE="${PIPESTATUS[0]}"
  set -e
  ELAPSED_SECONDS="$(timer_elapsed "$START_TIME")"
  aws s3 cp "$LOCAL_LOG" "$LOG_PATH" >/dev/null

  if [[ "$EXIT_CODE" -ne 0 ]]; then
    echo "Job fallito. Log: ${LOG_PATH}" >&2
    rm -f "$LOCAL_LOG"
    exit "$EXIT_CODE"
  fi

  OUTPUT_ROWS="$(grep -E '^output_rows=' "$LOCAL_LOG" | tail -n 1 | cut -d '=' -f 2)"
  OUTPUT_ROWS="${OUTPUT_ROWS:-0}"

  append_timing_row \
    "$TIMING_PATH" \
    "$ANALYSIS" \
    "$TECHNOLOGY" \
    "$RUN_SIZE" \
    "$INPUT_PATH" \
    "${OUTPUT_ROOT}/${RUN_SIZE}" \
    "$ELAPSED_SECONDS" \
    "$OUTPUT_ROWS" \
    "$RUN_TIMESTAMP"

  rm -f "$LOCAL_LOG"
  echo "Run completata: ${RUN_SIZE}, secondi=${ELAPSED_SECONDS}, output_rows=${OUTPUT_ROWS}"
  echo "Timing CSV: ${TIMING_PATH}"
done < <(resolve_run_sizes "$REQUESTED_RUN_SIZE")
