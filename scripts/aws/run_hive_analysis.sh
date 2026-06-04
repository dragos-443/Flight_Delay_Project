#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Uso: $0 <analysis_3_1|analysis_3_2> <run_size|all|scale_all>" >&2
  exit 1
fi

ANALYSIS="$1"
REQUESTED_RUN_SIZE="$2"
TECHNOLOGY="hive"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/emr_common.sh"

case "$ANALYSIS" in
  analysis_3_1|analysis_3_2) ;;
  *)
    echo "Analisi non supportata: ${ANALYSIS}" >&2
    exit 1
    ;;
esac

if [[ "$REQUESTED_RUN_SIZE" == "all" ]]; then
  aws s3 rm "${AWS_BENCHMARKS_ROOT}/${ANALYSIS}/${TECHNOLOGY}/timings.csv" >/dev/null 2>&1 || true
elif [[ "$REQUESTED_RUN_SIZE" == "scale_all" ]]; then
  aws s3 rm "${AWS_BENCHMARKS_ROOT}/scalability/${ANALYSIS}/${TECHNOLOGY}/timings.csv" >/dev/null 2>&1 || true
fi

create_hive_sql() {
  local analysis="$1"
  local input_path="$2"
  local output_table="$3"
  local parquet_output="$4"
  local csv_output="$5"

  if [[ "$analysis" == "analysis_3_1" ]]; then
    cat <<SQL
CREATE DATABASE IF NOT EXISTS flight_delay;
USE flight_delay;
DROP TABLE IF EXISTS flights_clean;
CREATE EXTERNAL TABLE flights_clean (
  year INT,
  month INT,
  day_of_month INT,
  day_of_week INT,
  fl_date DATE,
  op_unique_carrier STRING,
  op_carrier_fl_num INT,
  origin STRING,
  origin_city_name STRING,
  origin_state_nm STRING,
  dest STRING,
  dest_city_name STRING,
  dest_state_nm STRING,
  route STRING,
  dep_delay DOUBLE,
  arr_delay DOUBLE,
  cancelled INT,
  is_cancelled INT,
  cancellation_code STRING,
  distance DOUBLE,
  carrier_delay DOUBLE,
  weather_delay DOUBLE,
  nas_delay DOUBLE,
  security_delay DOUBLE,
  late_aircraft_delay DOUBLE,
  primary_disruption_cause STRING,
  departure_delay_band STRING
)
STORED AS PARQUET
LOCATION '${input_path}';

DROP TABLE IF EXISTS ${output_table};
CREATE TABLE ${output_table}
STORED AS PARQUET
LOCATION '${parquet_output}'
AS
SELECT
  op_unique_carrier,
  route,
  COUNT(*) AS flight_count,
  MIN(arr_delay) AS min_arr_delay,
  MAX(arr_delay) AS max_arr_delay,
  ROUND(AVG(arr_delay), 2) AS avg_arr_delay,
  ROUND(AVG(is_cancelled), 4) AS cancellation_rate,
  regexp_replace(
    concat_ws(',', sort_array(collect_set(lpad(CAST(month AS STRING), 2, '0')))),
    '(^|,)0',
    '\$1'
  ) AS operating_months
FROM flights_clean
GROUP BY op_unique_carrier, route;

INSERT OVERWRITE DIRECTORY '${csv_output}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
  op_unique_carrier,
  route,
  flight_count,
  min_arr_delay,
  max_arr_delay,
  avg_arr_delay,
  cancellation_rate,
  operating_months
FROM ${output_table}
ORDER BY op_unique_carrier, route;

SELECT COUNT(*) AS output_rows FROM ${output_table};
SELECT *
FROM ${output_table}
ORDER BY op_unique_carrier, route
LIMIT 10;
SQL
  else
    cat <<SQL
CREATE DATABASE IF NOT EXISTS flight_delay;
USE flight_delay;
DROP TABLE IF EXISTS flights_clean;
CREATE EXTERNAL TABLE flights_clean (
  year INT,
  month INT,
  day_of_month INT,
  day_of_week INT,
  fl_date DATE,
  op_unique_carrier STRING,
  op_carrier_fl_num INT,
  origin STRING,
  origin_city_name STRING,
  origin_state_nm STRING,
  dest STRING,
  dest_city_name STRING,
  dest_state_nm STRING,
  route STRING,
  dep_delay DOUBLE,
  arr_delay DOUBLE,
  cancelled INT,
  is_cancelled INT,
  cancellation_code STRING,
  distance DOUBLE,
  carrier_delay DOUBLE,
  weather_delay DOUBLE,
  nas_delay DOUBLE,
  security_delay DOUBLE,
  late_aircraft_delay DOUBLE,
  primary_disruption_cause STRING,
  departure_delay_band STRING
)
STORED AS PARQUET
LOCATION '${input_path}';

DROP TABLE IF EXISTS ${output_table};
CREATE TABLE ${output_table}
STORED AS PARQUET
LOCATION '${parquet_output}'
AS
WITH banded_flights AS (
  SELECT
    origin,
    month,
    departure_delay_band,
    dep_delay,
    arr_delay,
    primary_disruption_cause
  FROM flights_clean
  WHERE departure_delay_band IN ('low', 'medium', 'high')
),
band_metrics AS (
  SELECT
    origin,
    month,
    departure_delay_band,
    COUNT(*) AS flight_count,
    ROUND(AVG(dep_delay), 2) AS avg_dep_delay,
    ROUND(AVG(arr_delay), 2) AS avg_arr_delay
  FROM banded_flights
  GROUP BY origin, month, departure_delay_band
),
cause_counts AS (
  SELECT
    origin,
    month,
    departure_delay_band,
    primary_disruption_cause,
    COUNT(*) AS cause_count
  FROM banded_flights
  WHERE primary_disruption_cause IS NOT NULL
    AND primary_disruption_cause != 'none'
  GROUP BY origin, month, departure_delay_band, primary_disruption_cause
),
ranked_causes AS (
  SELECT
    origin,
    month,
    departure_delay_band,
    primary_disruption_cause,
    cause_count,
    ROW_NUMBER() OVER (
      PARTITION BY origin, month, departure_delay_band
      ORDER BY cause_count DESC, primary_disruption_cause
    ) AS cause_rank
  FROM cause_counts
),
top_causes AS (
  SELECT
    origin,
    month,
    departure_delay_band,
    concat_ws(
      ',',
      max(CASE WHEN cause_rank = 1 THEN concat(primary_disruption_cause, ':', CAST(cause_count AS STRING)) END),
      max(CASE WHEN cause_rank = 2 THEN concat(primary_disruption_cause, ':', CAST(cause_count AS STRING)) END),
      max(CASE WHEN cause_rank = 3 THEN concat(primary_disruption_cause, ':', CAST(cause_count AS STRING)) END)
    ) AS top_3_causes
  FROM ranked_causes
  WHERE cause_rank <= 3
  GROUP BY origin, month, departure_delay_band
)
SELECT
  band_metrics.origin,
  band_metrics.month,
  band_metrics.departure_delay_band,
  band_metrics.flight_count,
  band_metrics.avg_dep_delay,
  band_metrics.avg_arr_delay,
  COALESCE(NULLIF(top_causes.top_3_causes, ''), 'none') AS top_3_causes
FROM band_metrics
LEFT JOIN top_causes
  ON band_metrics.origin = top_causes.origin
 AND band_metrics.month = top_causes.month
 AND band_metrics.departure_delay_band = top_causes.departure_delay_band;

INSERT OVERWRITE DIRECTORY '${csv_output}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
  origin,
  month,
  departure_delay_band,
  flight_count,
  avg_dep_delay,
  avg_arr_delay,
  top_3_causes
FROM ${output_table}
ORDER BY
  origin,
  month,
  CASE departure_delay_band
    WHEN 'low' THEN 1
    WHEN 'medium' THEN 2
    WHEN 'high' THEN 3
    ELSE 4
  END;

SELECT COUNT(*) AS output_rows FROM ${output_table};
SELECT *
FROM ${output_table}
ORDER BY
  origin,
  month,
  CASE departure_delay_band
    WHEN 'low' THEN 1
    WHEN 'medium' THEN 2
    WHEN 'high' THEN 3
    ELSE 4
  END
LIMIT 10;
SQL
  fi
}

get_hive_count() {
  local output_table="$1"
  hive -S -e "USE flight_delay; SELECT COUNT(*) FROM ${output_table};" 2>/dev/null | grep -E '^[0-9]+$' | tail -n 1
}

while IFS= read -r RUN_SIZE; do
  INPUT_PATH="$(resolve_input_path "$RUN_SIZE")"
  SAFE_RUN_SIZE="${RUN_SIZE//[^A-Za-z0-9_]/_}"

  if is_scale_run_size "$RUN_SIZE"; then
    OUTPUT_ROOT="${AWS_OUTPUTS_ROOT}/scalability/${ANALYSIS}/${TECHNOLOGY}"
    TIMING_PATH="${AWS_BENCHMARKS_ROOT}/scalability/${ANALYSIS}/${TECHNOLOGY}/timings.csv"
  else
    OUTPUT_ROOT="${AWS_OUTPUTS_ROOT}/${ANALYSIS}/${TECHNOLOGY}"
    TIMING_PATH="${AWS_BENCHMARKS_ROOT}/${ANALYSIS}/${TECHNOLOGY}/timings.csv"
  fi

  PARQUET_OUTPUT="${OUTPUT_ROOT}/${RUN_SIZE}/parquet"
  CSV_OUTPUT="${OUTPUT_ROOT}/${RUN_SIZE}/csv"
  OUTPUT_TABLE="${ANALYSIS}_${TECHNOLOGY}_${SAFE_RUN_SIZE}"
  SQL_FILE="$(mktemp)"
  LOCAL_LOG="$(mktemp)"
  LOG_PATH="${AWS_LOGS_ROOT}/${ANALYSIS}/${TECHNOLOGY}/${RUN_SIZE}_$(date -u '+%Y%m%dT%H%M%SZ').log"
  RUN_TIMESTAMP="$(current_timestamp_utc)"

  echo "Eseguo ${ANALYSIS} Hive (${RUN_SIZE})"
  echo "Input: ${INPUT_PATH}"
  echo "Output: ${OUTPUT_ROOT}/${RUN_SIZE}"

  aws s3 rm "$PARQUET_OUTPUT" --recursive >/dev/null 2>&1 || true
  aws s3 rm "$CSV_OUTPUT" --recursive >/dev/null 2>&1 || true
  create_hive_sql "$ANALYSIS" "$INPUT_PATH" "$OUTPUT_TABLE" "$PARQUET_OUTPUT" "$CSV_OUTPUT" > "$SQL_FILE"

  START_TIME="$(timer_now)"
  set +e
  hive -f "$SQL_FILE" 2>&1 | tee "$LOCAL_LOG"
  EXIT_CODE="${PIPESTATUS[0]}"
  set -e
  ELAPSED_SECONDS="$(timer_elapsed "$START_TIME")"
  aws s3 cp "$LOCAL_LOG" "$LOG_PATH" >/dev/null

  if [[ "$EXIT_CODE" -ne 0 ]]; then
    echo "Job Hive fallito. Log: ${LOG_PATH}" >&2
    rm -f "$SQL_FILE" "$LOCAL_LOG"
    exit "$EXIT_CODE"
  fi

  OUTPUT_ROWS="$(get_hive_count "$OUTPUT_TABLE")"
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

  rm -f "$SQL_FILE" "$LOCAL_LOG"
  echo "Run completata: ${RUN_SIZE}, secondi=${ELAPSED_SECONDS}, output_rows=${OUTPUT_ROWS}"
  echo "Timing CSV: ${TIMING_PATH}"
done < <(resolve_run_sizes "$REQUESTED_RUN_SIZE")
