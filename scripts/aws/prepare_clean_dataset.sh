#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/emr_common.sh"

spark-submit \
  --master "$SPARK_MASTER" \
  --deploy-mode "$SPARK_DEPLOY_MODE" \
  "${SRC_ROOT}/prepare_clean_dataset.py" \
  --input "$RAW_CSV_PATH" \
  --parquet-output "$PROCESSED_PARQUET_PATH" \
  --csv-output "$PROCESSED_CSV_PATH"
