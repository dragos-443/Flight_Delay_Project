#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/emr_common.sh"

LOCAL_DATASET_PATH="${1:-data/raw/flight_data_2024.csv}"

echo "Upload codice su ${AWS_ROOT}"
aws s3 sync "${PROJECT_ROOT}/src" "${AWS_ROOT}/src" --delete
aws s3 sync "${PROJECT_ROOT}/scripts/aws" "${AWS_ROOT}/scripts/aws" --delete

if [[ -f "$LOCAL_DATASET_PATH" ]]; then
  echo "Upload dataset raw: ${LOCAL_DATASET_PATH} -> ${RAW_CSV_PATH}"
  aws s3 cp "$LOCAL_DATASET_PATH" "$RAW_CSV_PATH"
else
  echo "Dataset non trovato in ${LOCAL_DATASET_PATH}; salto upload raw."
fi

echo "Upload completato."
echo "Sul nodo master EMR:"
echo "  export S3_BUCKET=${S3_BUCKET}"
echo "  export S3_PREFIX=${S3_PREFIX}"
echo "  aws s3 sync ${AWS_ROOT}/src ./src"
echo "  aws s3 sync ${AWS_ROOT}/scripts/aws ./scripts/aws"
echo "  chmod +x scripts/aws/*.sh"
