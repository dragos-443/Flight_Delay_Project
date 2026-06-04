#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/emr_common.sh"

LOCAL_OUTPUT_ROOT="${1:-outputs/benchmarks/aws}"

mkdir -p "$LOCAL_OUTPUT_ROOT"
aws s3 sync "${AWS_BENCHMARKS_ROOT}" "$LOCAL_OUTPUT_ROOT"

echo "Benchmark AWS scaricati in ${LOCAL_OUTPUT_ROOT}"
