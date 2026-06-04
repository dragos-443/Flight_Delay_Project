#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${SCRIPT_DIR}/run_spark_analysis.sh" analysis_3_1 spark_sql all
"${SCRIPT_DIR}/run_spark_analysis.sh" analysis_3_1 spark_core all
"${SCRIPT_DIR}/run_hive_analysis.sh" analysis_3_1 all
"${SCRIPT_DIR}/run_spark_analysis.sh" analysis_3_2 spark_sql all
"${SCRIPT_DIR}/run_spark_analysis.sh" analysis_3_2 spark_core all
"${SCRIPT_DIR}/run_hive_analysis.sh" analysis_3_2 all

"${SCRIPT_DIR}/run_spark_analysis.sh" analysis_3_1 spark_sql scale_all
"${SCRIPT_DIR}/run_spark_analysis.sh" analysis_3_1 spark_core scale_all
"${SCRIPT_DIR}/run_hive_analysis.sh" analysis_3_1 scale_all
"${SCRIPT_DIR}/run_spark_analysis.sh" analysis_3_2 spark_sql scale_all
"${SCRIPT_DIR}/run_spark_analysis.sh" analysis_3_2 spark_core scale_all
"${SCRIPT_DIR}/run_hive_analysis.sh" analysis_3_2 scale_all
