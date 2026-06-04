from __future__ import annotations

import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
AWS_BENCHMARKS_DIR = ROOT / "outputs" / "benchmarks" / "aws"
AWS_BENCHMARK_SUMMARY_PATH = ROOT / "outputs" / "benchmarks" / "aws_benchmark_summary.csv"
AWS_SCALABILITY_SUMMARY_PATH = ROOT / "outputs" / "benchmarks" / "aws_scalability_summary.csv"

RUN_SIZE_ORDER = ["100k", "500k", "half", "full"]
SCALE_ORDER = ["1x", "2x", "4x"]
ANALYSIS_ORDER = ["analysis_3_1", "analysis_3_2"]
TECHNOLOGY_ORDER = ["spark_sql", "spark_core", "hive"]


def read_rows(pattern: str, allowed_run_sizes: list[str]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in AWS_BENCHMARKS_DIR.glob(pattern):
        with path.open(newline="", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                if row.get("run_size") in allowed_run_sizes:
                    rows.append(row)

    return sorted(
        rows,
        key=lambda row: (
            ANALYSIS_ORDER.index(row["analysis"]),
            allowed_run_sizes.index(row["run_size"]),
            TECHNOLOGY_ORDER.index(row["technology"]),
        ),
    )


def write_summary(rows: list[dict[str, str]], output_path: Path) -> None:
    if not rows:
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "analysis",
        "technology",
        "run_size",
        "input_path",
        "output_path",
        "execution_time_seconds",
        "output_rows",
        "run_timestamp",
        "environment",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    benchmark_rows = read_rows("analysis_3_*/**/timings.csv", RUN_SIZE_ORDER)
    scalability_rows = read_rows("scalability/analysis_3_*/**/timings.csv", SCALE_ORDER)

    write_summary(benchmark_rows, AWS_BENCHMARK_SUMMARY_PATH)
    write_summary(scalability_rows, AWS_SCALABILITY_SUMMARY_PATH)

    print("=== AWS benchmark summary ===")
    print(f"benchmark_rows={len(benchmark_rows)}")
    print(f"benchmark_summary={AWS_BENCHMARK_SUMMARY_PATH}")
    print(f"scalability_rows={len(scalability_rows)}")
    print(f"scalability_summary={AWS_SCALABILITY_SUMMARY_PATH}")


if __name__ == "__main__":
    main()
