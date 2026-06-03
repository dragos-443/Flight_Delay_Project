from argparse import ArgumentParser

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SAMPLE_SIZES = {
    "100k": 100_000,
    "500k": 500_000,
}

ORDER_COLUMNS = [
    "fl_date",
    "op_unique_carrier",
    "op_carrier_fl_num",
    "origin",
    "dest",
    "dep_delay",
    "arr_delay",
    "cancelled",
    "route",
]


def parse_args():
    parser = ArgumentParser(description="Prepare deterministic benchmark samples.")
    parser.add_argument("--input", required=True, help="Clean Parquet input path in HDFS.")
    parser.add_argument("--output-root", required=True, help="Sample output root path in HDFS.")
    return parser.parse_args()


def build_row_hash(df):
    hash_columns = [
        F.coalesce(F.col(column_name).cast("string"), F.lit("__NULL__"))
        for column_name in df.columns
    ]
    return F.sha2(F.concat_ws("||", *hash_columns), 256)


def main():
    args = parse_args()
    spark = (
        SparkSession.builder.appName("FlightDelayPrepareBenchmarkSamples")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    clean_df = spark.read.parquet(args.input)
    total_rows = clean_df.count()
    sorted_df = (
        clean_df.withColumn("_benchmark_row_hash", build_row_hash(clean_df))
        .orderBy(*[F.col(column_name).asc_nulls_last() for column_name in ORDER_COLUMNS], F.col("_benchmark_row_hash"))
        .drop("_benchmark_row_hash")
        .cache()
    )

    sample_targets = {
        "100k": SAMPLE_SIZES["100k"],
        "500k": SAMPLE_SIZES["500k"],
        "half": total_rows // 2,
        "full": total_rows,
    }

    print("=== Flight Delay benchmark sample preparation ===")
    print(f"input={args.input}")
    print(f"output_root={args.output_root}")
    print(f"total_rows={total_rows}")

    for sample_name, target_rows in sample_targets.items():
        output_path = f"{args.output_root}/flights_clean_{sample_name}.parquet"
        sample_df = sorted_df if sample_name == "full" else sorted_df.limit(target_rows)
        sample_rows = sample_df.count()
        sample_df.write.mode("overwrite").parquet(output_path)
        print(f"sample={sample_name}, target_rows={target_rows}, rows={sample_rows}, output={output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
