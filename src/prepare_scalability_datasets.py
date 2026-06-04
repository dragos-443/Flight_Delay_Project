from argparse import ArgumentParser

from pyspark.sql import SparkSession


SCALE_FACTORS = {
    "1x": 1,
    "2x": 2,
    "4x": 4,
}


def parse_args():
    parser = ArgumentParser(description="Prepare materialized scalability datasets.")
    parser.add_argument("--input", required=True, help="Clean Parquet input path in HDFS.")
    parser.add_argument("--output-root", required=True, help="Scaled dataset output root path in HDFS.")
    return parser.parse_args()


def build_scaled_dataset(clean_df, factor: int):
    scaled_df = clean_df
    for _ in range(factor - 1):
        scaled_df = scaled_df.unionByName(clean_df)
    return scaled_df


def main():
    args = parse_args()
    spark = (
        SparkSession.builder.appName("FlightDelayPrepareScalabilityDatasets")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    clean_df = spark.read.parquet(args.input).cache()
    total_rows = clean_df.count()

    print("=== Flight Delay scalability dataset preparation ===")
    print(f"input={args.input}")
    print(f"output_root={args.output_root}")
    print(f"base_rows={total_rows}")

    for scale_name, factor in SCALE_FACTORS.items():
        output_path = f"{args.output_root}/flights_clean_{scale_name}.parquet"
        scaled_df = build_scaled_dataset(clean_df, factor)
        scaled_rows = scaled_df.count()
        scaled_df.write.mode("overwrite").parquet(output_path)
        print(
            f"scale={scale_name}, factor={factor}, rows={scaled_rows}, "
            f"expected_rows={total_rows * factor}, output={output_path}"
        )

    spark.stop()


if __name__ == "__main__":
    main()
