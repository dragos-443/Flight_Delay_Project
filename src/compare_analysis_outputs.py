from argparse import ArgumentParser

from pyspark.sql import SparkSession


def parse_args():
    parser = ArgumentParser(description="Compare two Parquet analysis outputs.")
    parser.add_argument("--left", required=True, help="First Parquet output path.")
    parser.add_argument("--right", required=True, help="Second Parquet output path.")
    parser.add_argument("--left-label", default="left", help="Label for the first output.")
    parser.add_argument("--right-label", default="right", help="Label for the second output.")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = (
        SparkSession.builder.appName("FlightDelayCompareAnalysisOutputs")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    left_df = spark.read.parquet(args.left)
    right_df = spark.read.parquet(args.right)

    left_rows = left_df.count()
    right_rows = right_df.count()
    left_only_rows = left_df.exceptAll(right_df).count()
    right_only_rows = right_df.exceptAll(left_df).count()
    matching = left_only_rows == 0 and right_only_rows == 0

    print("=== Flight Delay analysis output comparison ===")
    print(f"left_label={args.left_label}")
    print(f"right_label={args.right_label}")
    print(f"left_path={args.left}")
    print(f"right_path={args.right}")
    print(f"left_rows={left_rows}")
    print(f"right_rows={right_rows}")
    print(f"left_only_rows={left_only_rows}")
    print(f"right_only_rows={right_only_rows}")
    print(f"matching={str(matching).lower()}")

    if not matching:
        raise RuntimeError(
            f"Outputs differ: {args.left_label} has {left_only_rows} unmatched rows, "
            f"{args.right_label} has {right_only_rows} unmatched rows."
        )

    spark.stop()


if __name__ == "__main__":
    main()
