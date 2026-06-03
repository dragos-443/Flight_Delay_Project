from argparse import ArgumentParser

from pyspark.sql import SparkSession


VALID_RUN_SIZES = {"100k", "500k", "half", "full"}


def parse_args():
    parser = ArgumentParser(description="Run analysis 3.1 with Spark SQL.")
    parser.add_argument("--input", required=True, help="Clean Parquet input path in HDFS.")
    parser.add_argument("--parquet-output", required=True, help="Parquet output path in HDFS.")
    parser.add_argument("--csv-output", required=True, help="CSV output path in HDFS.")
    parser.add_argument(
        "--run-size",
        choices=sorted(VALID_RUN_SIZES),
        default="full",
        help="Dataset size to process.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = (
        SparkSession.builder.appName("FlightDelayAnalysis31SparkSQL")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    clean_df = spark.read.parquet(args.input)
    clean_df.createOrReplaceTempView("flights_clean")

    result_df = spark.sql(
        """
        SELECT
            op_unique_carrier,
            route,
            COUNT(*) AS flight_count,
            MIN(arr_delay) AS min_arr_delay,
            MAX(arr_delay) AS max_arr_delay,
            ROUND(AVG(arr_delay), 2) AS avg_arr_delay,
            ROUND(AVG(is_cancelled), 4) AS cancellation_rate,
            concat_ws(
                ',',
                transform(
                    sort_array(collect_set(month)),
                    month_value -> CAST(month_value AS STRING)
                )
            ) AS operating_months
        FROM flights_clean
        GROUP BY op_unique_carrier, route
        ORDER BY op_unique_carrier, route
        """
    ).cache()

    output_rows = result_df.count()
    result_df.write.mode("overwrite").parquet(args.parquet_output)
    result_df.write.mode("overwrite").option("header", "true").csv(args.csv_output)

    print("=== Flight Delay analysis 3.1 Spark SQL metrics ===")
    print(f"run_size={args.run_size}")
    print(f"input={args.input}")
    print(f"parquet_output={args.parquet_output}")
    print(f"csv_output={args.csv_output}")
    print(f"output_rows={output_rows}")
    print("top_10_rows_start")
    result_df.show(10, truncate=False)
    print("top_10_rows_end")

    spark.stop()


if __name__ == "__main__":
    main()
