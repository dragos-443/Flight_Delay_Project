from argparse import ArgumentParser

from pyspark.sql import SparkSession


VALID_RUN_SIZES = {"100k", "500k", "half", "full"}


def parse_args():
    parser = ArgumentParser(description="Run analysis 3.2 with Spark SQL.")
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
        SparkSession.builder.appName("FlightDelayAnalysis32SparkSQL")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    clean_df = spark.read.parquet(args.input)
    clean_df.createOrReplaceTempView("flights_clean")

    result_df = spark.sql(
        """
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
                    transform(
                        sort_array(
                            collect_list(
                                named_struct(
                                    'cause_rank', cause_rank,
                                    'cause_label',
                                    concat(primary_disruption_cause, ':', CAST(cause_count AS STRING))
                                )
                            )
                        ),
                        cause -> cause.cause_label
                    )
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
            COALESCE(top_causes.top_3_causes, 'none') AS top_3_causes
        FROM band_metrics
        LEFT JOIN top_causes
          ON band_metrics.origin = top_causes.origin
         AND band_metrics.month = top_causes.month
         AND band_metrics.departure_delay_band = top_causes.departure_delay_band
        ORDER BY
            band_metrics.origin,
            band_metrics.month,
            CASE band_metrics.departure_delay_band
                WHEN 'low' THEN 1
                WHEN 'medium' THEN 2
                WHEN 'high' THEN 3
                ELSE 4
            END
        """
    ).cache()

    output_rows = result_df.count()
    result_df.write.mode("overwrite").parquet(args.parquet_output)
    result_df.write.mode("overwrite").option("header", "true").csv(args.csv_output)

    print("=== Flight Delay analysis 3.2 Spark SQL metrics ===")
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
