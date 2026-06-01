from argparse import ArgumentParser
from decimal import Decimal, ROUND_HALF_UP

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


VALID_RUN_SIZES = {"100k", "500k", "half", "full"}
VALID_DELAY_BANDS = {"low", "medium", "high"}
DELAY_BAND_ORDER = {"low": 1, "medium": 2, "high": 3}


RESULT_SCHEMA = StructType(
    [
        StructField("origin", StringType(), False),
        StructField("month", IntegerType(), False),
        StructField("departure_delay_band", StringType(), False),
        StructField("flight_count", LongType(), False),
        StructField("avg_dep_delay", DoubleType(), True),
        StructField("avg_arr_delay", DoubleType(), True),
        StructField("top_3_causes", StringType(), False),
    ]
)


def parse_args():
    parser = ArgumentParser(description="Run analysis 3.2 with Spark Core RDDs.")
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


def apply_run_size(df, run_size):
    if run_size == "full":
        return df

    if run_size == "100k":
        return df.limit(100_000)

    if run_size == "500k":
        return df.limit(500_000)

    total_rows = df.count()
    return df.limit(total_rows // 2)


def spark_round_or_none(value, digits):
    if value is None:
        return None

    quantizer = Decimal("1").scaleb(-digits)
    return float(Decimal(str(value)).quantize(quantizer, rounding=ROUND_HALF_UP))


def build_initial_band_stats(row):
    dep_delay = row.dep_delay
    arr_delay = row.arr_delay
    return {
        "flight_count": 1,
        "dep_delay_sum": dep_delay if dep_delay is not None else 0.0,
        "dep_delay_count": 1 if dep_delay is not None else 0,
        "arr_delay_sum": arr_delay if arr_delay is not None else 0.0,
        "arr_delay_count": 1 if arr_delay is not None else 0,
    }


def merge_band_stats(left, right):
    return {
        "flight_count": left["flight_count"] + right["flight_count"],
        "dep_delay_sum": left["dep_delay_sum"] + right["dep_delay_sum"],
        "dep_delay_count": left["dep_delay_count"] + right["dep_delay_count"],
        "arr_delay_sum": left["arr_delay_sum"] + right["arr_delay_sum"],
        "arr_delay_count": left["arr_delay_count"] + right["arr_delay_count"],
    }


def build_top_causes(cause_counts):
    top_causes = sorted(cause_counts, key=lambda item: (-item[1], item[0]))[:3]
    if not top_causes:
        return "none"
    return ",".join(f"{cause}:{count}" for cause, count in top_causes)


def metrics_to_row(item):
    key, (stats, cause_counts) = item
    origin, month, delay_band = key

    avg_dep_delay = None
    if stats["dep_delay_count"] > 0:
        avg_dep_delay = stats["dep_delay_sum"] / stats["dep_delay_count"]

    avg_arr_delay = None
    if stats["arr_delay_count"] > 0:
        avg_arr_delay = stats["arr_delay_sum"] / stats["arr_delay_count"]

    return Row(
        origin=origin,
        month=int(month),
        departure_delay_band=delay_band,
        flight_count=int(stats["flight_count"]),
        avg_dep_delay=spark_round_or_none(avg_dep_delay, 2),
        avg_arr_delay=spark_round_or_none(avg_arr_delay, 2),
        top_3_causes=build_top_causes(cause_counts or []),
    )


def main():
    args = parse_args()
    spark = (
        SparkSession.builder.appName("FlightDelayAnalysis32SparkCore")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    clean_df = spark.read.parquet(args.input)
    analysis_input_df = apply_run_size(clean_df, args.run_size)
    banded_rdd = analysis_input_df.rdd.filter(
        lambda row: row.departure_delay_band in VALID_DELAY_BANDS
    ).cache()

    band_metrics_rdd = (
        banded_rdd.map(
            lambda row: (
                (row.origin, row.month, row.departure_delay_band),
                build_initial_band_stats(row),
            )
        )
        .reduceByKey(merge_band_stats)
    )

    cause_counts_rdd = (
        banded_rdd.filter(
            lambda row: row.primary_disruption_cause is not None
            and row.primary_disruption_cause != "none"
        )
        .map(
            lambda row: (
                (
                    row.origin,
                    row.month,
                    row.departure_delay_band,
                    row.primary_disruption_cause,
                ),
                1,
            )
        )
        .reduceByKey(lambda left, right: left + right)
        .map(lambda item: (item[0][:3], (item[0][3], item[1])))
        .groupByKey()
        .mapValues(list)
    )

    result_rdd = (
        band_metrics_rdd.leftOuterJoin(cause_counts_rdd)
        .map(metrics_to_row)
        .sortBy(
            lambda row: (
                row.origin,
                row.month,
                DELAY_BAND_ORDER.get(row.departure_delay_band, 4),
            )
        )
    )
    result_df = spark.createDataFrame(result_rdd, RESULT_SCHEMA).cache()

    output_rows = result_df.count()
    result_df.write.mode("overwrite").parquet(args.parquet_output)
    result_df.write.mode("overwrite").option("header", "true").csv(args.csv_output)

    print("=== Flight Delay analysis 3.2 Spark Core metrics ===")
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
