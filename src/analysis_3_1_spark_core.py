from argparse import ArgumentParser
from decimal import Decimal, ROUND_HALF_UP

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


VALID_RUN_SIZES = {"100k", "500k", "half", "full"}


RESULT_SCHEMA = StructType(
    [
        StructField("op_unique_carrier", StringType(), False),
        StructField("route", StringType(), False),
        StructField("flight_count", LongType(), False),
        StructField("min_arr_delay", DoubleType(), True),
        StructField("max_arr_delay", DoubleType(), True),
        StructField("avg_arr_delay", DoubleType(), True),
        StructField("cancellation_rate", DoubleType(), True),
        StructField("operating_months", StringType(), False),
    ]
)


def parse_args():
    parser = ArgumentParser(description="Run analysis 3.1 with Spark Core RDDs.")
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


def spark_round_or_none(value, digits):
    if value is None:
        return None

    quantizer = Decimal("1").scaleb(-digits)
    return float(Decimal(str(value)).quantize(quantizer, rounding=ROUND_HALF_UP))


def build_initial_stats(row):
    arr_delay = row.arr_delay
    arr_count = 1 if arr_delay is not None else 0
    return {
        "flight_count": 1,
        "min_arr_delay": arr_delay,
        "max_arr_delay": arr_delay,
        "arr_delay_sum": arr_delay if arr_delay is not None else 0.0,
        "arr_delay_count": arr_count,
        "cancelled_sum": float(row.is_cancelled or 0),
        "months": {row.month},
    }


def merge_stats(left, right):
    min_values = [
        value for value in [left["min_arr_delay"], right["min_arr_delay"]] if value is not None
    ]
    max_values = [
        value for value in [left["max_arr_delay"], right["max_arr_delay"]] if value is not None
    ]

    return {
        "flight_count": left["flight_count"] + right["flight_count"],
        "min_arr_delay": min(min_values) if min_values else None,
        "max_arr_delay": max(max_values) if max_values else None,
        "arr_delay_sum": left["arr_delay_sum"] + right["arr_delay_sum"],
        "arr_delay_count": left["arr_delay_count"] + right["arr_delay_count"],
        "cancelled_sum": left["cancelled_sum"] + right["cancelled_sum"],
        "months": left["months"] | right["months"],
    }


def stats_to_row(item):
    (carrier, route), stats = item
    avg_arr_delay = None
    if stats["arr_delay_count"] > 0:
        avg_arr_delay = stats["arr_delay_sum"] / stats["arr_delay_count"]

    return Row(
        op_unique_carrier=carrier,
        route=route,
        flight_count=int(stats["flight_count"]),
        min_arr_delay=stats["min_arr_delay"],
        max_arr_delay=stats["max_arr_delay"],
        avg_arr_delay=spark_round_or_none(avg_arr_delay, 2),
        cancellation_rate=spark_round_or_none(
            stats["cancelled_sum"] / stats["flight_count"], 4
        ),
        operating_months=",".join(str(month) for month in sorted(stats["months"])),
    )


def main():
    args = parse_args()
    spark = (
        SparkSession.builder.appName("FlightDelayAnalysis31SparkCore")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    analysis_input_df = spark.read.parquet(args.input)

    result_rdd = (
        analysis_input_df.rdd.map(
            lambda row: ((row.op_unique_carrier, row.route), build_initial_stats(row))
        )
        .reduceByKey(merge_stats)
        .map(stats_to_row)
        .sortBy(lambda row: (row.op_unique_carrier, row.route))
    )
    result_df = spark.createDataFrame(result_rdd, RESULT_SCHEMA).cache()

    output_rows = result_df.count()
    result_df.write.mode("overwrite").parquet(args.parquet_output)
    result_df.write.mode("overwrite").option("header", "true").csv(args.csv_output)

    print("=== Flight Delay analysis 3.1 Spark Core metrics ===")
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
