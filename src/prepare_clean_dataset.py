from argparse import ArgumentParser

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DELAY_CAUSE_COLUMNS = [
    ("carrier_delay", "carrier"),
    ("weather_delay", "weather"),
    ("nas_delay", "nas"),
    ("security_delay", "security"),
    ("late_aircraft_delay", "late_aircraft"),
]

OUTPUT_COLUMNS = [
    "year",
    "month",
    "day_of_month",
    "day_of_week",
    "fl_date",
    "op_unique_carrier",
    "op_carrier_fl_num",
    "origin",
    "origin_city_name",
    "origin_state_nm",
    "dest",
    "dest_city_name",
    "dest_state_nm",
    "route",
    "dep_delay",
    "arr_delay",
    "cancelled",
    "is_cancelled",
    "cancellation_code",
    "distance",
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay",
    "primary_disruption_cause",
    "departure_delay_band",
]


def parse_args():
    parser = ArgumentParser(description="Prepare the common clean flight dataset.")
    parser.add_argument("--input", required=True, help="Raw CSV input path in HDFS.")
    parser.add_argument("--parquet-output", required=True, help="Clean Parquet output path in HDFS.")
    parser.add_argument("--csv-output", required=True, help="Clean CSV output directory in HDFS.")
    return parser.parse_args()


def normalize_text(column_name):
    return F.upper(F.trim(F.col(column_name)))


def build_primary_disruption_cause():
    max_delay = F.greatest(
        *[F.coalesce(F.col(column_name), F.lit(0.0)) for column_name, _ in DELAY_CAUSE_COLUMNS]
    )

    delayed_cause = F.lit("none")
    for column_name, cause_name in reversed(DELAY_CAUSE_COLUMNS):
        delayed_cause = F.when(
            (F.coalesce(F.col(column_name), F.lit(0.0)) == max_delay) & (max_delay > F.lit(0.0)),
            F.lit(cause_name),
        ).otherwise(delayed_cause)

    cancellation_cause = (
        F.when(F.col("cancellation_code") == "A", F.lit("carrier"))
        .when(F.col("cancellation_code") == "B", F.lit("weather"))
        .when(F.col("cancellation_code") == "C", F.lit("nas"))
        .when(F.col("cancellation_code") == "D", F.lit("security"))
        .otherwise(F.lit("cancellation_unknown"))
    )

    return F.when(F.col("cancelled") == 1, cancellation_cause).otherwise(delayed_cause)


def build_filtered_dataset(raw_df):
    typed_df = (
        raw_df.select(
            F.col("year").cast("int").alias("year"),
            F.col("month").cast("int").alias("month"),
            F.col("day_of_month").cast("int").alias("day_of_month"),
            F.col("day_of_week").cast("int").alias("day_of_week"),
            F.to_date(F.col("fl_date"), "yyyy-MM-dd").alias("fl_date"),
            normalize_text("op_unique_carrier").alias("op_unique_carrier"),
            F.col("op_carrier_fl_num").cast("double").cast("int").alias("op_carrier_fl_num"),
            normalize_text("origin").alias("origin"),
            F.trim(F.col("origin_city_name")).alias("origin_city_name"),
            F.trim(F.col("origin_state_nm")).alias("origin_state_nm"),
            normalize_text("dest").alias("dest"),
            F.trim(F.col("dest_city_name")).alias("dest_city_name"),
            F.trim(F.col("dest_state_nm")).alias("dest_state_nm"),
            F.col("dep_delay").cast("double").alias("dep_delay"),
            F.col("arr_delay").cast("double").alias("arr_delay"),
            F.col("cancelled").cast("double").cast("int").alias("cancelled"),
            normalize_text("cancellation_code").alias("cancellation_code"),
            F.col("diverted").cast("double").cast("int").alias("diverted"),
            F.col("distance").cast("double").alias("distance"),
            F.col("carrier_delay").cast("double").alias("carrier_delay"),
            F.col("weather_delay").cast("double").alias("weather_delay"),
            F.col("nas_delay").cast("double").alias("nas_delay"),
            F.col("security_delay").cast("double").alias("security_delay"),
            F.col("late_aircraft_delay").cast("double").alias("late_aircraft_delay"),
        )
        .filter(F.col("fl_date").isNotNull())
        .filter(F.col("month").between(1, 12))
        .filter(F.col("op_unique_carrier").isNotNull() & (F.col("op_unique_carrier") != ""))
        .filter(F.col("origin").isNotNull() & (F.col("origin") != ""))
        .filter(F.col("dest").isNotNull() & (F.col("dest") != ""))
        .filter(F.col("origin") != F.col("dest"))
        .filter(F.coalesce(F.col("diverted"), F.lit(0)) == 0)
    )

    return (
        typed_df.withColumn("route", F.concat_ws("-", F.col("origin"), F.col("dest")))
        .withColumn("is_cancelled", F.when(F.col("cancelled") == 1, F.lit(1)).otherwise(F.lit(0)))
        .withColumn("primary_disruption_cause", build_primary_disruption_cause())
        .withColumn(
            "departure_delay_band",
            F.when(F.col("dep_delay").isNull(), F.lit("unknown"))
            .when(F.col("dep_delay") < 15, F.lit("low"))
            .when(F.col("dep_delay") <= 60, F.lit("medium"))
            .otherwise(F.lit("high")),
        )
    )


def build_clean_dataset(raw_df):
    return build_filtered_dataset(raw_df).select(*OUTPUT_COLUMNS)


def main():
    args = parse_args()
    spark = (
        SparkSession.builder.appName("FlightDelayPrepareCleanDataset")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_df = spark.read.option("header", "true").option("mode", "PERMISSIVE").csv(args.input)
    filtered_df = build_filtered_dataset(raw_df)
    clean_df = filtered_df.select(*OUTPUT_COLUMNS)

    raw_count = raw_df.count()
    clean_count = clean_df.count()
    null_key_count = clean_df.filter(
        F.col("fl_date").isNull()
        | F.col("month").isNull()
        | F.col("op_unique_carrier").isNull()
        | F.col("origin").isNull()
        | F.col("dest").isNull()
    ).count()
    diverted_count = filtered_df.filter(F.col("diverted") == 1).count()

    if null_key_count != 0:
        raise RuntimeError(f"Clean dataset still contains {null_key_count} rows with null key fields.")

    if diverted_count != 0:
        raise RuntimeError(f"Clean dataset still contains {diverted_count} diverted rows.")

    clean_df.write.mode("overwrite").parquet(args.parquet_output)
    clean_df.write.mode("overwrite").option("header", "true").csv(args.csv_output)

    print("=== Flight Delay clean dataset metrics ===")
    print(f"raw_rows={raw_count}")
    print(f"processed_rows={clean_count}")
    print(f"removed_rows={raw_count - clean_count}")
    print(f"null_key_rows={null_key_count}")
    print(f"diverted_rows_after_filter={diverted_count}")
    print(f"parquet_output={args.parquet_output}")
    print(f"csv_output={args.csv_output}")

    spark.stop()


if __name__ == "__main__":
    main()
