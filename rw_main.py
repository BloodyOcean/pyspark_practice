import argparse

from pyspark.sql import SparkSession, Window
from datetime import datetime

from pyspark.sql.types import StructType, StructField, DateType, TimestampType, LongType, StringType, ArrayType, \
    DoubleType, IntegerType, BinaryType

from utils.parser import ConfigReader
import pyspark.sql.functions as F

REIMBURSEMENT_CLASS_LIST_ = ['VIP']


def parse_date(date_str):
    """Parse a date string in the format YYYY-MM-DD."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: '{date_str}'. Use YYYY-MM-DD.")


def main():
    parser = argparse.ArgumentParser(description="Process start and end dates.")
    parser.add_argument("start_date", type=parse_date, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("end_date", type=parse_date, help="End date in YYYY-MM-DD format.")

    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date

    config_reader = ConfigReader("config.ini")
    database_host = config_reader.get("database", "host", fallback="localhost")
    database_port = config_reader.get_int("database", "port", fallback=5432)
    user = config_reader.get("database", "user")
    table_name = config_reader.get("database", "table_name")
    db_name = config_reader.get("database", "db_name")
    password = config_reader.get("database", "password")

    # Path to the PostgreSQL JDBC driver
    jdbc_driver_path = "./postgresql-42.7.3.jar"

    # Create a Spark session and include the JDBC driver
    spark = SparkSession.builder \
        .appName("PostgresConnector") \
        .config("spark.jars", jdbc_driver_path) \
        .getOrCreate()

    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    # Database connection details
    jdbc_url = f"jdbc:postgresql://{database_host}:{database_port}/{db_name}"
    connection_properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }

    # Read data from a PostgreSQL table into a PySpark DataFrame
    df_keys = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

    # Define custom formats
    custom_date_format = "EEE MMM dd yyyy"
    trip_custom_timestamp_format = "EEE MMM dd HH:mm:ss yyyy"
    user_custom_timestamp_format = "yyyy-MM-dd HH:mm:ss z"

    # Define the schema for the JSON data
    user_schema = StructType([
        StructField("date", DateType(), nullable=True),  # Storing as StringType initially for custom parsing
        StructField("users", ArrayType(
            StructType([
                StructField("user_id", StringType(), nullable=True),
                StructField("full_name", StringType(), nullable=True),
                StructField("station", StringType(), nullable=True),
                StructField("platform", IntegerType(), nullable=True),
                StructField("sked_dep_time", TimestampType(), nullable=True),
                StructField("class", StringType(), nullable=True),
                StructField("price", DoubleType(), nullable=True)
            ])
        ), nullable=True)
    ])

    # Define the schema
    trip_schema = StructType([
        StructField("date", DateType(), nullable=True),
        StructField("trips", ArrayType(
            StructType([
                StructField("act_arr_time", TimestampType(), nullable=True),
                StructField("act_dep_time", TimestampType(), nullable=True),
                StructField("platform", LongType(), nullable=True),
                StructField("sked_arr_time", TimestampType(), nullable=True),
                StructField("sked_dep_time", TimestampType(), nullable=True),
                StructField("station", StringType(), nullable=True),
                StructField("train_no", StringType(), nullable=True)
            ])
        ), nullable=True)
    ])

    df_trips = (spark.read.schema(trip_schema)
                .option("multiline", "true")
                .json('./trips.json', dateFormat=custom_date_format, timestampFormat=trip_custom_timestamp_format)
                .where(F.col('date').between(start_date, end_date))
                .select(F.col('date'), F.explode('trips').alias('trip'))
                .withColumn('trip_id', F.expr('uuid()')))

    df_users = (spark.read.schema(user_schema)
                .option("multiline", "true")
                .json('./users.json', dateFormat=custom_date_format, timestampFormat=user_custom_timestamp_format)
                .where(F.col('date').between(start_date, end_date))
                .select(F.col('date'), F.explode('users').alias('user')))

    # Needed for non-additive metrics calculation
    w = Window.partitionBy(F.col('t.date'), F.col('u.user.user_id'), F.col('t.trip_id')).orderBy(F.col('t.trip.sked_dep_time'))

    df_trips_users = (df_trips.alias('t')
                      .join(df_users.alias('u'), (F.col('t.trip.sked_dep_time') == F.col('u.user.sked_dep_time')) &
                            (F.col('u.user.station') == F.col('t.trip.station')) &
                            (F.col('u.user.platform') == F.col('t.trip.platform')))
                      .withColumn('delta_arr_min', (F.unix_seconds(F.col('t.trip.act_arr_time')) - F.unix_seconds(F.col('t.trip.sked_arr_time'))) / 60)
                      .withColumn('delta_dep_min', (F.unix_seconds(F.col('t.trip.act_dep_time')) - F.unix_seconds(F.col('t.trip.sked_dep_time'))) / 60)
                      .withColumn('reimbursement_dep_delay', F.when((F.col('u.user.class').isin(REIMBURSEMENT_CLASS_LIST_) & (F.col('delta_dep_min') >= F.lit(60))), F.lit(100)).otherwise(F.lit(0)))
                      .withColumn('reimbursement_arr_delay', F.when(F.col('delta_arr_min') < F.lit(60), F.col('u.user.price') / 100 * F.col('delta_arr_min')).otherwise(F.col('u.user.price')))
                      .withColumn('reimbursement_for_delay', F.col('reimbursement_dep_delay') + F.col('reimbursement_arr_delay'))
                      .withColumn('dayweek_name', F.date_format('t.date', 'EEEE'))
                      .withColumn('rn', F.row_number().over(w)))

    df_reimb_agg = (df_trips_users.groupby(F.col('u.user.user_id'), F.col('t.trip_id'))
                    .agg(F.sum(F.when(F.col('rn') == F.lit(1), F.col('u.user.price'))).alias('price'),
                         F.sum('reimbursement_for_delay').alias('reimbursement_for_delay_total')))

    df_station_week_agg = (df_trips_users.groupby(F.col('t.trip.station'))
                           .pivot('dayweek_name')
                           .agg(F.struct(
                                    F.count(F.col('trip_id')).alias('trips_cnt'),
                                    F.count_distinct(F.col('u.user.user_id')).alias('user_cnt'),
                                    F.count_distinct(F.when(F.col('u.user.class').isin('VIP'), F.col('u.user.user_id'))).alias('vip_cnt'),
                                    F.count_distinct(F.when(F.col('u.user.class').isin('STANDART'), F.col('u.user.user_id'))).alias('standart_cnt'),
                                    F.count_distinct(F.when(F.col('u.user.class').isin('ECONOM'), F.col('u.user.user_id'))).alias('econom_cnt'),
                                    F.sum(F.col('u.user.price')).alias('total_price'),
                                    (F.sum(F.col('u.user.price')) - F.sum('reimbursement_for_delay')).alias('total_income')
                                )
                           ))

    df_station_week_agg.write.mode('overwrite').json('week_agg_report')
    df_reimb_agg.write.mode('overwrite').json('reimbursement_report')


if __name__ == '__main__':
    main()
