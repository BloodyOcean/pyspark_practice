from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType, LongType

from main import ETL


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local").appName("ETLTest").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def etl_instance(spark):
    return ETL(spark)


def test_etl_process(spark, etl_instance):
    schema = StructType([
        StructField('user_id', IntegerType(), True),
        StructField('logs', StringType(), True),
        StructField('dt', TimestampType(), True)
    ])

    test_data = [
        (1, "Database connection failure: Error 505. Restarting...", datetime.strptime("2023-07-15 00:00:00", "%Y-%m-%d %H:%M:%S")),
        (2, "Some other log message", datetime.strptime("2023-07-15 01:00:00", "%Y-%m-%d %H:%M:%S")),
        (2, "Some other log message", datetime.strptime("2023-07-15 01:00:00", "%Y-%m-%d %H:%M:%S")),
        (2, "Some other log message", datetime.strptime("2023-07-15 01:00:00", "%Y-%m-%d %H:%M:%S")),
        (3, "Security breach: Unauthorized access", datetime.strptime("2023-07-15 02:00:00", "%Y-%m-%d %H:%M:%S")),
        (4, "Non-relevant log", datetime.strptime("2023-07-15 03:00:00", "%Y-%m-%d %H:%M:%S")),
        (10, "Database connection failure: Error 505. Restarting...Security breach: Unauthorized access", datetime.strptime("2024-07-15 01:00:00", "%Y-%m-%d %H:%M:%S")),
        (10, "Database connection failure: Error 505. Restarting...Security breach: Unauthorized access", datetime.strptime("2024-07-15 01:03:00", "%Y-%m-%d %H:%M:%S")),

    ]

    df = spark.createDataFrame(test_data, schema)

    result_df = etl_instance.transform(df)

    expected_schema = StructType([
        StructField('issue_name', StringType(), True),
        StructField('dt_trunc', DateType(), True),
        StructField('sum', LongType(), True)
    ])

    expected_data = [
        ('Unauthorized access', datetime.strptime("2024-07-15 02:00:00", "%Y-%m-%d %H:%M:%S"), 1),
        ("Database connection failure", datetime.strptime("2023-07-15 00:00:00", "%Y-%m-%d %H:%M:%S"), 1),
        ('Database connection failure', datetime.strptime("2024-07-15 02:00:00", "%Y-%m-%d %H:%M:%S"), 1),
        ("Unauthorized access", datetime.strptime("2023-07-15 02:00:00", "%Y-%m-%d %H:%M:%S"), 1),
    ]

    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Compare schemas
    assert result_df.schema == expected_df.schema

    # Compare data
    result_data = [tuple(row) for row in result_df.collect()]
    expected_data = [tuple(row) for row in expected_df.collect()]

    assert result_data == expected_data
