import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

scala_version = '2.12'
spark_version = '3.5.1'

# Ensure match above values match the correct versions
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1'
]


# Kafka configuration
KAFKA_TOPIC = 'orders'
KAFKA_SERVER = 'localhost:19092'

# Smoothing factor for EWMA
alpha = 0.3


def process_job(spark):
    # Define the schema for the JSON data
    schema = StructType([
        StructField("user_id", StringType(), True),  # user_id as String
        StructField("product_count", IntegerType(), True),  # product_count as Integer
        StructField("total_price", FloatType(), True),  # total_price as Float
        StructField("time", TimestampType(), True)  # time as Timestamp
    ])

    source_df = spark.readStream\
        .format('kafka')\
        .option("kafka.bootstrap.servers", KAFKA_SERVER)\
        .option('subscribe', KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    df = source_df.selectExpr("CAST(value AS STRING) as json_string")

    # Parse the JSON data
    parsed_df = df \
        .select(F.from_json(F.col("json_string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn('key_split', F.split(F.col('user_id'), '/')) \
        .where((F.col('key_split')[1] == F.lit('lviv')) & (F.col('key_split')[0] == F.lit('ukraine'))) \
        .withWatermark("time", "30 seconds") \
        .groupBy(F.col('key_split')[2].alias('store_address'), F.window(F.col("time"), "30 seconds")) \
        .agg(F.avg("total_price").alias("avg_total_price"),
             F.avg("product_count").alias("avg_product_count"),
             F.mode("product_count").alias("mode_product_count"))

    parsed_df.writeStream \
        .trigger(processingTime='30 seconds') \
        .outputMode('complete') \
        .format('console') \
        .option("truncate", "false") \
        .start()

    spark.streams.awaitAnyTermination()


if __name__ == '__main__':

    spark = SparkSession.builder\
        .appName('streaming_app') \
        .master("local[3]")\
        .config('spark.jars.packages', ','.join(packages))\
        .getOrCreate()

    process_job(spark)
