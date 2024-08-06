from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, DateType

from utils.utils import *


class ETL:
    def __init__(self, spark):
        self.spark = spark

    def extract(self):
        df_raw = self.spark.read.csv('./log.csv', header=True, inferSchema=True, sep=';')
        return df_raw

    def transform(self, df):
        @F.udf(returnType='int')
        def count_occurrence(substr, text):
            return text.count(substr)

        schema = StructType([
            StructField("issue_name", StringType(), True),
            StructField("issue_affiliate", StringType(), True)
        ])

        data = [
            ('Database connection failure', "Database connection failure: Error 505. Restarting..."),
            ('Server timeout', "Server timeout: Issue 102"),
            ('Memory allocation issue', "Application crash: Memory allocation issue 203. Clean the storage."),
            ('Network instability', "Network instability: Packet loss 404"),
            ('Unauthorized access', "Security breach: Unauthorized access"),
            ('Service disruption', "Service disruption: Extended maintenance 506"),
            ('Data corruption', "Data corruption: Integrity check"),
            ('Performance degradation', "Performance degradation: High CPU usage 808"),
            ('Disk space error', "Disk space error: Storage limit 12 000 MB"),
            ('Dependency failure', "Dependency failure: External API 1010")
        ]

        df_issues = self.spark.createDataFrame(data, schema)

        df_transformed = (df.dropDuplicates().alias('a')
                          .join(F.broadcast(df_issues).alias('b'), F.col('a.logs').contains(F.col('b.issue_name')))
                          .withColumn('num_occurrence', count_occurrence(F.col('b.issue_affiliate'), F.col('a.logs')))
                          )

        df_min_ts = (df_transformed.groupBy(F.col('user_id'), F.col('issue_name')).agg(F.min('dt').alias('min_dt')))

        w = Window.partitionBy(F.col('s.user_id'), F.col('s.issue_name'), F.col('group_no')).orderBy(F.col('dt'))

        df_joined = (df_transformed.alias('s')
                     .join(df_min_ts.alias('t'),
                           (F.col('s.user_id') == F.col('t.user_id')) & (
                                       F.col('s.issue_name') == F.col('t.issue_name')))
                     .withColumn('group_no',
                                 (F.unix_timestamp(F.col('dt')) - F.unix_timestamp(F.col('min_dt'))) / (5 * 60))
                     .withColumn('group_no', F.col('group_no').cast('integer'))
                     .withColumn('rn', F.row_number().over(w))
                     .where(F.col('rn') == F.lit(1))
                     .withColumn('dt_trunc', F.col('dt').cast(DateType()))
                     .groupBy(F.col('s.issue_name'), F.col('dt_trunc'))
                     .agg(F.sum(F.col('num_occurrence')).alias('sum'))
                     )

        return df_joined

    def load(self, df):
        BUCKETS_NUMBER = 2
        OUTPUT_PATH = './result_issues'
        df.repartition(BUCKETS_NUMBER).write.partitionBy('dt_trunc') \
            .format('parquet') \
            .mode("overwrite") \
            .option("path", OUTPUT_PATH) \
            .save()


def process_job(spark):
    etl_instance = ETL(spark)
    etl_instance.load(etl_instance.transform(etl_instance.extract()))

    w = Window.partitionBy(F.col('dt_trunc')).orderBy(F.col('sum').desc())
    w1 = Window.partitionBy(F.col('issue_name')).orderBy(F.col('dt_trunc'))

    df_raw = (spark.read.parquet('./result_issues'))
    df_critical = (df_raw.select('issue_name', 'sum', 'dt_trunc')
                   .withColumn('dr', F.dense_rank().over(w))
                   .where(F.col('dr').between(1, 3))
                   .withColumn('day_grp', F.date_add(F.col('dt_trunc'), -1 * F.row_number().over(w1)))
                   .groupBy(F.col('issue_name'), F.col('day_grp'))
                   .agg(F.count('*').alias('cnt'))
                   .where(F.col('cnt') >= F.lit(5))
                   .select('issue_name')
                   )
    df_critical.write.format('parquet').option('path', './critical_issues').mode('overwrite').save()


if __name__ == '__main__':
    cnf = {"spark.sql.sources.partitionOverwriteMode": "dynamic",
           "mapreduce.fileoutputcommitter.marksuccessfuljobs": "false",
           "spark.sql.parquet.compression.codec": "none",
           "write.parquet.compression-codec": "none",
           "spark.sql.parquet.mergeSchema": "false"
           }

    spark_session_manager = SparkSessionManager(cnf)
    spark_session_manager.start_session()
    process_job(spark_session_manager.spark)
    spark_session_manager.close_session()
