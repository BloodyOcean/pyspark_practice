from collections import Counter

from pyspark import Row
from pyspark.ml.feature import Bucketizer
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


def subtract_rdd(spark):
    list_a = [1, 2, 3, 4, 5]
    list_b = [4, 5, 6, 7, 8]
    rdd_a = spark.sparkContext.parallelize(list_a)
    rdd_b = spark.sparkContext.parallelize(list_b)
    rdd_a_w_b = rdd_a.subtract(rdd_b)
    rdd_b_w_a = rdd_b.subtract(rdd_a)
    print('Present in A and not in B:\n')
    res = rdd_a_w_b.collect()
    print(res)
    print('Unique for both A and B')
    res_union = rdd_b_w_a.union(rdd_a_w_b).collect()
    print(res_union)


def get_age_statistics(spark):
    # Create a sample DataFrame
    data = [("A", 10), ("B", 20), ("C", 30), ("D", 40), ("E", 50), ("F", 15), ("G", 28), ("H", 54), ("I", 41),
            ("J", 86)]
    df = spark.createDataFrame(data, ["Name", "Age"])
    # Get min, median, max etc
    res = df.approxQuantile('Age', [0.00, 0.25, 0.50, 0.75, 1.00], 0.01)
    print(res)


def get_top_n_jobs(spark, n: int):
    # Sample data
    data = [
        Row(name='John', job='Engineer'),
        Row(name='John', job='Engineer'),
        Row(name='Mary', job='Scientist'),
        Row(name='Bob', job='Engineer'),
        Row(name='Bob', job='Engineer'),
        Row(name='Bob', job='Scientist'),
        Row(name='Sam', job='Doctor'),
    ]

    # create DataFrame
    df = spark.createDataFrame(data)

    top_n_jobs = (df.groupBy('job')
                  .agg(F.count(F.col('job')).alias('cnt'))
                  .orderBy(F.col('cnt').desc())
                  .limit(n)
                  .select('job')
                  .rdd
                  .flatMap(lambda x: x).collect())

    res_df = df.withColumn('job', F.when(F.col('job').isin(top_n_jobs), F.col('job')).otherwise(F.lit('Other')))

    # show DataFrame
    res_df.show()


def create_timeseries(spark):
    df = spark.createDataFrame([('2000-01-01', '2000-03-04')], ('start_date', 'end_date'))
    df = df \
        .selectExpr("explode(sequence(to_date(start_date), to_date(end_date)))") \
        .withColumn('rnd', F.rand(seed=42)) \
        .withColumn('art_id', F.monotonically_increasing_id())

    df.show()


def remove_null_rows(spark):
    new_order = ['id', 'Name', 'Value']
    # Assuming df is your DataFrame
    df = spark.createDataFrame([
        ("john", 1, None),
        ("jack", None, "123"),
        ("bob", 3, "456"),
        ("alice", None, None),
    ], ["Name", "Value", "id"])

    df_res = df.dropna(subset=['value']).withColumn('Name', F.initcap(F.col('Name'))).select(*new_order)
    df_res.show()


def rename_cols(spark):
    # suppose you have the following DataFrame
    df = spark.createDataFrame([(1, 2, 3), (4, 5, 6)], ["col1", "col2", "col3"])

    # old column names
    old_names = ["col1", "col2", "col3"]

    # new column names
    new_names = ["new_col1", "new_col2", "new_col3"]

    zip_lst = dict(zip(old_names, new_names))
    for k, v in zip_lst.items():
        df = df.withColumnRenamed(k, v)

    df.show()


def bucket_the_nums(spark, l=100, n=10):
    df_nums = spark.range(l).withColumn('rnd', F.rand(seed=42))
    df_nums.show()
    bucket_lst = df_nums.approxQuantile('rnd', [i / n for i in range(n + 1)], 0.01)
    buckets = Bucketizer(splits=bucket_lst, inputCol='rnd', outputCol='rnd_new')
    df_new = buckets.transform(df_nums)
    df_new.show()


def unpivot_rows(spark):
    # Sample data
    data = [
        (2021, 1, "US", 5000),
        (2021, 1, "EU", 4000),
        (2021, 2, "US", 5500),
        (2021, 2, "EU", 4500),
        (2021, 3, "US", 6000),
        (2021, 3, "EU", 5000),
        (2021, 4, "US", 7000),
        (2021, 4, "EU", 6000),
    ]

    # Create DataFrame
    columns = ["year", "quarter", "region", "revenue"]
    df = spark.createDataFrame(data, columns)
    df_res = df.groupBy("year", "quarter").pivot('region').agg(F.sum('revenue'))
    df_res.show()


def replace_frequent_character(spark):
    def replace_char(s):
        tmp = s.replace(' ', '')
        c = Counter(tmp)
        least_freq_char = min(c, key=c.get)
        return s.replace(' ', least_freq_char)

    udf_f = F.udf(replace_char, StringType())

    # Sample DataFrame
    df = spark.createDataFrame([('dbc deb abed gade',), ], ["string"])
    df = df.withColumn('new', udf_f('string'))
    df.show()


class SparkSessionManager:
    def __init__(self, config=None):
        self.config = config
        self.spark = None

    def start_session(self):
        self.spark = SparkSession.builder.config(map=self.config).appName('practice_spark').getOrCreate()
        print("Started Spark Session.")

    def close_session(self):
        if self.spark is not None:
            self.spark.stop()
            print("Stopped Spark Session.")
