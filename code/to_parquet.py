from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, input_file_name
from datetime import datetime
from pyspark.sql.functions import input_file_name
import sys
from pyspark.sql.functions import regexp_extract

import os
print("GOOGLE_APPLICATION_CREDENTIALS =", os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

APP_NAME = "TestSaveParquet"
#RAW_BASE = "gs://nuni-bucket/wiki/2024-01/dt=20240101/"
RAW_BASE = "gs://nuni-bucket/wiki/"
SAVE_BASE = "gs://nuni-bucket/wiki/test/parquet"
DT = sys.argv[1]

# SparkSession 생성 (마스터 URL 지정 X, spark-submit에서 설정됨)
spark = SparkSession.builder.appName(f"{APP_NAME}_{DT}").getOrCreate()

def load_pageviews(file_path, date_str):
    return spark.read.option("delimiter", " ").csv(file_path, inferSchema=True) \
        .toDF("domain", "title", "views", "size") \
        .withColumn("date", lit(date_str)) \
        .withColumn("hour", regexp_extract(input_file_name(), r'pageviews-\d{8}-(\d{2})', 1).cast("int"))\
        .withColumn("file_path", input_file_name())

def extract_prefix_and_partition(date_str: str) -> tuple[str, str]:
    """
    "2024-03-01" → ("2024-03", "20240301")
    """
    parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
    prefix = parsed_date.strftime("%Y-%m")
    partition = parsed_date.strftime("%Y%m%d")
    return prefix, partition

prefix, partition = extract_prefix_and_partition(DT)
raw_path = f"{RAW_BASE}/{prefix}/dt={partition}"
df = load_pageviews(raw_path, partition)

print("LOAD".center(33, "*"))
df.show(10)


print("SAVE START".center(33, "*"))
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").partitionBy("date").parquet(SAVE_BASE)
print("SAVE END".center(33, "*"))

# Spark 세션 종료
spark.stop()
