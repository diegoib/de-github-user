import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


parser = argparse.ArgumentParser()

parser.add_argument('--temp_dataproc_bucket', required=True)
parser.add_argument('--data_bucket', required=True)
parser.add_argument('--bigquery_table', required=True)
parser.add_argument('--year', required=True)
parser.add_argument('--month', required=True)
parser.add_argument('--day', required=True)
args = parser.parse_args()

temp_dataproc_bucket = args.temp_dataproc_bucket
data_bucket = args.data_bucket
bigquery_table = args.bigquery_table
year = args.year
month = args.month
day = args.day

spark = SparkSession.builder \
    .appName('test_project') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', temp_dataproc_bucket)

path_files = f'gs://{data_bucket}/data/{year}-{month.zfill(2)}-{day.zfill(2)}-*'
df = spark.read.parquet(path_files)

df = df.select(F.date_trunc("Hour", F.col('created_at')).alias('Hour'),
        F.col('type'),
        F.col('actor.display_login').alias('user_name'),
        F.col('repo.name').alias('repo_name'))

df.write.format('bigquery') \
    .option('table', bigquery_table) \
    .option('partitionType', 'HOUR') \
    .option('partitionField', 'Hour') \
    .save()