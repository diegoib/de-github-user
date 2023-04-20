import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


parser = argparse.ArgumentParser()

parser.add_argument('--temp_dataproc_bucket', required=True)
parser.add_argument('--data_bucket_path', required=True)
parser.add_argument('--bigquery_table', required=True)
args = parser.parse_args()

temp_dataproc_bucket = args.temp_dataproc_bucket
data_bucket_path = args.data_bucket_path
bigquery_table = args.bigquery_table

spark = SparkSession.builder \
    .appName('test_project') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', temp_dataproc_bucket)

df = spark.read.parquet(data_bucket_path)

'''

CODIGO CON LAS TRANSFORMACIONES

'''

df.write.format('bigquery') \
    .option('table', bigquery_table) \
    .save()