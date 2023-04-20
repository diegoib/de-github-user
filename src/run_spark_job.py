import os
import subprocess
import itertools
from io import StringIO
from pathlib import Path
import pandas as pd
from prefect import flow, task

import yaml


def runcmd(cmd: str, verbose = False, *args, **kwargs) -> None:
    '''Executes bash commands in another terminal session'''
    process = subprocess.Popen(
        cmd,
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        text = True,
        shell = True
    )
    std_out, std_err = process.communicate()
    if verbose:
        print(std_out.strip(), std_err)
    pass

with open('config.yaml', 'r') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)


@task()
def submit_job(cluster: str, region: str, spark_file_path: str, temp_dataproc_bucket: str,
               data_bucket_path: str, bigquery_table: str) -> None:
    ''' Submit a job to Dataproc '''
    runcmd(f'''gcloud dataproc jobs submit pyspark \
            --cluster={cluster} \
            --region={region} \
            -- jars=gs//spark-lib/bigquey/spark-bigquery-latest_2.12.jar \
            {spark_file_path}
            --
                --temp_dataproc_bucket={temp_dataproc_bucket}
                --data_bucket_path={data_bucket_path}
                --bigquery_table={bigquery_table}
                '''
           )
    
if __name__ == '__main__':
    cluster = config['GCP']['CLUSTER']
    region = config['GCP']['REGION']
    spark_file_path = config['GCP']['SPARK_PATH_FILE']
    temp_dataproc_bucket = config['GCP']['TEMP_DATAPROC_BUCKET']
    data_bucket_path = config['GCP']['DATA_BUCKET_PATH']
    bigquery_table = config['GCP']['BIGQUERY_TABLE']

    submit_job(cluster, region, spark_file_path, temp_dataproc_bucket,
               data_bucket_path, bigquery_table)