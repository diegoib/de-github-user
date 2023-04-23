import os
from prefect import flow
import yaml

@flow()
def submit_job(cluster: str, region: str, spark_file_path: str, 
               data_bucket: str, bigquery_table: str, year: str, month: str, 
               day: str) -> None:
    ''' Submit a job to Dataproc '''
    os.system(f'''gcloud dataproc jobs submit pyspark \
                --cluster={cluster} \
                --region={region} \
                --jars=gs://spark-lib/bigquery/spark-3.1-bigquery-0.30.0.jar \
                {spark_file_path} \
                -- \
                    --data_bucket={data_bucket} \
                    --bigquery_table={bigquery_table} \
                    --year={year} \
                    --month={month} \
                    --day={day}
                    '''
    )
    
if __name__ == '__main__':

    with open('config.yaml', 'r') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    cluster = config['GCP']['CLUSTER']
    region = config['GCP']['REGION']
    spark_file_path = config['GCP']['SPARK_PATH_FILE']
    data_bucket = config['GCP']['DATA_BUCKET']
    bigquery_table = config['GCP']['BIGQUERY_TABLE']
    year = config['GH']['YEAR']
    month = config['GH']['MONTH']
    day = config['GH']['DAY']

    submit_job(cluster, region, spark_file_path, 
               data_bucket, bigquery_table, year, month, day)