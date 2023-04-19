import os
import subprocess
import itertools
from io import StringIO
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


def runcmd(cmd, verbose = False, *args, **kwargs):
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


def fetch_data(year: int, month: int, day: int, hour: int) -> str:
    '''Fetch data from source'''
    dataset_name = f'{year:04}-{month:02}-{day:02}-{hour}'
    cmd_fetch = f'''wget https://data.gharchive.org/{dataset_name}.json.gz -O data/{dataset_name}.json.gz && \
            gzip -d data/{dataset_name}.json.gz'''
    runcmd(cmd_fetch)
    return dataset_name


def ingest_data(dataset_name: str) -> pd.DataFrame:
    '''Ingest json data with pandas'''
    dfs = []
    with open(f'data/{dataset_name}.json', 'r') as f:
        while True:
            lines = list(itertools.islice(f, 1000))
            
            if lines:
                lines_str = ''.join(lines)
                dfs.append(pd.read_json(StringIO(lines_str), lines=True))
            else:
                break
    df = pd.concat(dfs)
    return df

def write_to_parquet(df: pd.DataFrame, dataset_name: str) -> Path:
    '''Write DataFrame out locally as parquet file'''
    path = Path(f'data/{dataset_name}.parquet')
    df.to_parquet(path, compression='gzip')
    return path       

def load_gcs(path):
    '''Upload local parquet file to GCS'''
    gcs_block = GcsBucket.load('de-project-bucket')
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path
    )

@flow()
def fetch_n_load(year: int, month: int, day: int, hour: int) -> None:
    """The main ETL function"""
    name = fetch_data(year, month, day, hour)
    df = ingest_data(name)
    path = write_to_parquet(df, name)
    load_gcs(path)

@flow
def parent_flow(year: int, month: int, day: int, hours: list[int]) -> None:
    for hour in hours:
        fetch_n_load(year, month, day, hour)

if __name__ == "__main__":
    year = 2023
    month = 4
    day = 1
    hours = list(range(24))
    parent_flow(year, month, day, hours)