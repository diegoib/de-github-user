import os
import subprocess
import itertools
from io import StringIO
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


def runcmd(cmd, verbose = False, *args, **kwargs):

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


def download_data(year: str, month: str, day: str, hour: str) -> None:
    cmd_fetch = f'''wget https://data.gharchive.org/{year}-{month}-{day}-{hour}.json.gz && \
            gzip -d {year}-{month}-{day}-{hour}.json.gz'''
    cmd_count = f'wc -l {year}-{month}-{day}-{hour}.json'
    runcmd(cmd_fetch)


def ingest(year: str, month: str, day: str, hour: str) -> pd.DataFrame:
    dfs = []
    with open(fp, 'r') as f:
        while True:
            lines = list(itertools.islice(f, 1000))
            
            if lines:
                lines_str = ''.join(lines)
                dfs.append(pd.read_json(StringIO(lines_str), lines=True))
            else:
                break
    df = pd.concat(dfs)
    return df

def parquetize(df: pd.DataFrame, year: str, month: str, day: str, hour: str) -> Path:
    '''Write DataFrame out locally as parquet file'''
    path = Path(f'data/{year}/{month}/{day}/{hour}.parquet')
    df.to_parquet(path, compression='gzip')
    return path       

def load_gcs():
    '''Upload local parquet file to GCS'''
    gcs_block = GcsBucket.load('zoom-gcs')
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path
    )

@flow()
def fetch_n_load() -> None:
    