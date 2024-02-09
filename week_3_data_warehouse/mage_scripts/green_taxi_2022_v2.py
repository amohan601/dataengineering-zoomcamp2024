from pandas import read_parquet
import pandas as pd
import requests
from io import BytesIO
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
    months = ['01','02','03','04','05','06','07','08','09','10','11','12']
    # months = ['01']
    bucket_name = 'nyc-tl-data-dts-2024-module3'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =  "/home/src/gcp-creds.json"
    project_id = kwargs.get('project_id')

    for month in months:
        file = f'green_tripdata_2022-{month}.parquet'
        print(file)
        parquet_url = base_url + file
        print(parquet_url)
        response = requests.get(parquet_url)
        df = read_parquet(BytesIO(response.content))
        object_key = f'green_taxi/{file}'
        root_path = f'{bucket_name}/{object_key}'
        table = pa.Table.from_pandas(df)
        # print(table)
        gcs = pa.fs.GcsFileSystem()
        pq.write_to_dataset(
            table,
            root_path = root_path,
            filesystem = gcs,
            use_deprecated_int96_timestamps=True
        )
    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

