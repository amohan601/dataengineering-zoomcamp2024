import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data(data, *args, **kwargs):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =  "/home/src/gcp-creds.json"
    bucket_name = kwargs.get('bucket_name')
    print('bucket name selected ',bucket_name)
    project_id = kwargs.get('project_id')
    print('project_id selected ',project_id)
    table_name = kwargs.get('table_name')
    print('table_name ',table_name)
    root_path = f'{bucket_name}/{table_name}'
    table = pa.Table.from_pandas(data)
    gcs = pa.fs.GcsFileSystem()
    pq.write_to_dataset(
        table,
        root_path = root_path,
        partition_cols =  ['lpep_pickup_date'],
        filesystem = gcs
    )
    print('export to GCS completed')



