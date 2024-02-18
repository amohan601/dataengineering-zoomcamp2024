import pandas as pd
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    project_id = kwargs.get('project_id')
    # Specify your data loading logic here
    #files = ['yellow','green']
    files = ['fhv']
    months = ['01','02','03','04','05','06','07','08','09','10','11','12']
    #months = ['01']
    #years = ['2019', '2020']
    years = ['2019']
    table_suffix = 'trips_data'
    schema_name = 'trips_data_full'
    
    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID':pd.Int64Dtype(),
        'store_and_fwd_flag':str,
        'PULocationID':pd.Int64Dtype(),
        'DOLocationID':pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra':float,
        'mta_tax':float,
        'tip_amount':float,
        'tolls_amount':float,
        'improvement_surcharge':float,
        'total_amount':float,
        'congestion_surcharge':float
    }
    fhv_dtype = {
        'dispatching_base_num': str,
        'PULocationID':pd.Int64Dtype(),
        'DOLocationID':pd.Int64Dtype(),
        'SR_Flag': str,
        'Affiliated_base_number': str
    }
    
    dtypes = {'yellow': taxi_dtypes, 'green': taxi_dtypes, 'fhv': fhv_dtype}
    parse_dates = {
        'yellow': ['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
         'green': ['lpep_pickup_datetime', 'lpep_dropoff_datetime'],
         'fhv': ['pickup_datetime',	'dropOff_datetime']
         }


    config_profile = 'default'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    # BigQuery data loader is instantiated with configuration settings
    bigquery_loader = BigQuery.with_config(ConfigFileLoader(config_path, config_profile))
    for filename in files:
        # Replace the following with your actual BigQuery table ID and configurations
        print('process started for file ', filename)
        table_name = f'{filename}_{table_suffix}'
        print(table_name)
        table_id = f'{project_id}.{schema_name}.{table_name}'
        print('insert starting for ',table_id)
        for year in years:
            for month in months:
                url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{filename}/{filename}_tripdata_{year}-{month}.csv.gz'
                df = pd.read_csv(url,dtype = dtypes[filename],parse_dates = parse_dates[filename])
                print('total size of records to be inserted for ', f'{filename}_tripdata_{year}-{month}', df.shape)    
                 # Exporting DataFrame to BigQuery
                bigquery_loader.export(df, table_id, if_exists='append')

    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
