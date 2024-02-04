import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    year = kwargs.get('year')
    print(year)
    base_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/'
    months = kwargs.get('months').split(',')
    print(months)

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
    
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    df = []
    for month in months:
        file = 'green_tripdata_2020-'+str(month)+'.csv.gz'
        url = base_url+file
        print(url)
        temp = pd.read_csv(url,dtype = taxi_dtypes, parse_dates = parse_dates)
        print('this df size ',temp.shape)
        df.append(temp)

    result = pd.concat(df)
    print('Size of dataframe ',result.shape)
    return result


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
