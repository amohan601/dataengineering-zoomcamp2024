if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print('data frame shape at the begining ', data.shape)
    # Remove rows where the passenger count is equal to 0 or the trip distance is equal to zero.
    filtered_data = data[~(data['passenger_count'] == 0)]
    print('check passenger_count = 0 ', filtered_data['passenger_count'].isin([0]).sum())
    print('after filtering passenger_count of 0 ', filtered_data.shape)
    filtered_data = filtered_data[~(filtered_data['trip_distance'] == 0)]
    print('check trip_distance = 0 ', filtered_data['trip_distance'].isin([0]).sum())
    print('after filtering trip_distance of 0 ', filtered_data.shape)

    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    filtered_data['lpep_pickup_date'] = filtered_data['lpep_pickup_datetime'].dt.date

    #Unique values for VendorID
    print('Unique values for VendorID ', filtered_data['VendorID'].unique())

    # Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
    print('current column names ',filtered_data.columns)
    
    new_columns = [col.lower().replace('id','_id') for col in filtered_data.columns]
    print(new_columns)
    filtered_data.columns = new_columns
    print('updated column names ',filtered_data.columns)

    new_data = filtered_data['lpep_pickup_datetime'].dt.year
    print(new_data.unique());


    print('data frame shape at the end ', filtered_data.shape)
    return filtered_data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

    # vendor_id is one of the existing values in the column (currently)
    assert ~output['vendor_id'].empty, 'vendor_id column not found'
    # passenger_count is greater than 0
    assert output['passenger_count'].isin([0]).sum() ==0 , 'passenger rides with 0 found'
    # trip_distance is greater than 0
    assert output['trip_distance'].isin([0]).sum() ==0 , 'trip distance with 0 found' 
