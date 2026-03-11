# TRANSFORM BLOCK
import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    # create datetime_dim
    df.rename(columns = {'Airport_fee': 'airport_fee'}, inplace = True)
    datetime_dim = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)
    datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['datetime_id'] = datetime_dim.index
    datetime_dim = datetime_dim[['datetime_id','tpep_pickup_datetime','pick_year', 'pick_month', 'pick_day', 'pick_hour',
                                'tpep_dropoff_datetime', 'drop_year', 'drop_month', 'drop_day', 'drop_hour']]

    # create passenger_count_dim
    passenger_count_dim = df[['passenger_count']].drop_duplicates().reset_index(drop=True)
    passenger_count_dim['passenger_id'] = passenger_count_dim.index
    passenger_count_dim = passenger_count_dim[['passenger_id', 'passenger_count']]

    # create pickup_location_dim
    pickup_location_dim = df[['PULocationID']].drop_duplicates().reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id','PULocationID']]

    # create dropoff_location_dim
    dropoff_location_dim = df[['DOLocationID']].drop_duplicates().reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id', 'DOLocationID']]

    # create rate_code_dim
    rate_code_dim = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
    rate_code_name = {
        1 : "Standard rate",
        2 : "JFK",
        3 : "Newark",
        4 : "Nassau or Westchester",
        5 : "Negotiated fare",
        6 : "Group ride",
        99 : "unknown"
    }
    rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_name)
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dim = rate_code_dim[['rate_code_id', 'RatecodeID' ,'rate_code_name']]

    # create payment_type_dim
    payment_type_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    payment_name = {
        0 : "Flex Fare trip",
        1 : "Credit card",
        2 : "Cash",
        3 : "No charge",
        4 : "Dispute",
        5 : "Unknown",
        6 : "Voided trip"
    }
    payment_type_dim['payment_name'] = payment_type_dim['payment_type'].map(payment_name)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim = payment_type_dim[['payment_type_id', 'payment_type', 'payment_name']]

    # create trip_distance_dim
    trip_distance_dim = df[['trip_distance']].drop_duplicates().reset_index(drop=True)
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
    trip_distance_dim = trip_distance_dim[['trip_distance_id', 'trip_distance']]


    # create Fact_table
    fact_table = (
        df.merge(datetime_dim, on=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
        .merge(passenger_count_dim, on='passenger_count')
        .merge(pickup_location_dim, on='PULocationID')
        .merge(dropoff_location_dim, on='DOLocationID')
        .merge(rate_code_dim, on='RatecodeID')
        .merge(trip_distance_dim, on='trip_distance')
        .merge(payment_type_dim, on='payment_type')
        [[
            'VendorID', 'datetime_id', 'passenger_id', 'pickup_location_id', 
            'dropoff_location_id', 'rate_code_id', 'trip_distance_id', 
            'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 
            'tolls_amount', 'improvement_surcharge', 'total_amount', 
            'congestion_surcharge', 'airport_fee', 'cbd_congestion_fee'
        ]]
    )
    
    return {"datetime_dim":datetime_dim,
    "passenger_count_dim":passenger_count_dim,
    "trip_distance_dim":trip_distance_dim,
    "rate_code_dim":rate_code_dim,
    "pickup_location_dim":pickup_location_dim,
    "dropoff_location_dim":dropoff_location_dim,
    "payment_type_dim":payment_type_dim,
    "fact_table":fact_table} 

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
