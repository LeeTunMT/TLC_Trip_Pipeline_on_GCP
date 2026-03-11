# EXTRACT BLOCK
import pandas as pd
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_from_google_cloud_storage(*args, **kwargs):

    bucket_name = 'project1-elt-data'
    object_key = 'yellow_tripdata_2025-01.parquet'
    
    gcs_path = f'gs://{bucket_name}/{object_key}'
        
    df = pd.read_parquet(gcs_path)
    
    return df

@test
def test_output(output, *args) -> None:
    assert output is not None, 'Success'
    assert isinstance(output, pd.DataFrame), 'The output must be a pandas DataFrame'

