# LOAD BLOCK
import pandas as pd
from pandas import DataFrame

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_big_query(data, **kwargs) -> None:
    """
    Export data from dictionary (block's transform) to Google BigQuery.
    """
    project_id = 'project1-tlc-data-analytics'
    dataset_id = 'tlc_data_small'

    for table_name, df_value in data.items():
        # table_name is 'datetime_dim', 'fact_table', v.v.
        destination_table = f"{project_id}.{dataset_id}.{table_name}"
                
        df_value.to_gbq(
            destination_table=destination_table,
            project_id=project_id,
            if_exists='replace',
            progress_bar=True
        )