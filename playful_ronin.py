from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform_in_postgres(*args, **kwargs):
    """
    Template for transforming data in a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-transformation#postgresql
    """
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS transformed_taxi AS
    SELECT
        *,
        tpep_pickup_datetime::TIMESTAMP AS tpep_pickup_datetime_transformed
    FROM magic.taxi
    WHERE 1 = 0;  -- This creates an empty table with the same structure, including the transformed column
    '''
    
    insert_data_query = '''
    INSERT INTO transformed_taxi
    SELECT
        *,
        tpep_pickup_datetime::TIMESTAMP AS tpep_pickup_datetime_transformed
    FROM magic.taxi;
    '''
    
    sample_schema = 'magic'
    sample_table = 'transformed_taxi'
    sample_size = 10000  # Number of rows to sample

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # Create the new table with the same structure and the transformed column
        loader.execute(create_table_query)
        
        # Insert transformed data into the new table
        loader.execute()        
        loader.commit()  # Permanently apply database changes
        
        # Sample data from the new table
        sampled_data = loader.sample(sample_schema, sample_table, sample_size)
        
        # Convert the DataFrame to a list of dictionaries
        return sampled_data.to_dict(orient='records')


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    
    # Print the type of the output for debugging
    print(f"Output type: {type(output)}")
    
    assert isinstance(output, list), 'Output should be a list'
    assert len(output) > 0, 'Output list is empty'

    # Add specific checks for column types and data
    sample_row = output[0]
    assert 'tpep_pickup_datetime_transformed' in sample_row, 'Missing transformed column'
    assert isinstance(sample_row['tpep_pickup_datetime_transformed'], str), 'Transformed column should be of type TIMESTAMP'

    # Print some sample rows for debug purposes
    print('Sample rows:', output[:5])

    # Check the original column is still present and correct type
    assert 'tpep_pickup_datetime' in sample_row, 'Original column missing'
    assert isinstance(sample_row['tpep_pickup_datetime'], str), 'Original column should be of type TEXT'

    # Further checks can be added here as needed
