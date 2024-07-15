from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path

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
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    create_table_query = '''
    CREATE TABLE IF NOT EXISTS magic.new_taxii AS
    SELECT
        *,
        tpep_pickup_datetime::TIMESTAMP AS tpep_pickup_datetime_transformed
    FROM magic.taxi
    WHERE 1 = 0;  -- This creates an empty table with the same structure, including the transformed column
    '''

    insert_data_query = '''
    INSERT INTO magic.new_taxi
    SELECT
        *,
        tpep_pickup_datetime::TIMESTAMP AS tpep_pickup_datetime_transformed
    FROM magic.taxi;
    '''

    sample_schema = 'magic'
    sample_table = 'new_taxi'
    sample_size = 10_000

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # Create the new table with the same structure and the transformed column
        loader.execute(create_table_query)
        print("Table creation query executed successfully.")
        
        # Insert transformed data into the new table
        loader.execute(insert_data_query)
        print("Data insertion query executed successfully.")
        
        loader.commit()  # Permanently apply database changes
        print("Database changes committed successfully.")
        
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
    
   