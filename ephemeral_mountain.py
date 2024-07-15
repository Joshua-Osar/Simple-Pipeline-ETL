from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
from pandas import DataFrame

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform_in_postgres(*args, **kwargs) -> DataFrame:
    """
    Performs a transformation in Postgres
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Specify your SQL transformation query
    query = '''
    CREATE TABLE IF NOT EXISTS magic.new_taxii AS
    SELECT
        *,
        tpep_pickup_datetime::TIMESTAMP AS tpep_pickup_datetime_transformed
    FROM magic.taxi
    WHERE 1=0;
    '''

    query2 = '''
    INSERT INTO magic.new_taxii
    SELECT
        *,
        tpep_pickup_datetime::TIMESTAMP AS tpep_pickup_datetime_transformed
    FROM magic.taxi;'''

    # Specify table to sample data from. Use to visualize changes to table.
    sample_table = 'new_taxii'
    sample_schema = 'magic'
    sample_size = 100

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # Write queries to transform your dataset with
        loader.execute(query)
        loader.execute(query2)
        loader.commit() # Permanently apply database changes
        return loader.sample(sample_schema, sample_table, sample_size)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
