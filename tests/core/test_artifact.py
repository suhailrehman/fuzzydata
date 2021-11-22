import pandas as pd
import pytest
import os

from fuzzydata.core.artifact import DataFrameArtifact
from fuzzydata.core.generator import generate_schema


@pytest.fixture(scope="session")
def dataframe_artifact(tmpdir_factory):
    tmp_dir = tmpdir_factory.mktemp("fuzzydata_test")
    return DataFrameArtifact('test_df', filename=tmp_dir.join('test_df.csv'))


@pytest.mark.dependency()
def test_generate(dataframe_artifact):
    tmp_schema = generate_schema(20)
    dataframe_artifact.generate(100, tmp_schema)


@pytest.mark.dependency(depends=["test_generate"])
def test_serialize_deserialize(dataframe_artifact):
    df_file = dataframe_artifact.filename
    dataframe_artifact.serialize()
    assert os.path.exists(df_file)
    del dataframe_artifact.table
    dataframe_artifact.deserialize()
    assert isinstance(dataframe_artifact.table, pd.DataFrame)