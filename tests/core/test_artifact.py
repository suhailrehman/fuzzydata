import pandas as pd
import pytest
import os

import sqlalchemy

from fuzzydata.core.artifact import DataFrameArtifact, SQLArtifact
from fuzzydata.core.generator import generate_schema


@pytest.mark.dependency()
@pytest.mark.parametrize('artifact', ['dataframe_artifact', 'sql_artifact'])
def test_generate(artifact, request):
    tmp_schema = generate_schema(20)
    concrete_artifact = request.getfixturevalue(artifact)
    concrete_artifact.generate(100, tmp_schema)


@pytest.mark.dependency(depends=["test_generate"])
@pytest.mark.parametrize('artifact', ['dataframe_artifact', 'sql_artifact'])
def test_serialize_deserialize(artifact, request):
    concrete_artifact = request.getfixturevalue(artifact)
    df_file = concrete_artifact.filename
    concrete_artifact.serialize()
    assert os.path.exists(df_file)
    concrete_artifact.destroy()
    concrete_artifact.deserialize()
    assert isinstance(concrete_artifact.table, pd.DataFrame)