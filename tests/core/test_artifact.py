import pandas as pd
import pytest
import os

from pytest_dependency import depends

from fuzzydata.core.generator import generate_schema
from tests.conftest import artifact_fixtures


@pytest.mark.dependency()
@pytest.mark.parametrize('artifact', artifact_fixtures)
def test_generate(artifact, request):
    tmp_schema = generate_schema(20)
    concrete_artifact = request.getfixturevalue(artifact)
    concrete_artifact.generate(100, tmp_schema)


@pytest.mark.dependency()
@pytest.mark.parametrize('artifact', artifact_fixtures)
def test_serialize_deserialize(artifact, request):
    # Depend on the same-parameter instance. A static depends=["test_generate"] never
    # resolves for a parametrized test (it registers as "test_generate[<param>]"), which
    # silently skipped this test entirely.
    depends(request, [f'test_generate[{artifact}]'])
    concrete_artifact = request.getfixturevalue(artifact)
    df_file = concrete_artifact.filename
    concrete_artifact.serialize()
    assert os.path.exists(df_file)
    concrete_artifact.destroy()
    concrete_artifact.deserialize()
    assert isinstance(concrete_artifact.to_df(), concrete_artifact.pd.DataFrame)