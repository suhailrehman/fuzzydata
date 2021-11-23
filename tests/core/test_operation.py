import pytest

from fuzzydata.core.operation import Operation

_artifact_fixtures = ['dataframe_artifact_generated', 'sql_artifact_generated']


@pytest.mark.parametrize('artifact', _artifact_fixtures)
def test_sample(artifact, request):
    concrete_artifact = request.getfixturevalue(artifact)
    sample_op = concrete_artifact.operation_class(sources=[concrete_artifact], new_label='sample_df',
                                                  op='sample', args={'frac': 0.5})
    sample_op.execute()

