import pytest

from tests.conftest import generated_artifact_fixtures


operation_arg_pairs = [
    ('sample', {'frac': 0.5}),
    ('sample', {'frac': 0.5}),

]

@pytest.mark.parametrize('artifact', generated_artifact_fixtures)
def test_sample(artifact, request):
    concrete_artifact = request.getfixturevalue(artifact)
    sample_op = concrete_artifact.operation_class(sources=[concrete_artifact], new_label='sample_df',
                                                  op='sample', args={'frac': 0.5})
    sample_op.execute()