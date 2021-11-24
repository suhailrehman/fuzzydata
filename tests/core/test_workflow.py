import pytest

from fuzzydata.core.artifact import Artifact
from fuzzydata.clients.pandas import DataFrameWorkflow
from fuzzydata.clients.sqlite import SQLWorkflow


@pytest.fixture(scope='session')
def df_workflow(tmpdir_factory):
    out_dir = tmpdir_factory.mktemp('fuzzydata_temp_wf_df')
    return DataFrameWorkflow(name='test_wf', out_directory=out_dir)


@pytest.fixture(scope='session')
def sql_workflow(tmpdir_factory):
    out_dir = tmpdir_factory.mktemp('fuzzydata_temp_wf_df')
    return SQLWorkflow(name='test_wf', out_directory=out_dir)


@pytest.mark.dependency()
@pytest.mark.parametrize('abstract_workflow', ['df_workflow', 'sql_workflow'])
def test_generate_base_artifact(abstract_workflow, request):
    workflow = request.getfixturevalue(abstract_workflow)
    previous_len = len(workflow)
    workflow.generate_base_artifact(num_rows=100, num_cols=10)
    new_label = workflow.artifact_list[-1]
    assert len(workflow) == previous_len+1
    assert isinstance(workflow.artifact_dict[new_label], Artifact)
    assert new_label in workflow.graph.nodes()


@pytest.mark.dependency(depends=['test_generate_base_artifact'])
@pytest.mark.parametrize('abstract_workflow', ['df_workflow', 'sql_workflow'])
def test_generate_artifact_from_operation(abstract_workflow, request):
    workflow = request.getfixturevalue(abstract_workflow)
    previous_len = len(workflow)
    source_label = workflow.artifact_list[-1]
    source_artifact = workflow.artifact_dict[source_label]
    workflow.generate_artifact_from_operation([source_artifact], 'sample', {'frac': 0.5})
    new_label = workflow.artifact_list[-1]
    assert len(workflow) == previous_len + 1
    assert isinstance(workflow.artifact_dict[new_label], Artifact)
    assert new_label in workflow.graph.nodes()

