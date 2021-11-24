import pytest

from fuzzydata.core.artifact import Artifact
from tests.conftest import workflow_fixtures


@pytest.mark.dependency()
@pytest.mark.parametrize('abstract_workflow', workflow_fixtures)
def test_generate_base_artifact(abstract_workflow, request):
    workflow = request.getfixturevalue(abstract_workflow)
    previous_len = len(workflow)
    workflow.generate_base_artifact(num_rows=100, num_cols=10)
    new_label = workflow.artifact_list[-1]
    assert len(workflow) == previous_len+1
    assert isinstance(workflow.artifact_dict[new_label], Artifact)
    assert new_label in workflow.graph.nodes()


@pytest.mark.dependency(depends=['test_generate_base_artifact'])
@pytest.mark.parametrize('abstract_workflow', workflow_fixtures)
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

