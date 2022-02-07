import glob
import os.path
import logging
import pytest

from fuzzydata.core.artifact import Artifact
from tests.conftest import workflow_fixtures

# Disable Faker log spam in DEBUG mode
logger = logging.getLogger(__name__)


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


@pytest.mark.dependency(depends=['test_generate_artifact_from_operation'])
@pytest.mark.parametrize('abstract_workflow', workflow_fixtures)
def test_serialize_deserialize_workflow(abstract_workflow, request, tmpdir_factory):
    workflow = request.getfixturevalue(abstract_workflow)
    output_path = tmpdir_factory.mktemp(workflow.name)
    logger.info(f'Output Dir {output_path}')

    workflow.serialize_workflow(output_dir=output_path)

    assert os.path.exists(f"{output_path}/artifacts/")
    assert len(list(glob.glob(f"{output_path}/artifacts/*.csv"))) == len(workflow.artifact_dict)
    assert os.path.exists(f"{output_path}/{workflow.name}_operations.json")
    assert os.path.getsize(f"{output_path}/{workflow.name}_operations.json") > 0
    assert os.path.exists(f"{output_path}/{workflow.name}_gt_graph.csv")

    # Testing deserialization (same workflow type)
    new_out_dir = tmpdir_factory.mktemp('replay_wf')
    new_wf_cls = workflow.__class__
    new_wf_cls.load_workflow(output_path, new_out_dir, replay=True)
