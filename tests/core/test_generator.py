import glob
import itertools
import logging
import os

import pandas as pd
import pytest

from fuzzydata.clients import supported_workflows, SQLWorkflow, travis_workflows
from fuzzydata.core.generator import generate_schema, generate_table, generate_workflow

logger = logging.getLogger(__name__)

# Skipping modin tests on travis for time and existing modin error
# https://github.com/modin-project/modin/issues/4287
if "TRAVIS" in os.environ and os.environ["TRAVIS"] == "true":
    workflows_to_test = travis_workflows.values()
else:
    # Skipping modin tests
    # workflows_to_test = supported_workflows.values()
    workflows_to_test = travis_workflows.values()


@pytest.fixture(scope="module", params=[10, 15, 20])
def schema(request):
    generated_schema = generate_schema(request.param)
    for col_name, faker_col_name in generated_schema.items():
        assert isinstance(col_name, str)
        prefix, fake_name = col_name.split('__')
        assert fake_name == faker_col_name
    return generated_schema


@pytest.mark.parametrize('num_rows', [10, 100, 1000])
def test_generate_table(schema, num_rows):
    table = generate_table(num_rows, column_dict=schema)
    assert isinstance(table, pd.DataFrame)
    assert num_rows == len(table.index)


@pytest.mark.parametrize('wf_class,num_versions,base_shape', itertools.product(workflows_to_test,
                                                                               [10, 20],
                                                                               [(10, 1000), (20, 10000)]))
def test_generate_workflow(wf_class, num_versions, base_shape, tmpdir_factory):
    try:
        output_path = tmpdir_factory.mktemp(f'test_{wf_class.__name__}')
        # Exclude pivots from SQLWorkflow test
        if wf_class.__name__ == 'SQLWorkflow':
            exclude = ['pivot']
        else:
            exclude = []
        workflow = generate_workflow(wf_class, name=f'test_{wf_class.__name__}', num_versions=num_versions,
                                     base_shape=base_shape, out_directory=output_path, exclude_ops=exclude)

        assert len(workflow) == num_versions
        assert os.path.exists(f"{output_path}/artifacts/")
        assert len(list(glob.glob(f"{output_path}/artifacts/*.csv"))) == len(workflow.artifact_dict)
        assert os.path.exists(f"{output_path}/{workflow.name}_operations.json")
        assert os.path.getsize(f"{output_path}/{workflow.name}_operations.json") > 0
        assert os.path.exists(f"{output_path}/{workflow.name}_gt_graph.csv")
    except Exception as e:
        logger.error(f"Error in Workflow Path: {output_path}")
        raise e