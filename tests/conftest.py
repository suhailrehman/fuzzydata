import logging
import os

import pytest
import sqlalchemy

from fuzzydata.clients.sqlite import SQLArtifact, SQLWorkflow
from fuzzydata.clients.pandas import DataFrameArtifact, DataFrameWorkflow
from fuzzydata.core.generator import generate_schema

logger = logging.getLogger(__name__)

# The modin client is deprecated and untested as of 0.1.0. It is excluded here by explicit
# opt-in rather than by install-detection, because ModinWorkflow.__init__ unconditionally
# starts a dask.distributed cluster (or ray runtime). That cluster is shared for the whole
# pytest session and, once a worker dies, every later modin test fails inside dask rather
# than inside fuzzydata. Gating on the env var means modin/dask/ray are never even imported.
#
# To exercise the modin client manually before a release (never in CI):
#     FUZZYDATA_TEST_MODIN=1 pytest
RUN_MODIN_TESTS = os.environ.get('FUZZYDATA_TEST_MODIN', '').lower() in ('1', 'true', 'yes')
if RUN_MODIN_TESTS:
    from fuzzydata.clients.modin import ModinArtifact, ModinWorkflow

_static_schema_test = {'EafKN__rgb_color': 'rgb_color',
                       'RFD4U__uuid4': 'uuid4',
                       'M8OoL__postcode': 'postcode',
                       'Qe0kk__ipv4_network_class': 'ipv4_network_class',
                       'qL81j__domain_name': 'domain_name',
                       'a0UaD__zipcode_in_state': 'zipcode_in_state',
                       'dHchx__suffix_female': 'suffix_female',
                       'Vg4hn__name_male': 'name_male',
                       'dwdle__zipcode_plus4': 'zipcode_plus4',
                       'Vyl6E__text': 'text',
                       'AqhyH__century': 'century',
                       'zmpoV__randomize_nb_elements': 'randomize_nb_elements',
                       'mRIWF__postalcode_in_state': 'postalcode_in_state',
                       '9YjpC__credit_card_provider': 'credit_card_provider'}

def _with_modin(fixtures, modin_fixture):
    """Append the modin fixture only under FUZZYDATA_TEST_MODIN, so by default no modin
    parameter is generated and no modin test exists to run."""
    return fixtures + ([modin_fixture] if RUN_MODIN_TESTS else [])


artifact_fixtures = _with_modin(['dataframe_artifact', 'sql_artifact'], 'modin_artifact')
generated_artifact_fixtures = _with_modin(['dataframe_artifact_generated', 'sql_artifact_generated'],
                                          'modin_artifact_generated')
static_artifact_fixtures = _with_modin(['dataframe_artifact_static', 'sql_artifact_static'],
                                       'modin_artifact_static')
workflow_fixtures = _with_modin(['df_workflow', 'sql_workflow'], 'modin_workflow')


@pytest.fixture(scope="session")
def dataframe_artifact(tmpdir_factory):
    tmp_dir = tmpdir_factory.mktemp("fuzzydata_df_test")
    return DataFrameArtifact('test_df', filename=tmp_dir.join('test_df.csv'))


@pytest.fixture(scope="session")
def modin_artifact(tmpdir_factory):
    tmp_dir = tmpdir_factory.mktemp("fuzzydata_modin_test")
    return ModinArtifact('test_df', filename=tmp_dir.join('test_df.csv'))


@pytest.fixture(scope="session")
def sql_artifact(tmpdir_factory):
    tmp_dir = tmpdir_factory.mktemp("fuzzydata_sql_test")
    logger.info(f'Test Directory: {tmp_dir}')
    sql_engine = sqlalchemy.create_engine(f"sqlite:///{tmp_dir}/fuzzydata_test.db")
    return SQLArtifact('test_df', filename=tmp_dir.join('test_df.csv'), sql_engine=sql_engine)


@pytest.fixture(scope="session")
def dataframe_artifact_generated(dataframe_artifact):
    tmp_schema = generate_schema(20)
    dataframe_artifact.generate(100, tmp_schema)
    return dataframe_artifact


@pytest.fixture(scope="session")
def sql_artifact_generated(sql_artifact):
    tmp_schema = generate_schema(20)
    sql_artifact.generate(100, tmp_schema)
    return sql_artifact


@pytest.fixture(scope="session")
def modin_artifact_generated(modin_artifact):
    tmp_schema = generate_schema(20)
    modin_artifact.generate(100, tmp_schema)
    return modin_artifact


@pytest.fixture(scope="session")
def dataframe_artifact_static(dataframe_artifact):
    dataframe_artifact.generate(100, _static_schema_test)
    return dataframe_artifact


@pytest.fixture(scope="session")
def sql_artifact_static(sql_artifact):
    sql_artifact.generate(100, _static_schema_test)
    return sql_artifact


@pytest.fixture(scope="session")
def modin_artifact_static(modin_artifact):
    modin_artifact.generate(100, _static_schema_test)
    return modin_artifact


@pytest.fixture(scope='session')
def df_workflow(tmpdir_factory):
    out_dir = tmpdir_factory.mktemp('fuzzydata_temp_wf_df')
    return DataFrameWorkflow(name='test_df_wf', out_directory=out_dir)


@pytest.fixture(scope='session')
def sql_workflow(tmpdir_factory):
    out_dir = tmpdir_factory.mktemp('fuzzydata_temp_wf_df')
    return SQLWorkflow(name='test_sql_wf', out_directory=out_dir)


@pytest.fixture(scope='session')
def modin_workflow(tmpdir_factory):
    out_dir = tmpdir_factory.mktemp('fuzzydata_temp_wf_df')
    return ModinWorkflow(name='test_modin_wf', out_directory=out_dir)
