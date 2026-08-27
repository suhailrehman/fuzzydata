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


# Each artifact variant gets its OWN object. They used to be derived from one another --
# *_static and *_generated both called .generate() on the same shared session artifact -- so
# whichever was instantiated first won, and adding a test file that touched them in a
# different order silently corrupted the others. Independent objects remove the ordering
# dependence entirely.

def _tmp_csv(tmpdir_factory, name):
    return tmpdir_factory.mktemp(f'fuzzydata_{name}').join(f'{name}.csv')


def _sql_engine(tmpdir_factory, name):
    tmp_dir = tmpdir_factory.mktemp(f'fuzzydata_{name}')
    logger.info(f'Test Directory: {tmp_dir}')
    return sqlalchemy.create_engine(f"sqlite:///{tmp_dir}/{name}.db"), tmp_dir.join(f'{name}.csv')


@pytest.fixture(scope="session")
def dataframe_artifact(tmpdir_factory):
    return DataFrameArtifact('test_df', filename=_tmp_csv(tmpdir_factory, 'df_test'))


@pytest.fixture(scope="session")
def modin_artifact(tmpdir_factory):
    return ModinArtifact('test_df', filename=_tmp_csv(tmpdir_factory, 'modin_test'))


@pytest.fixture(scope="session")
def sql_artifact(tmpdir_factory):
    engine, filename = _sql_engine(tmpdir_factory, 'sql_test')
    return SQLArtifact('test_df', filename=filename, sql_engine=engine)


@pytest.fixture(scope="session")
def dataframe_artifact_generated(tmpdir_factory):
    artifact = DataFrameArtifact('gen_df', filename=_tmp_csv(tmpdir_factory, 'df_gen'))
    artifact.generate(100, generate_schema(20))
    return artifact


@pytest.fixture(scope="session")
def sql_artifact_generated(tmpdir_factory):
    engine, filename = _sql_engine(tmpdir_factory, 'sql_gen')
    artifact = SQLArtifact('gen_df', filename=filename, sql_engine=engine)
    artifact.generate(100, generate_schema(20))
    return artifact


@pytest.fixture(scope="session")
def modin_artifact_generated(tmpdir_factory):
    artifact = ModinArtifact('gen_df', filename=_tmp_csv(tmpdir_factory, 'modin_gen'))
    artifact.generate(100, generate_schema(20))
    return artifact


@pytest.fixture(scope="session")
def dataframe_artifact_static(tmpdir_factory):
    artifact = DataFrameArtifact('static_df', filename=_tmp_csv(tmpdir_factory, 'df_static'))
    artifact.generate(100, dict(_static_schema_test))
    return artifact


@pytest.fixture(scope="session")
def sql_artifact_static(tmpdir_factory):
    engine, filename = _sql_engine(tmpdir_factory, 'sql_static')
    artifact = SQLArtifact('static_df', filename=filename, sql_engine=engine)
    artifact.generate(100, dict(_static_schema_test))
    return artifact


@pytest.fixture(scope="session")
def modin_artifact_static(tmpdir_factory):
    artifact = ModinArtifact('static_df', filename=_tmp_csv(tmpdir_factory, 'modin_static'))
    artifact.generate(100, dict(_static_schema_test))
    return artifact


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
