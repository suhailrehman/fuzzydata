import logging
import pytest
import sqlalchemy
import modin.pandas

from fuzzydata.clients.modin import ModinArtifact, ModinWorkflow
from fuzzydata.clients.sqlite import SQLArtifact, SQLWorkflow
from fuzzydata.clients.pandas import DataFrameArtifact, DataFrameWorkflow
from fuzzydata.core.generator import generate_schema

logger = logging.getLogger(__name__)

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


artifact_fixtures = ['dataframe_artifact', 'sql_artifact', 'modin_artifact']
generated_artifact_fixtures = ['dataframe_artifact_generated', 'sql_artifact_generated', 'modin_artifact_generated']
static_artifact_fixtures = ['dataframe_artifact_static', 'sql_artifact_static', 'modin_artifact_static']
workflow_fixtures = ['df_workflow', 'sql_workflow', 'modin_workflow']


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
