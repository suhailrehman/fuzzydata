import pytest
import sqlalchemy

from fuzzydata.clients.sqlite import SQLArtifact, SQLWorkflow
from fuzzydata.clients.pandas import DataFrameArtifact, DataFrameWorkflow
from fuzzydata.core.generator import generate_schema

artifact_fixtures = ['dataframe_artifact', 'sql_artifact']
generated_artifact_fixtures = ['dataframe_artifact_generated', 'sql_artifact_generated']
workflow_fixtures = ['df_workflow', 'sql_workflow']

@pytest.fixture(scope="session")
def dataframe_artifact(tmpdir_factory):
    tmp_dir = tmpdir_factory.mktemp("fuzzydata_test")
    return DataFrameArtifact('test_df', filename=tmp_dir.join('test_df.csv'))


@pytest.fixture(scope="session")
def sql_artifact(tmpdir_factory):
    tmp_dir = tmpdir_factory.mktemp("fuzzydata_test")
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


@pytest.fixture(scope='session')
def df_workflow(tmpdir_factory):
    out_dir = tmpdir_factory.mktemp('fuzzydata_temp_wf_df')
    return DataFrameWorkflow(name='test_wf', out_directory=out_dir)

@pytest.fixture(scope='session')
def sql_workflow(tmpdir_factory):
    out_dir = tmpdir_factory.mktemp('fuzzydata_temp_wf_df')
    return SQLWorkflow(name='test_wf', out_directory=out_dir)
