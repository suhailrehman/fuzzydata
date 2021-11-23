import pytest
import sqlalchemy

from fuzzydata.core.artifact import DataFrameArtifact, SQLArtifact
from fuzzydata.core.generator import generate_schema


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