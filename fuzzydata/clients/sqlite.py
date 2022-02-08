import math
from typing import List

import pandas
import sqlalchemy

from fuzzydata.core.artifact import Artifact
from fuzzydata.core.generator import generate_table
from fuzzydata.core.operation import Operation, T
from fuzzydata.core.workflow import Workflow


class SQLArtifact(Artifact):

    def __init__(self, *args, **kwargs):
        self.sql_engine = kwargs.pop("sql_engine")
        self.from_sql = kwargs.pop("from_sql", None)
        self.sync_df = kwargs.pop("sync_df", False)

        super(SQLArtifact, self).__init__(*args, **kwargs)

        self.operation_class = SQLOperation
        self.pd = pandas

        self._deserialization_function = {
            'csv': self.pd.read_csv
        }
        self._serialization_function = {
            'csv': 'to_csv'
        }

        self._get_table = f'SELECT * FROM {self.label}'
        self._del_table = f'DROP TABLE IF EXISTS {self.label}'
        self._num_rows = f'SELECT COUNT(*) FROM {self.label}'

        if self.from_sql:
            self.sql_engine.execute(self.from_sql)
            if self.sync_df:
                self.table = self.pd.read_sql(self._get_table, con=self.sql_engine)

    def generate(self, num_rows, schema):
        df = generate_table(num_rows, column_dict=schema)
        df.to_sql(self.label, con=self.sql_engine, if_exists='replace')
        self.schema_map = schema
        if self.sync_df:
            self.table = df
        # self.in_memory = True

    def deserialize(self, filename=None):
        if not filename:
            filename = self.filename

        df = self._deserialization_function[self.file_format](filename)
        df.to_sql(self.label, con=self.sql_engine, if_exists='replace')
        if self.sync_df:
            self.table = df
        # self.in_memory = True

    def serialize(self, filename=None):
        if not filename:
            filename = self.filename

        df = self.pd.read_sql(self._get_table, con=self.sql_engine)
        serialization_method = getattr(df, self._serialization_function[self.file_format])
        serialization_method(filename)

    def destroy(self):
        if self.sync_df:
            del self.table
        self.sql_engine.execute(self._del_table)

    def to_df(self):
        return self.pd.read_sql(self._get_table, con=self.sql_engine)

    def __len__(self):
        return self.sql_engine.execute(self._num_rows).first()[0]


class SQLOperation(Operation['SQLArtifact']):

    def __init__(self, *args, **kwargs):
        super(SQLOperation, self).__init__(*args, **kwargs)
        self.agg_function_dict = {
            'mean': 'AVG'
        }

    def sample(self, frac: float) -> SQLArtifact:
        super(SQLOperation, self).sample(frac)
        num_rows = len(self.sources[0])
        sample_rows = math.ceil(num_rows*frac)
        sql_sample_stmt = f"CREATE TABLE `{self.new_label}` AS " \
                          f"SELECT * FROM `{self.sources[0].label}` ORDER BY RANDOM() " \
                          f"LIMIT {sample_rows} "
        return SQLArtifact(label=self.new_label,
                           sql_engine=self.sources[0].sql_engine,
                           from_sql=sql_sample_stmt,
                           schema_map=self.dest_schema_map)

    def groupby(self, group_columns: List[str], agg_columns: List[str], agg_function: str) -> SQLArtifact:
        super(SQLOperation, self).groupby(group_columns, agg_columns, agg_function)
        group_cols_str = ', '.join([f"`{x}`" for x in group_columns])

        # Translate the aggregate function string if required
        if agg_function in self.agg_function_dict:
            agg_function = self.agg_function_dict[agg_function]

        agg_cols_str = f"{','.join([f'{agg_function}(`{x}`) AS `{x}`' for x in agg_columns])}"
        sql_groupby_stmt = f"CREATE TABLE {self.new_label} AS SELECT {group_cols_str}, {agg_cols_str} " \
                           f"FROM {self.sources[0].label} " \
                           f"GROUP BY {group_cols_str} "
        return SQLArtifact(label=self.new_label,
                           sql_engine=self.sources[0].sql_engine,
                           from_sql=sql_groupby_stmt,
                           schema_map=self.dest_schema_map)

    def project(self, output_cols: List[str]) -> T:
        super(SQLOperation, self).project(output_cols)

        project_predicate = ','.join([f"`{x}`" for x in output_cols])

        sql_project_stmt = f"CREATE TABLE `{self.new_label}` AS " \
                           f"SELECT {project_predicate} FROM `{self.sources[0].label}` "
        return SQLArtifact(label=self.new_label,
                           sql_engine=self.sources[0].sql_engine,
                           from_sql=sql_project_stmt,
                           schema_map=self.dest_schema_map)

    def select(self, condition: str) -> T:
        super(SQLOperation, self).select(condition)
        sql_select_stmt = f"CREATE TABLE `{self.new_label}` AS SELECT * FROM `{self.sources[0].label}` " \
                          f"WHERE {condition}"
        return SQLArtifact(label=self.new_label,
                           sql_engine=self.sources[0].sql_engine,
                           from_sql=sql_select_stmt,
                           schema_map=self.dest_schema_map)

    def merge(self, key_col: List[str]) -> T:
        super(SQLOperation, self).merge(key_col)
        sql_select_stmt = f"CREATE TABLE {self.new_label} AS SELECT * FROM {self.sources[0].label} " \
                          f"INNER JOIN {self.sources[0].label}" \
                          f"WHERE {self.sources[0].label}.{key_col} = {self.sources[1].label}.{key_col}"
        return SQLArtifact(label=self.new_label,
                           sql_engine=self.sources[0].sql_engine,
                           from_sql=sql_select_stmt,
                           schema_map=self.dest_schema_map)

    def pivot(self, index_cols: List[str], columns: List[str], value_col: List[str], agg_func: str) -> T:
        raise NotImplementedError('Generic Pivots in SQL are Hard!')


class SQLWorkflow(Workflow):
    def __init__(self, *args, **kwargs):
        super(SQLWorkflow, self).__init__(*args, **kwargs)
        self.artifact_class = SQLArtifact
        self.operator_class = SQLOperation
        self.sql_engine = sqlalchemy.create_engine(f"sqlite:///{self.out_dir}/{self.name}.db")

    def initialize_new_artifact(self, label=None, filename=None):
        return SQLArtifact(label, filename=filename, sql_engine=self.sql_engine)
