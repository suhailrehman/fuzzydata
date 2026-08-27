import math
from typing import List

import pandas
import sqlalchemy
import logging

from fuzzydata.core.artifact import Artifact
from fuzzydata.core.generator import generate_table
from fuzzydata.core.operation import Operation, T
from fuzzydata.core.workflow import Workflow

logger = logging.getLogger(__name__)

class SQLArtifact(Artifact):

    def __init__(self, *args, **kwargs):
        self.sql_engine = kwargs.pop("sql_engine")
        self.from_sql = kwargs.pop("from_sql", None)
        self.sync_df = kwargs.pop("sync_df", False)
        from_df = kwargs.pop("from_df", None)

        super(SQLArtifact, self).__init__(*args, **kwargs)

        self.operation_class = SQLOperation
        self.pd = pandas

        self._deserialization_function = {
            'csv': self.pd.read_csv
        }
        self._serialization_function = {
            'csv': 'to_csv'
        }

        self._get_table = sqlalchemy.text(f'SELECT * FROM `{self.label}`')
        self._del_table = sqlalchemy.text(f'DROP TABLE IF EXISTS `{self.label}`')
        self._num_rows = sqlalchemy.text(f'SELECT COUNT(*) FROM `{self.label}`')

        if self.from_sql:
            self.execute_sql(sqlalchemy.text(self.from_sql))
            if self.sync_df:
                self.table = self.pd.read_sql(self._get_table, con=self.sql_engine)

        elif from_df is not None:
            self.from_df(from_df)

    def execute_sql(self, sql_code):
        with self.sql_engine.connect() as conn:
            return conn.execute(sql_code)

    def generate(self, num_rows, schema, rng=None):
        df = generate_table(num_rows, column_dict=schema, rng=rng)
        # index=False for consistency with from_df(); otherwise an extra "index" column
        # appears in the table and in every downstream SELECT *.
        df.to_sql(self.label, con=self.sql_engine, if_exists='replace', index=False)
        self.schema_map = schema
        if self.sync_df:
            self.table = df
        # self.in_memory = True

    def from_df(self, df):
        df.to_sql(self.label, con=self.sql_engine, if_exists='replace', index=False)
        if self.sync_df:
            self.table = df

    def deserialize(self, filename=None):
        if not filename:
            filename = self.filename

        df = self._deserialization_function[self.file_format](filename)
        df.to_sql(self.label, con=self.sql_engine, if_exists='replace', index=False)
        if self.sync_df:
            self.table = df
        # self.in_memory = True

    def serialize(self, filename=None):
        if not filename:
            filename = self.filename

        df = self.pd.read_sql(self._get_table, con=self.sql_engine)
        serialization_method = getattr(df, self._serialization_function[self.file_format])
        serialization_method(filename, index=False)

    def destroy(self):
        if self.sync_df:
            del self.table
        self.execute_sql(self._del_table)

    def to_df(self):
        return self.pd.read_sql(self._get_table, con=self.sql_engine)

    def __len__(self):
        return self.execute_sql(self._num_rows).first()[0]


class SQLOperation(Operation['SQLArtifact']):

    #: Generic pivots have no portable SQL expression; see pivot() below.
    unsupported_ops = frozenset({'pivot'})

    def __init__(self, *args, **kwargs):
        self.artifact_class = kwargs.pop('artifact_class', SQLArtifact)
        super(SQLOperation, self).__init__(*args, **kwargs)
        self.agg_function_dict = {
            'mean': 'AVG'
        }
        self.code = f"SELECT * FROM `{self.sources[0].label}`"

    def sample(self, frac: float, random_state: int = None) -> SQLArtifact:
        # random_state is accepted and recorded for parity with the pandas client, but
        # SQLite's RANDOM() cannot be seeded without a custom UDF, so which rows come back
        # is NOT reproducible here. The row COUNT is deterministic (ceil(n*frac)), which is
        # what the replay test asserts for this client. Making SQL sampling seedable needs a
        # different strategy (a deterministic hash ordering) and is deferred to Track B/B4.
        super(SQLOperation, self).sample(frac, random_state)
        num_rows = len(self.sources[0])
        sample_rows = math.ceil(num_rows*frac)
        sql_sample_stmt = f"SELECT * FROM {{source}} ORDER BY RANDOM() " \
                          f"LIMIT {sample_rows} "
        return sql_sample_stmt

    def apply(self, numeric_col: str, a: float, b: float) -> SQLArtifact:
        super(SQLOperation, self).apply(numeric_col, a, b)
        new_col_name = self.apply_column_name(numeric_col, a, b)
        sql_apply_stmt = f"SELECT *, (`{numeric_col}` * {a}) + {b} AS `{new_col_name}` " \
                         f"FROM {{source}}"
        return sql_apply_stmt

    def groupby(self, group_columns: List[str], agg_columns: List[str], agg_function: str) -> SQLArtifact:
        super(SQLOperation, self).groupby(group_columns, agg_columns, agg_function)
        group_cols_str = ', '.join([f"`{x}`" for x in group_columns])

        # Translate the aggregate function string if required
        if agg_function in self.agg_function_dict:
            agg_function = self.agg_function_dict[agg_function]

        agg_cols_str = f"{','.join([f'{agg_function}(`{x}`) AS `{x}`' for x in agg_columns])}"
        sql_groupby_stmt = f"SELECT {group_cols_str}, {agg_cols_str} " \
                           f"FROM {{source}} " \
                           f"GROUP BY {group_cols_str} "
        return sql_groupby_stmt

    def project(self, output_cols: List[str]) -> T:
        super(SQLOperation, self).project(output_cols)

        project_predicate = ','.join([f"`{x}`" for x in output_cols])

        sql_project_stmt = f"SELECT {project_predicate} FROM {{source}} "
        return sql_project_stmt

    def select(self, condition: str) -> T:
        super(SQLOperation, self).select(condition)
        sql_select_stmt = f"SELECT * FROM {{source}} " \
                          f"WHERE {condition}"
        return sql_select_stmt

    def merge(self, key_col: List[str]) -> T:
        super(SQLOperation, self).merge(key_col)
        sql_select_stmt = f"SELECT * FROM {{source}} " \
                          f"INNER JOIN `{self.sources[1].label}` " \
                          f"USING (`{key_col}`)"
        return sql_select_stmt

    def pivot(self, index_cols: List[str], columns: List[str], value_col: List[str], agg_func: str) -> T:
        raise NotImplementedError('Generic Pivots in SQL are Hard!')

    def fill(self, col_name: str, old_value, new_value):
        super(SQLOperation, self).fill(col_name, old_value, new_value)
        # NB: was `set(col_name)`, which is the set of the string's characters -- the column
        # was never actually excluded. Iterate the schema map in order so the projected
        # column order is deterministic rather than set-iteration dependent.
        other_cols = [f"`{x}`" for x in self.current_schema_map if x != col_name]
        # Trailing comma would be a syntax error when col_name is the only column.
        other_columns = (','.join(other_cols) + ',') if other_cols else ''
        # SQL string literals: double any embedded single quote. Values arrive raw (the
        # pandas client repr()s them for its own eval()'d code; we must not inherit that).
        def _lit(v):
            return "'" + str(v).replace("'", "''") + "'"
        sql_fill_stmt = f"SELECT {other_columns} " \
                        f"CASE WHEN `{col_name}` = {_lit(old_value)} THEN {_lit(new_value)} " \
                        f"ELSE `{col_name}` END " \
                        f"AS `{col_name}` FROM {{source}}"
        return sql_fill_stmt

    def chain_operation(self, op, args):
        new_code = getattr(self, op)(**args)
        logger.debug(f'Code before chaining: {self.code}')
        self.code = new_code.replace('{source}', f'({self.code})')
        logger.debug(f'Code after chaining: {self.code}')
        # Must run after code generation: records op_list metadata used by the JSON spec
        # and by replay. Without it SQL workflows serialize with an empty op_list.
        super(SQLOperation, self).chain_operation(op, args)

    def materialize(self, new_label):
        super(SQLOperation, self).materialize(new_label)
        logger.debug(f'Executing SQL code: {self.code}')
        self.code = f'CREATE VIEW `{self.new_label}` AS {self.code}'
        return self.artifact_class(label=self.new_label,
                                   sql_engine=self.sources[0].sql_engine,
                                   from_sql=self.code,
                                   schema_map=self.current_schema_map)


class SQLWorkflow(Workflow):
    def __init__(self, *args, **kwargs):
        sql_string = kwargs.pop('sql_string', None)
        super(SQLWorkflow, self).__init__(*args, **kwargs)
        self.artifact_class = SQLArtifact
        self.operator_class = SQLOperation
        if not sql_string:
            sql_string = f"sqlite:///{self.out_dir}/{self.name}.db"
        self.sql_engine = sqlalchemy.create_engine(sql_string)

    def initialize_new_artifact(self, label=None, filename=None, schema_map=None):
        return SQLArtifact(label, filename=filename, sql_engine=self.sql_engine, schema_map=schema_map)
